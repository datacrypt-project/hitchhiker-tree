(ns hitchhiker.bench
  (:require [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.tools.cli :refer [parse-opts]]
            [excel-templates.build :as excel]
            [hitchhiker.redis :as redis]
            [hitchhiker.s3 :as s3]
            [hitchhiker.tree.core :as core]
            [hitchhiker.tree.messaging :as msg])
  (:import [java.io File FileWriter]))

(defn generate-test-datasets
  "Returns a list of datasets"
  []
  [{:name "in-order" :data (range)}
   {:name "random" :data (repeatedly rand)}])

(defn core-b-tree
  "Returns a b-tree with core insert"
  [b backend]
  {:structure (core/b-tree (core/->Config b b 0))
   :insert core/insert
   :delete core/delete
   :flush (fn [x] (core/flush-tree x backend))})

(defn msg-b-tree
  "Returns a b-tree with msg insert"
  [b backend]
  (let [sqrt-b (long (Math/sqrt b))]
    {:structure (core/b-tree(core/->Config sqrt-b b (- b sqrt-b)))
     :insert msg/insert
     :delete msg/delete
     :flush (fn [x] (core/flush-tree x backend))}))

(defn sorted-set-repr
  "Returns a sorted set"
  []
  {:structure (sorted-map)
   :insert assoc
   :delete dissoc
   :flush (fn [set]
            {:tree set
             :stats (atom {})})})

(defn create-output-dir
  [dir aux]
  (let [my-dir (File. dir aux)]
    (when (.exists my-dir)
      (throw (ex-info (str "Output dir already exists: " dir) {})))
    (.mkdirs my-dir)
    (spit (File. my-dir "time") (str (java.util.Date.)))
    (let [speed-csv (FileWriter. (File. my-dir (str "speed_" aux ".csv")))
         ; flush-csv (FileWriter. (File. my-dir (str "flush_iops_" aux ".csv")))
          ]
      {:speed speed-csv
       ;:flush flush-csv
       })))

(defn benchmark
  "n is the total number of samples
   dataset is the test dataset
   flush-freq is the number of keys per flush
   datastruct is the test data structure
   out is the stream to write the results to (as well as stdout)"
  [n dataset flush-freq datastruct out delete-xform]
  (let [{:keys [structure delete insert flush]} datastruct
        dataset (take n (:data dataset))]
    (loop [[x & data] dataset
           t 0
           tree structure
           last-flush nil
           i 0
           inserting? true
           outputs []]
      (let [i' (inc i)
            {flushed-tree :tree
             stats :stats} (when (zero? (mod i' flush-freq))
                             (flush tree))
            before (System/nanoTime)
            tree' (if inserting?
                    (insert (or flushed-tree tree) x x)
                    (delete (or flushed-tree tree) x))
            after (System/nanoTime)
            log-inserts (zero? (mod i' (quot n 100)))
            updated-outputs (atom outputs)]
        (when log-inserts ;; 1000 pieces
          (binding [*out* (:speed out)]
            (let [ks (sort (keys last-flush))
                  avg-ns (float (/ t (quot n 100)))]
              (when (zero? i)
                (println (str "elements,op,insert_took_avg_ns,"
                              (str/join "," ks))))
              (println (str i' "," (if inserting? "insert" "delete") "," avg-ns
                            "," (str/join "," (map #(get last-flush %) ks))))
              (swap! updated-outputs conj (-> (into {} last-flush)
                                              (assoc :ins-avg-ns avg-ns
                                                     (if inserting?
                                                       :insert
                                                       :delete) true
                                                     :n i'))))))
        (cond
          (seq data)
          (recur data
                 (if log-inserts
                   0
                   (+ t (- after before)))
                 tree'
                 (if stats (merge-with + last-flush @stats) last-flush)
                 i'
                 inserting?
                 @updated-outputs)
          inserting?
          (recur (delete-xform dataset)
                 0
                 tree'
                 nil
                 i'
                 false
                 @updated-outputs)
          :else
          @updated-outputs)))))

(def options
  [["-n" "--num-operations NUM_OPS" "The number of elements that will be applied to the data structure"
    :default 100000
    :parse-fn #(Long. %)
    :validate [pos? "n must be positive"]]
   [nil "--data-structure STRUCT" "Which data structure to run the test on"
    :default "fractal"
    :validate [#(#{"fractal" "b-tree" "sorted-set"} %) "Data structure must be fractal, b-tree, or sorted set"]]
   [nil "--backend testing" "Runs the benchmark with the specified backend"
    :default "testing"
    :validate [#(#{"redis" "testing" "s3"} %) "Backend must be redis, s3 or testing"]]
   ["-d" "--delete-pattern PATTERN" "Specifies how the operations will be reordered on delete"
    :default "forward"
    :validate [#(#{"forward" "reverse" "shuffle" "zero"} %) "Incorrect delete pattern"]]
   [nil "--sorted-set" "Runs the benchmarks on a sorted set"]
   ["-b" "--tree-width WIDTH" "Determines the width of the trees. Fractal trees use sqrt(b) child pointers; the rest is for messages."
    :default 300
    :parse-fn #(Long. %)
    :validate [pos? "b must be positive"]]
   ["-f" "--flush-freq FREQ" "After how many operations should the tree get flushed?"
    :default 1000
    :parse-fn #(Long. %)
    :validate [pos? "flush frequency must be positive"]]
   [nil "--bucket STRING" "The S3 bucket to use."
    :default "hitchhiker-tree-s3-test"]
   ["-h" "--help" "Prints this help"]])

(defn exit
  [status msg]
  (println msg)
  (System/exit status))

(defn error-msg
  [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (str/join \newline errors)))

(defn usage
  [options-summary]
  (str/join \newline
            ["Usage: bench output-dir [options] [-- [other-options]]*"
             ""
             "Options:"
             options-summary
             ""
             "Delete patterns:"
             "forward: we delete the elements in the order they were inserted"
             "reverse: we delete the elements in the reverse order they were inserted"
             "shuffle: we delete the elements in a random order"
             "zero: we repeatedly attempt to delete 0, thus never actually deleting"
             ""
             "Backends:"
             "testing: this backend serializes nothing, just using an extra indirection"
             "redis: this backend uses a local redis server"
             "s3: this backend uses an S3 bucket"]))

(defn make-template-for-one-tree-freq-combo
  [list-of-benchmark-results filter-by]
  ;(clojure.pprint/pprint list-of-benchmark-results)
  (assert (= 2 (count list-of-benchmark-results)) "Should be random and ordered")
  (let [indexed (group-by :ds list-of-benchmark-results)]
    (map #(vector (:n %1) (:ins-avg-ns %1) (:writes %1) (:ins-avg-ns %2) (:writes %2))
         (filter filter-by (:results (first (get indexed "in-order"))))
         (filter filter-by (:results (first (get indexed "random")))))))

(defn template-one-sheet
  [pair-of-results-for-one-ds-config]
  (let [{:keys [tree ds freq n b results delete-pattern]}
        (first pair-of-results-for-one-ds-config)
        x {0 [["Data Structure" (name tree) "" "n" n "" "Data Set" ds]]
           1 [["Flush Frequency" freq "" "b" b "" "delete pattern" delete-pattern]]
           [5 18] (make-template-for-one-tree-freq-combo pair-of-results-for-one-ds-config :insert)
           [22 35] (make-template-for-one-tree-freq-combo pair-of-results-for-one-ds-config :delete)}]
    x))

(defn -main
  [& [root & args]]
  (let [outputs (atom [])]
    (doseq [args (or (->> args
                          (partition-by #(= % "--"))
                          (map-indexed vector)
                          (filter (comp even? first))
                          (map second)
                          (seq))
                     [[]])] ; always do one iteration
      (let [{:keys [options arguments errors summary]} (parse-opts args options)
            tree-to-test (atom {})
            results (atom [])]
        (cond
          (or (= "-h" root)
              (= "--help" root)
              (nil? root)
              (:help options)) (exit 0 (usage summary))
          (not= (count arguments) 0) (exit 1 (usage summary))
          errors (exit 1 (error-msg errors)))
        (let [backend (case (:backend options)
                        "testing" (core/->TestingBackend)
                        "redis" (do (redis/start-expiry-thread!)
                                    (redis/->RedisBackend))
                        "s3" (s3/->S3Backend (:bucket options)))
              delete-xform (case (:delete-pattern options)
                             "forward" identity
                             "reverse" reverse
                             "shuffle" shuffle
                             "zero" #(repeat (count %) 0.0))
              [tree-name structure]
              (case (:data-structure options)
                "b-tree" ["b-tree" (core-b-tree (:tree-width options) backend)]
                "fractal" ["fractal" (msg-b-tree (:tree-width options) backend)]
                "sorted-set" ["sorted-set" (sorted-set-repr)])
              flush-freq (:flush-freq options)
              codename (str tree-name
                            "__flush_"
                            flush-freq
                            "__b_"
                            (:tree-width options)
                            "__"
                            (:backend options)
                            "__n_"
                            (:num-operations options)
                            "__del_"
                            (:delete-pattern options))]
          (doseq [ds (generate-test-datasets)
                  :let [codename (str codename
                                      "_"
                                      (:name ds))
                        out (create-output-dir
                              root
                              codename)
                        _ (println "Doing" codename)
                        bench-res (benchmark (:num-operations options) ds flush-freq structure out delete-xform)]]
            (swap! results conj
                   {:tree tree-name
                    :ds (:name ds)
                    :freq flush-freq
                    :n (:num-operations options)
                    :b (:tree-width options)
                    :delete-pattern (:delete-pattern options)
                    :results bench-res}))
          ;(println "results")
          ;(clojure.pprint/pprint @results)
          (swap! outputs conj (template-one-sheet @results)))))
    (excel/render-to-file
      "template_benchmark.xlsx"
      (.getPath (File. root "analysis.xlsx"))
      {"SingleDS"
       (map-indexed (fn [i s]
                      (assoc s :sheet-name (str "Trial " (inc i))))
                    @outputs)})))
