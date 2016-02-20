(ns tree.bench
  (:require [clojure.pprint :as pp]
            [clojure.string :as str]
            [excel-templates.build :as excel]
            [clojure.tools.cli :refer (parse-opts)]
            [tree.redis :as redis]
            [tree.core :as core]
            [tree.messaging :as msg])
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
   :flush (fn [x] (core/flush-tree x backend))})

(defn msg-b-tree
  "Returns a b-tree with msg insert"
  [b backend]
  (let [sqrt-b (long (Math/sqrt b))]
    {:structure (core/b-tree(core/->Config sqrt-b b (- b sqrt-b)))
     :insert msg/insert
     :flush (fn [x] (core/flush-tree x backend))}))

(defn sorted-set-repr
  "Returns a sorted set"
  []
  {:structure (sorted-set)
   :insert conj
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
  [n dataset flush-freq datastruct out]
  (let [{:keys [structure insert flush]} datastruct]
    (loop [[x & data] (take n (:data dataset))
           t 0
           tree structure
           last-flush nil
           i 0
           outputs []]
      (let [i' (inc i)
            {flushed-tree :tree
             stats :stats} (when (zero? (mod i' flush-freq))
                             (flush tree))
            before (System/nanoTime)
            tree' (insert (or flushed-tree tree) x)
            after (System/nanoTime) 
            log-inserts (zero? (mod i' (quot n 100)))
            updated-outputs (atom outputs)]
        (when log-inserts ;; 1000 pieces
          (binding [*out* (:speed out)]
            (let [ks (sort (keys last-flush))
                  avg-ns (float (/ t (quot n 100)))]
              (when (zero? i)
                (println (str "elements,insert_took_avg_ns,"
                              (str/join "," ks))))
              (println (str i' "," avg-ns
                            "," (str/join "," (map #(get last-flush %) ks))))
              (swap! updated-outputs conj (-> (into {} last-flush)
                                              (assoc :avg-ns avg-ns
                                                     :n i))))))
        (if (seq data)
          (recur data
                 (if log-inserts
                   0
                   (+ t (- after before)))
                 tree'
                 (if stats (merge-with + last-flush @stats) last-flush)
                 i'
                 @updated-outputs)
          @updated-outputs)))))

(def options
  [["-n" "--num-operations NUM_OPS" "The number of elements that will be applied to the data structure"
    :default 100000
    :parse-fn #(Long. %)
    :validate [pos? "n must be positive"]]
   [nil "--data-structure" "Which data structure to run the test on"
    :default "fractal"
    :validate [#(#{"fractal" "b-tree" "sorted-set"} %) "Data structure must be fractal, b-tree, or sorted set"]]
   [nil "--backend testing" "Runs the benchmark with the specified backend"
    :default "testing"
    :validate [#(#{"redis" "testing"} %) "Backend must be redis or testing"]] 
   [nil "--sorted-set" "Runs the benchmarks on a sorted set"]
   ["-b" "--tree-width" "Determines the width of the trees. Fractal trees use sqrt(b) child pointers; the rest is for messages."
    :default 300
    :parse-fn #(Long. %)
    :validate [pos? "b must be positive"]]
   ["-f" "--flush-freq FREQ" "After how many operations should the tree get flushed?"
    :default 1000
    :parse-fn #(Long. %)
    :validate [pos? "flush frequency must be positive"]]
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
            ["Usage: bench [options] output-dir"
             ""
             "Options:"
             options-summary
             ""
             "Backends:"
             "testing: this backend serializes nothing, just using an extra indirection"
             "redis: this backend uses a local redis server"]))

(defn make-template-for-one-tree-freq-combo
  [list-of-benchmark-results]
    (clojure.pprint/pprint list-of-benchmark-results)
  (assert (= 2 (count list-of-benchmark-results)) "Should be random and ordered")
  (let [indexed (group-by :ds list-of-benchmark-results)]
    (map #(vector (:n %1) (:avg-ns %1) (:writes %1) (:avg-ns %2) (:writes %2))
         (:results (first (get indexed "in-order")))
         (:results (first (get indexed "random"))))))

(defn template-one-sheet
  [pair-of-results-for-one-ds-config]
  (let [{:keys [tree ds freq n b results]} (first pair-of-results-for-one-ds-config)
        x {;:sheet-name (str (name tree) " " ds " flushed every " freq)
           0 [["Data Structure" (name tree) "" "n" n]]
           1 [["Flush Frequency" freq "" "b" b]]
           [5 104] (make-template-for-one-tree-freq-combo pair-of-results-for-one-ds-config)}]
    x))

(defn -main
  [root & args]
  (doseq [args (->> args
                    (partition-by #(= % "--"))
                    (map-indexed vector)
                    (filter (comp even? first))
                    (map second))]
    (let [{:keys [options arguments errors summary]} (parse-opts args options)
          tree-to-test (atom {})
          results (atom [])]
      (cond
        (:help options) (exit 0 (usage summary))
        (not= (count arguments) 0) (exit 1 (usage summary))
        errors (exit 1 (error-msg errors)))
      (let [backend (case (:backend options)
                      "testing" (core/->TestingBackend)
                      "redis" (redis/->RedisBackend
                                #_(java.util.concurrent.Executors/newFixedThreadPool 4)))
            [tree-name structure]
            (case (:data-structure options)
              "b-tree" ["b-tree" (core-b-tree (:tree-width options) backend)]
              "fractal" ["fractal" (msg-b-tree (:tree-width options) backend)]
              "sorted-set" ["sorted-set" (sorted-set-repr)])
            flush-freq (:flush-freq options)
            codename (str tree-name
                          "_flush"
                          flush-freq
                          "_b"
                          (:tree-width options)
                          "_"
                          (:backend options)
                          "_n"
                          (:num-operations options))]
        (doseq [ds (generate-test-datasets)
                :let [codename (str codename
                                    "_"
                                    (:name ds))
                      out (create-output-dir
                            root
                            codename)]]
                (println "Doing" codename)
          (swap! results conj 
                 {:tree tree-name
                  :ds (:name ds)
                  :freq flush-freq
                  :n (:num-operations options)
                  :b (:tree-width options)
                  :results (benchmark (:num-operations options) ds flush-freq structure out)}))
      (excel/render-to-file
        "template_benchmark.xlsx"
        (.getPath (File. root (str codename "_analysis.xlsx")))
        {"SingleDS"
         (template-one-sheet @results)})))))
