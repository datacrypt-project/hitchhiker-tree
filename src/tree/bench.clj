(ns tree.bench
  (:require [clojure.pprint :as pp]
            [clojure.string :as str]
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
  [b]
  {:structure (core/b-tree (core/->Config b b 0))
   :insert core/insert
   :flush (fn [x] (core/flush-tree x (core/->TestingBackend)))})

(defn msg-b-tree
  "Returns a b-tree with msg insert"
  [b]
  (let [sqrt-b (long (Math/sqrt b))]
    {:structure (core/b-tree(core/->Config sqrt-b b (- b sqrt-b)))
     :insert msg/insert
     :flush (fn [x] (core/flush-tree x (core/->TestingBackend)))}))

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
           i 0]
      (let [i' (inc i)
            {flushed-tree :tree
             stats :stats} (when (zero? (mod i' flush-freq))
                             (flush tree))
            before (System/nanoTime)
            tree' (insert (or flushed-tree tree) x)
            after (System/nanoTime) 
            log-inserts (zero? (mod i' (quot n 100)))]
        (when log-inserts ;; 1000 pieces
          (binding [*out* (:speed out)]
            (let [ks (sort (keys last-flush))]
              (when (zero? i)
                (println (str "elements,insert_took_avg_ns,"
                              (str/join "," ks))))
              (println (str i' "," (float (/ t (quot n 100)))
                            "," (str/join "," (map #(get last-flush %) ks)))))))
        (when (seq data)
          (recur data
                 (if log-inserts
                   0
                   (+ t (- after before)))
                 tree'
                 (if stats @stats last-flush)
                 i'))))))

(defn -main
  [& args]
  (let [output (first args)
        trees {:core (core-b-tree 300)
               :msg (msg-b-tree 300)
               :set (sorted-set-repr)}]
    (doseq [tree (keys trees)
            ds (generate-test-datasets)
            flush-freq [100]
            :let [out (create-output-dir
                        output
                        (str (name tree)
                             "_"
                             (:name ds)
                             "_"
                             flush-freq))]]
      (println "Doing the" tree "on the" (:name ds) "dataset, flushing every" flush-freq)
      (benchmark 1000000 ds flush-freq (get trees tree) out))))
