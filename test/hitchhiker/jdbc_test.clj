(ns hitchhiker.jdbc-test
  (:require [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hitchhiker.jdbc :as jdbc]
            [hitchhiker.tree.core :as core]
            hitchhiker.tree.core-test
            [hitchhiker.tree.messaging :as msg]))

(defn insert
  [t k]
  (msg/insert t k k))

(defn lookup-fwd-iter
  [t v]
  (seq (map first (msg/lookup-fwd-iter t v))))

(defn mixed-op-seq
  "This is like the basic mixed-op-seq tests, but it also mixes in flushes to sqlite
   and automatically deletes the old tree"
  [add-freq del-freq flush-freq universe-size num-ops]
  (let [db (jdbc/find-or-create-db "/tmp/yolo.sqlite")]
    (prop/for-all [ops (gen/vector (gen/frequency
                                    [[add-freq (gen/tuple (gen/return :add)
                                                          (gen/no-shrink gen/int))]
                                     [flush-freq (gen/return [:flush])]
                                     [del-freq (gen/tuple (gen/return :del)
                                                          (gen/no-shrink gen/int))]])
                                   40)]
      (assert (empty? (jdbc/list-keys db))
              "Start with no keys")
      (let [[b-tree root set]
            (reduce (fn [[t root set] [op x]]
                      (let [x-reduced (when x (mod x universe-size))]
                        (condp = op
                          :flush (let [t (:tree (core/flush-tree t (jdbc/->JDBCBackend db)))]
                                   (when root
                                     (jdbc/drop-key db root))
                                   #_(println "flush" root)
                                   [t @(:storage-addr t) set])
                          :add (do #_(println "add" x) [(insert t x-reduced) root (conj set x-reduced)])
                          :del (do #_(println "del" x) [(msg/delete t x-reduced) root (disj set x-reduced)]))))
                    [(core/b-tree (core/->Config 3 3 2)) nil #{}]
                    ops)]
        #_(println "Make it to the end of a test," root "has" (count (lookup-fwd-iter b-tree -1)) "keys left")
        (let [b-tree-order (lookup-fwd-iter b-tree -1)
              res (= b-tree-order (seq (sort set)))]

          (jdbc/drop-key db root)
          (assert (empty? (jdbc/list-keys db))
                  "End with no keys")

          (assert res (str "These are unequal: " (pr-str b-tree-order) " " (pr-str (seq (sort set)))))
          res)))))

(defspec test-many-keys-bigger-trees
  100
  (mixed-op-seq 800 200 10 1000 1000))
