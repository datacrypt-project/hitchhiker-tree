(ns hitchhiker.redis-test
  (:require [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hitchhiker.redis :as redis]
            [hitchhiker.tree.core :refer [<??] :as core]
            hitchhiker.tree.core-test
            [hitchhiker.tree.messaging :as msg]
            [taoensso.carmine :as car :refer [wcar]]))

(defn insert
  [t k]
  (msg/insert t k k))

(defn lookup-fwd-iter
  [t v]
  (seq (map first (msg/lookup-fwd-iter t v))))

(defn mixed-op-seq
  "This is like the basic mixed-op-seq tests, but it also mixes in flushes to redis
   and automatically deletes the old tree"
  [add-freq del-freq flush-freq universe-size num-ops]
  (prop/for-all [ops (gen/vector (gen/frequency
                                   [[add-freq (gen/tuple (gen/return :add)
                                                         (gen/no-shrink gen/int))]
                                    [flush-freq (gen/return [:flush])]
                                    [del-freq (gen/tuple (gen/return :del)
                                                         (gen/no-shrink gen/int))]])
                                 num-ops)]
                (assert (let [ks (wcar {} (car/keys "*"))]
                          (or (empty? ks)
                              (= ["refcount:expiry"] ks)))
                        "Start with no keys")
                (let [[b-tree root set]
                      (reduce (fn [[t root set] [op x]]
                                       (let [x-reduced (when x (mod x universe-size))]
                                         (condp = op
                                           :flush (let [t (:tree (<?? (core/flush-tree t (redis/->RedisBackend))))]
                                                    (when root
                                                      (wcar {} (redis/drop-ref root)))
                                                    #_(println "flush")
                                                    [t (<?? (:storage-addr t)) set])
                                           :add (do #_(println "add") [(<?? (insert t x-reduced)) root (conj set x-reduced)])
                                           :del (do #_(println "del") [(<?? (msg/delete t x-reduced)) root (disj set x-reduced)]))))
                              [(<?? (core/b-tree (core/->Config 3 3 2))) nil #{}]
                              ops)]
                  #_(println "Make it to the end of a test, tree has" (count (lookup-fwd-iter b-tree -1)) "keys left")
                  (let [b-tree-order (lookup-fwd-iter b-tree -1)
                        res (= b-tree-order (seq (sort set)))]
                    (wcar {} (redis/drop-ref root))
                    (assert (let [ks (wcar {} (car/keys "*"))]
                              (or (empty? ks)
                                  (= ["refcount:expiry"] ks))) "End with no keys")
                    (assert res (str "These are unequal: " (pr-str b-tree-order) " " (pr-str (seq (sort set)))))
                    res))))

(defspec test-many-keys-bigger-trees
  100
  (mixed-op-seq 800 200 10 1000 1000))

(comment
  (test-many-keys-bigger-trees)


  (count (remove  (reduce (fn [t [op x]]
                            (let [x-reduced (when x (mod x 1000))]
                              (condp = op
                                :flush t
                                :add (conj t x-reduced)
                                :del (disj t x-reduced))))
                          #{}
                          (drop-last 2 opseq)) (lookup-fwd-iter (msg/delete test-tree -33) 0)))
  (:op-buf test-tree)
  (count (sort (reduce (fn [t [op x]]
                         (let [x-reduced (when x (mod x 1000))]
                           (condp = op
                             :flush t
                             :add (conj t x-reduced)
                             :del (disj t x-reduced))))
                       #{}
                       opseq)))


  (let [ops (->> (read-string (slurp "broken-data.edn"))
                 (map (fn [[op x]] [op (mod x 100000)]))
                 (drop-last  125))]
    (let [[b-tree s] (reduce (fn [[t s] [op x]]
                               (let [x-reduced (mod x 100000)]
                                 (condp = op
                                   :add [(insert t x-reduced)
                                         (conj s x-reduced)]
                                   :del [(msg/delete t x-reduced)
                                         (disj s x-reduced)])))
                             [(core/b-tree (core/->Config 3 3 2)) #{}]
                             ops)]
      (println ops)
      (println (->> (read-string (slurp "broken-data.edn"))
                    (map (fn [[op x]] [op (mod x 100000)]))
                    (take-last  125)
                    first))
      (println (lookup-fwd-iter b-tree -1))
      (println (sort s))
      ))
  (defn trial []
    (let [opseq (read-string (slurp "broken-data.edn"))
          [b-tree root] (reduce (fn [[t root] [op x]]
                                  (let [x-reduced (when x (mod x 1000))]
                                    (condp = op
                                      :flush (let [_ (println "About to flush...")
                                                   t (:tree (core/flush-tree t (redis/->RedisBackend)))]
                                               (when root
                                                 (wcar {} (redis/drop-ref root)))
                                               (println "flushed")
                                               [t @(:storage-addr t)])
                                      :add (do (println "about to add" x-reduced "...")
                                               (let [x [(insert t x-reduced) root]]
                                                 (println "added") x
                                                 ))
                                      :del (do (println "about to del" x-reduced "...")
                                               (let [x [(msg/delete t x-reduced) root]]
                                                 (println "deled") x)))))
                                [(core/b-tree (core/->Config 3 3 2))]
                                opseq)]
      (def test-tree b-tree)
      (println "Got diff"
               (count (remove  (reduce (fn [t [op x]]
                                         (let [x-reduced (when x (mod x 1000))]
                                           (condp = op
                                             :flush t
                                             :add (conj t x-reduced)
                                             :del (disj t x-reduced))))
                                       #{}
                                       opseq) (lookup-fwd-iter test-tree 0))))
      (println "balanced?" (hitchhiker.tree.core-test/check-node-is-balanced test-tree))
      (def my-root root)))

  (map #(and (second %) (mod (second %) 1000)) opseq)


  (def opseq (read-string (io/resource "redis_test_data.clj"))))
