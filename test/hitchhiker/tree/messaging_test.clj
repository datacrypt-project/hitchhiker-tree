(ns hitchhiker.tree.messaging-test
  (:require [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hitchhiker.tree.core :refer [go-try <??] :as core]
            hitchhiker.tree.core-test
            [hitchhiker.tree.messaging :as msg]))

(defn insert
  [t k]
  (<?? (msg/insert t k k)))

(defn lookup-fwd-iter
  [t v]
  (seq (map first (msg/lookup-fwd-iter t v))))

(defspec b-tree-sorts-uniques-random-int-vector
  1000
  (prop/for-all [v (gen/vector gen/int)]
                (let [sorted-set-order (into (sorted-set) v)
                      b-tree (reduce insert (<?? (core/b-tree (core/->Config 3 3 2))) v)
                      b-tree-order (lookup-fwd-iter b-tree Integer/MIN_VALUE)]
                  (= (seq sorted-set-order) b-tree-order))))

(defspec test-insert
  1000
  (prop/for-all [v (gen/vector gen/int)]
                (let [sorted-set-order (into (sorted-set) v)
                      b-tree (reduce insert (<?? (core/b-tree (core/->Config 3 3 2))) v)
                      b-tree-order (lookup-fwd-iter b-tree Integer/MIN_VALUE)]
                  (= (seq sorted-set-order) b-tree-order))))

(defspec test-delete2
  1000
  (prop/for-all [the-set (gen/vector-distinct gen/int)
                 num gen/nat]
                (let [set-a (sort the-set)
                      set-b (take num the-set)
                      b-tree (reduce insert (<?? (core/b-tree (core/->Config 3 3 2))) set-a)
                      b-tree-without (reduce #(<?? (msg/delete %1 %2)) b-tree set-b)
                      b-tree-order (lookup-fwd-iter b-tree-without Integer/MIN_VALUE)]
                  (= (seq (remove (set set-b) set-a)) b-tree-order))))

;(require '[clojure.pprint :refer (pprint)])
;(pprint (reduce msg/delete (reduce insert (core/b-tree (core/->Config 3 3 2)) [0 5 2 1 4 -1 3]) [0 5]))

(defspec test-balanced-after-many-inserts
  1000
  (prop/for-all [the-set (gen/vector (gen/no-shrink gen/int))]
                (let [b-tree (reduce insert (<?? (core/b-tree (core/->Config 3 3 2))) the-set)]
                  (hitchhiker.tree.core-test/check-node-is-balanced b-tree))))

(defspec test-wider-balanced-after-many-inserts
  1000
  (prop/for-all [the-set (gen/vector (gen/no-shrink gen/int))]
                (let [b-tree (reduce insert (<?? (core/b-tree (core/->Config 200 220 17))) the-set)]
                  (hitchhiker.tree.core-test/check-node-is-balanced b-tree))))

;; This test will show how if you apply killerop to the b-tree result, it corrupts
;; the tree by losing track of the element 20
(comment
  (let [split-idx (+ 125 #_270)
        all-ops (->> (read-string (slurp "broken-data.edn"))
                     (map (fn [[op x]] [op (mod x 100000)])))
        ops (->> all-ops
                 (drop-last split-idx))
        killer-op (->> all-ops
                       (take-last split-idx)
                       first)
        killer-op-dos (->> all-ops
                       (take-last split-idx)
                       second)
        ]
    (let [[b-tree s] (reduce (fn [[t s] [op x]]
                               (let [x-reduced (mod x 100000)]
                                 (condp = op
                                   :add [(insert t x-reduced)
                                         (conj s x-reduced)]
                                   :del [(msg/delete t x-reduced)
                                         (disj s x-reduced)])))
                             [(<?? (core/b-tree (core/->Config 3 3 2))) #{}]
                             ops)
          f #(case (first %1) :add (insert %2 (second %1))
               :del (msg/delete %2 (second %1)))
          ]
      ;  (println ops)
      (println killer-op)
      (clojure.pprint/pprint b-tree)
      (println (lookup-fwd-iter b-tree -1))
      (println (sort s))
      (def cool-test-tree b-tree)
      ;; It appears that the insert op is leapfrogging the pending delete op
      (clojure.pprint/pprint (f killer-op b-tree))
      (println (lookup-fwd-iter (f killer-op b-tree) -1))
      (println (sort (disj s (second killer-op))))
      (when killer-op-dos
        (println killer-op-dos)
        (clojure.pprint/pprint (f killer-op-dos (f killer-op b-tree)))))
    )

  (clojure.pprint/pprint cool-test-tree)
  (clojure.pprint/pprint (insert cool-test-tree 20))
  (clojure.pprint/pprint (msg/delete cool-test-tree 32))
  )

(defn mixed-op-seq
  "Returns a property that ensures trees produced by a sequence of adds and deletes
   in the given ratio, with universe-size distinct values"
  [add-vs-del-ratio universe-size num-ops]
  (let [add-freq (long (* 1000 add-vs-del-ratio))
        del-freq (long (* 1000 (- 1 add-vs-del-ratio)))]
    (prop/for-all [ops (gen/vector (gen/frequency
                                     [[add-freq (gen/tuple (gen/return :add)
                                                           (gen/no-shrink gen/int))]
                                      [del-freq (gen/tuple (gen/return :del)
                                                           (gen/no-shrink gen/int))]])
                                   num-ops)]
                  (let [[b-tree s] (reduce (fn [[t s] [op x]]
                                             (let [x-reduced (mod x universe-size)]
                                               (condp = op
                                                 :add [(insert t x-reduced)
                                                       (conj s x-reduced)]
                                                 :del [(<?? (msg/delete t x-reduced))
                                                       (disj s x-reduced)])))
                                           [(<?? (core/b-tree (core/->Config 3 3 2))) #{}]
                                           ops)]
                    ;                  (println ops)
                    (and (= (lookup-fwd-iter b-tree -1) (seq (sort s)))
                         (hitchhiker.tree.core-test/check-node-is-balanced b-tree))))))

(comment
  (let [data (read-string (slurp "broken-data2.edn"))
        universe-size 1000
        ]
    (let [[b-tree s] (reduce (fn [[t s] [op x]]
                               (let [x-reduced (mod x universe-size)]
                                 (condp = op
                                   :add [(insert t x-reduced)
                                         (conj s x-reduced)]
                                   :del [(msg/delete t x-reduced)
                                         (disj s x-reduced)])))
                             [(<?? (core/b-tree (core/->Config 3 3 2))) #{}]
                             data)]
      ;                  (println ops)
      (println (lookup-fwd-iter b-tree -1))
      (println (sort s))
           )
    )
  )

(defspec test-few-keys-many-ops
  50
  (mixed-op-seq 0.5 250 5000))

(defspec test-many-keys-bigger-trees
  1000
  (mixed-op-seq 0.8 1000 1000))

(defspec test-sparse-ops
  100
  (mixed-op-seq 0.7 100000 1000))
