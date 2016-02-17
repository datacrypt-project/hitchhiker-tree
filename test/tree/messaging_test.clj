(ns tree.messaging-test
  (:require [clojure.test :refer :all]
            [tree.core :as core]
            [tree.messaging :as msg]
            [tree.core-test]
            [clojure.test.check :as tc]
            [clojure.test.check.clojure-test :refer (defspec)]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]))

(defspec b-tree-sorts-uniques-random-int-vector
  1000
  (prop/for-all [v (gen/vector gen/int)]
                (let [sorted-set-order (into (sorted-set) v)
                      b-tree (reduce msg/insert (core/b-tree (core/->Config 3 3 2)) v)
                      b-tree-order (msg/lookup-fwd-iter b-tree Integer/MIN_VALUE)]
                  (= (seq sorted-set-order) b-tree-order))))

(defspec test-insert
  1000
  (prop/for-all [v (gen/vector gen/int)]
                (let [sorted-set-order (into (sorted-set) v)
                      b-tree (reduce msg/insert (core/b-tree (core/->Config 3 3 2)) v)
                      b-tree-order (msg/lookup-fwd-iter b-tree Integer/MIN_VALUE)]
                  (= (seq sorted-set-order) b-tree-order)))) 

(defspec test-delete2
  1000
  (prop/for-all [the-set (gen/vector-distinct gen/int)
                 num gen/nat]
                (let [set-a (sort the-set)
                      set-b (take num the-set)
                      b-tree (reduce msg/insert (core/b-tree (core/->Config 3 3 2)) set-a)
                      b-tree-without (reduce msg/delete b-tree set-b)
                      b-tree-order (msg/lookup-fwd-iter b-tree-without Integer/MIN_VALUE)]
                  (= (seq (remove (set set-b) set-a)) b-tree-order))))

;(require '[clojure.pprint :refer (pprint)])
;(pprint (reduce msg/delete (reduce msg/insert (core/b-tree (core/->Config 3 3 2)) [0 5 2 1 4 -1 3]) [0 5]))

(defspec test-balanced-after-many-inserts
  1000
  (prop/for-all [the-set (gen/vector (gen/no-shrink gen/int))]
                (let [b-tree (reduce msg/insert (core/b-tree (core/->Config 3 3 2)) the-set)]
                  (tree.core-test/check-node-is-balanced b-tree))))

(defspec test-wider-balanced-after-many-inserts
  1000
  (prop/for-all [the-set (gen/vector (gen/no-shrink gen/int))]
                (let [b-tree (reduce msg/insert (core/b-tree (core/->Config 200 220 17)) the-set)]
                  (tree.core-test/check-node-is-balanced b-tree))))

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
                  (let [b-tree (reduce (fn [t [op x]]
                                         (let [x-reduced (mod x universe-size)]
                                           (condp = op
                                             :add (msg/insert t x-reduced)
                                             :del (msg/delete t x-reduced))))
                                       (core/b-tree (core/->Config 3 3 2))
                                       ops)]
  ;                  (println ops)
                    (tree.core-test/check-node-is-balanced b-tree)))))

(defspec test-few-keys-many-ops
  50
  (mixed-op-seq 0.5 250 5000))

(defspec test-many-keys-bigger-trees
  1000
  (mixed-op-seq 0.8 1000 1000))

(defspec test-sparse-ops
  1000
  (mixed-op-seq 0.7 100000 1000))
