(ns tree.core-test
  (:refer-clojure :exclude [compare resolve]) 
  (:require [clojure.test :refer :all]
            [tree.core :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check.clojure-test :refer (defspec)]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]))


(deftest simple-read-only-behavior
  (testing "Basic searches"
    (let [data1 (->DataNode [1 2 3 4 5])
          data2 (->DataNode [6 7 8 9 10])
          root (->IndexNode [5] [data1 data2])]
      (is (= (lookup-key root -10) 1) "first key must be LEQ than search key to go past the first elt")
      (is (= (lookup-key root 100) 10) "last key is still LEQ than search key")
      (dotimes [i 10]
        (is (= (lookup-key root (inc i)) (inc i))))))
  (testing "basic fwd iterator"
    (let [data1 (->DataNode [1 2 3 4 5])
          data2 (->DataNode [6 7 8 9 10])
          root (->IndexNode [5] [data1 data2])]
      (is (= (lookup-fwd-iter root 4) (range 4 11)))
      (is (= (lookup-fwd-iter root 0) (range 1 11))))))

(def added-keys-appear-in-order
  (prop/for-all [v (gen/vector gen/int)]
                (let [sorted-set-order (into (sorted-set) v)
                      b-tree (reduce insert (empty-b-tree) v)
                      b-tree-order (lookup-fwd-iter b-tree Integer/MIN_VALUE)]
                  (= (seq sorted-set-order) b-tree-order))))

(defspec b-tree-sorts-uniques-random-int-vector
  1000
  added-keys-appear-in-order)

(defspec test-insert-into-sorted-vector
  1000
  (prop/for-all [set (gen/set gen/int)
                 resample? gen/boolean
                 e gen/int]
                (let [body (vec (sort set))
                      e (if resample? (first set) e)]
                  (= (sort (conj set e))
                     (-insertion-into-sorted-vector body e)))))

(defspec test-delete-from-sorted-vector
  1000
  (prop/for-all [set (gen/set gen/int)
                 resample? gen/boolean
                 e gen/int]
                (let [body (vec (sort set))
                      e (if resample? (first set) e)]
                  (= (sort (disj set e))
                     (-deletion-from-sorted-vector body e)))))

(defspec test-insert
  1000
  (prop/for-all [v (gen/vector gen/int)]
                (let [sorted-set-order (into (sorted-set) v)
                      b-tree (reduce insert (empty-b-tree) v)
                      b-tree-order (lookup-fwd-iter b-tree Integer/MIN_VALUE)]
                  (= (seq sorted-set-order) b-tree-order)))) 

(defspec test-delete2
  1000
  (prop/for-all [the-set (gen/vector-distinct gen/int)
                 num gen/nat]
                (let [set-a (sort the-set)
                      set-b (take num the-set)
                      b-tree (reduce insert (empty-b-tree) set-a)
                      b-tree-without (reduce delete b-tree set-b)
                      b-tree-order (lookup-fwd-iter b-tree-without Integer/MIN_VALUE)]
                  (= (seq (remove (set set-b) set-a)) b-tree-order))))

(deftest insert-test
  (let [data1 (->DataNode [1 2 3 4])
        root (->IndexNode [] [data1])]
    (is (= (lookup-fwd-iter (insert root 3) -10) [1 2 3 4]))
    (are [x] (= (lookup-fwd-iter (insert root x) -10) (sort (conj [1 2 3 4] x)))
         0
         2.5
         5))
  (let [data1 (->DataNode [1 2 3 4 5])
        root (->IndexNode [] [data1])]
    (are [x y] (= (lookup-fwd-iter (insert root x) y)
                  (drop-while
                    #(< % y)
                    (sort (conj [1 2 3 4 5] x))))
         0 3
         2.5 2
         5.5 3)))

(deftest simple-delete-tests
  (let [tree (reduce insert (empty-b-tree) (range 5))]
    (is (= (lookup-fwd-iter (delete tree 3) 0) [0 1 2 4])))
  (let [tree (reduce insert (empty-b-tree) (range 10))]
    (is (= (lookup-fwd-iter (delete tree 0) 0) (map inc (range 9)))))
  (let [tree (reduce insert (empty-b-tree) (range 6))]
    (is (= (lookup-fwd-iter (delete tree 0) 0) (map inc (range 5))))))

;;TODO make structure-verification tests that run a bunch of ops, then confirm the tree still looks OK--data and index nodes should have the right # of elements, with perfect balance on the tree
