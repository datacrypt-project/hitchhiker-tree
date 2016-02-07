(ns tree.core-test
  (:refer-clojure :exclude [compare resolve]) 
  (:require [clojure.test :refer :all]
            [tree.core :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check.clojure-test :refer (defspec)]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]))

(deftest simple-read-only-behavior
  (testing "scan-children-array"
    (are [x y] (= (scan-children-array [1 2 5 6] x) y)
         -10 0
         1 0
         3 2
         5 2
         10 4))
  (testing "Basic searches"
    (let [data1 (->DataNode [1 2 3 4 5])
          data2 (->DataNode [6 7 8 9 10])
          root (->RootNode [5] [data1 data2])]
      (is (= (lookup-key root -10) 1) "first key must be LEQ than search key to go past the first elt")
      (is (= (lookup-key root 100) 10) "last key is still LEQ than search key")
      (dotimes [i 10]
        (is (= (lookup-key root (inc i)) (inc i))))))
  (testing "basic fwd iterator"
    (let [data1 (->DataNode [1 2 3 4 5])
          data2 (->DataNode [6 7 8 9 10])
          root (->RootNode [5] [data1 data2])]
      (is (= (lookup-fwd-iter root 4) (range 4 11)))
      (is (= (lookup-fwd-iter root 0) (range 1 11))))))

(def added-keys-appear-in-order
  (prop/for-all [v (gen/vector gen/pos-int)]
                (let [sorted-set-order (into (sorted-set) v)
                      b-tree (reduce insert-key (empty-b-tree) v)
                      b-tree-order (lookup-fwd-iter b-tree Integer/MIN_VALUE)]
                  (= (seq sorted-set-order) b-tree-order))))

(defspec b-tree-sorts-uniques-random-int-vector
  added-keys-appear-in-order)

(comment
  (scan-children-array [0] 0)
  (clojure.pprint/pprint
    (reduce insert-key (empty-b-tree) [5 6 7 8 0 2 3 4 1 6])
    )
  (let [v #_[1 2 ]
        #_[8 0 9 1 10 14 15 2 16 3 17 18 4 11 19 20 7 6 12 13 5 9]
        [0 5 6 1 7 11 17 12 13 18 2 3 19 20 8 9 14 10 15 4 16 18]]
                (let [sorted-set-order (into (sorted-set) v)
                      b-tree (reduce insert-key (empty-b-tree) v)
                      b-tree-order (lookup-fwd-iter b-tree Integer/MIN_VALUE)
                      
                      ]
                  (clojure.pprint/pprint b-tree)
                  ;(println "path" (lookup-path b-tree Integer/MIN_VALUE)) 
                  ;(println "order" (lookup-fwd-iter b-tree Integer/MIN_VALUE))
                  (println (seq sorted-set-order))
                  (println b-tree-order)
                  (println (= (seq sorted-set-order)
                              b-tree-order))
                  ))

  (tc/quick-check 100 added-keys-appear-in-order))

()

(deftest insert-test
  (let [data1 (->DataNode [1 2 3 4])
        root (->RootNode [] [data1])]
    (is (= (lookup-fwd-iter (insert-key root 3) -10) [1 2 3 4]))
    (are [x] (= (lookup-fwd-iter (insert-key root x) -10) (sort (conj [1 2 3 4] x)))
         0
         2.5
         5))
  (let [data1 (->DataNode [1 2 3 4 5])
        root (->RootNode [] [data1])]
    (are [x y] (= (lookup-fwd-iter (insert-key root x) y)
                  (drop-while
                    #(< % y)
                    (sort (conj [1 2 3 4 5] x))))
         0 3
         2.5 2
         5.5 3)))
