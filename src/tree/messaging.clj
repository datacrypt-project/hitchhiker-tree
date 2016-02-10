(ns tree.messaging
  (:refer-clojure :exclude [subvec])
  (:require [tree.core :as core]
            [clojure.core.rrb-vector :refer (catvec subvec)])
  (:import java.io.Writer
           java.util.Collections)) 

(def op-buf-size 2)

;; An operation is an object with a few functions
;; 1. It has a function that it applies to the tree to apply its effect
;; In the future, it could also have
;; 2. It has a promise which can be filled with the end result
;;       (more memory but faster results for repeat queries)

(defprotocol IOperation
  (affects-key [op] "Which key this affects--currently must be a single key")
  (apply-op [op tree] "Applies the operation to the tree"))

(declare insert delete)

(defrecord InsertOp [key]
  IOperation
  (affects-key [_] key)
  (apply-op [_ tree] (core/insert tree key)))

(defrecord DeleteOp [key]
  IOperation
  (affects-key [_] key)
  (apply-op [_ tree] (core/delete tree key)))

(defn enqueue
  "When enqueing "
  ([tree msgs]
   (let [deferred-ops (atom [])
         msg-buffers-propagated (enqueue tree msgs deferred-ops)]
     (reduce (fn [tree op]
               (apply-op op tree))
             msg-buffers-propagated
             @deferred-ops)))
  ([tree msgs deferred-ops]
   (let [tree (core/resolve tree)]
     (cond
       (core/data-node? tree) ; need to return ops to apply to the tree proper...
       (do (swap! deferred-ops into msgs)
           tree)
       (<= (+ (count msgs) (count (:op-buf tree))) op-buf-size) ; will there be enough space
       (-> tree
           (core/dirty!)
           (update-in [:op-buf] into msgs))
       :else ; overflow, should be IndexNode
       (do (assert (core/index-node? tree))
           (loop [[child & children] (:children tree)
                  rebuilt-children []
                  msgs (vec (sort-by affects-key ;must be a stable sort 
                                     (concat msgs (:op-buf tree))))]
             (let [;; Do a binary search to which msgs belong child
                   ;; and which msgs belong to the next child
                   binsearch-result
                   (Collections/binarySearch
                     msgs
                     {:key (core/last-key child)}
                     (fn [x y]
                       (core/compare (:key x) (:key y))))
                   negative-binsearch-result (- (inc binsearch-result))

                   ;; Which messages should we apply?
                   [took-msgs extra-msgs]
                   (if (neg? binsearch-result)
                     [(subvec msgs 0 negative-binsearch-result) ;not found, exclusive
                      (subvec msgs negative-binsearch-result)]
                     [(subvec msgs 0 (inc binsearch-result)) ;found, inclusive
                      (subvec (inc binsearch-result))])

                   on-the-last-child? (empty? children)

                   ;; Any changes to the current child?
                   new-child
                   (cond
                     on-the-last-child?
                     (enqueue (core/resolve child)
                              (catvec took-msgs extra-msgs)
                              deferred-ops)
                     (empty? took-msgs) ; save a write
                     child
                     :else
                     (enqueue (core/resolve child)
                              took-msgs
                              deferred-ops))]

               (if on-the-last-child?
                 (-> tree
                     (assoc :children (conj rebuilt-children new-child))
                     (assoc :op-buf [])
                     (core/dirty!))
                 (recur children (conj rebuilt-children new-child) extra-msgs)))))))))

(defn insert
  (tree key)
  (enqueue tree [(->InsertOp key)]))

(clojure.pprint/pprint
  (-> (apply core/b-tree (range 30))
      (core/flush-tree) :tree
      (enqueue [(->InsertOp -1)])  
      (enqueue [(->InsertOp -2)])  
      (enqueue [(->InsertOp 50)])  
      (enqueue [(->InsertOp -4)])  
      (enqueue [(->InsertOp -5)]) ; tree is totally filled up here
      (enqueue [(->InsertOp -8)])
      ))

;;TODO delete in core needs to stop using the index-node constructor to be more
;;careful about how we handle op-bufs during splits and merges.
;;
;;After we've got delete working, lookup, pred, and succ should be fixed
;;
;;broadcast nodes will need IDs so that they can combine during merges...
;;


;; Do the lookup
;; Get all operations in total order
;; scan for all "interesting" operations
;; apply those operations

(defn lookup
  [tree key]
  )

