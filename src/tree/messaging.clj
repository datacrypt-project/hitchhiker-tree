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
  (apply-op-to-vector [op v] "Applies the operation to the vector")
  (apply-op-to-tree [op tree] "Applies the operation to the tree"))

(defrecord InsertOp [key]
  IOperation
  (affects-key [_] key)
  (apply-op-to-vector [_ v] (core/-insertion-into-sorted-vector v key))
  (apply-op-to-tree [_ tree] (core/insert tree key)))

(defrecord DeleteOp [key]
  IOperation
  (affects-key [_] key)
  (apply-op-to-vector [_ v] (core/-deletion-from-sorted-vector v key))
  (apply-op-to-tree [_ tree] (core/delete tree key)))

(defn enqueue
  "When enqueing "
  ([tree msgs]
   (let [deferred-ops (atom [])
         msg-buffers-propagated (enqueue tree msgs deferred-ops)]
     (reduce (fn [tree op]
               (apply-op-to-tree op tree))
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
                      (subvec msgs (inc binsearch-result))])

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
  [tree key]
  (enqueue tree [(->InsertOp key)]))

(comment
  (defn trial
    []
    (let [tree (-> (apply core/b-tree (range 10000))
                (core/flush-tree)
                :tree)
          new-keys (repeatedly 30 #(- (rand-int 1000) 500))
          reg (-> (reduce core/insert tree new-keys)
                  (core/flush-tree)
                  :stats
                  :writes)
          op-bufs (-> (reduce insert tree new-keys)
                      (core/flush-tree)
                      :stats
                      :writes)]
      (float (/ op-bufs reg))))


  (let [trials (sort (repeatedly 100 trial))]
    (println "avg" (/ (apply + trials) (count trials)))
    (doseq [quantile [0 24 49 74 99]]
      (println "Quantile" quantile "was" (nth trials quantile))
      )
    )
  )

#_(clojure.pprint/pprint
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

;;TODO we'll need to totally redo `apply-ops-in-path`
;;luckily, here's the scoop on the algorithm:
;;The idea is that once we've found a path, we now have a vector of data and
;;the operations we've found along the path. All we need to do is combine those
;;operations into the data-vector and we can finish our queries on the combined
;;vector.
;;
;;You may think that you could just apply every operation on the path to the vector.
;;This would only work for point queries--if you tried to do any sort of range query,
;;you'd see anomalies like operations in the root node appearing in every datanode
;;in the tree! So, we need to prune down all those operations into only the ones we
;;should apply to this data node.
;;
;;The idea here is that we need to apply every operation which affects a key less
;;than or equal to the data node's largest key to this node, while not applying
;;operations which are less than or equal to its left sibling.
;;
;;Although the data-node may not have a direct left sibling, one of its parents
;;could, so we must find the biggest left sibling's last key.
;;
;;We also need a special case to determine if we're the rightmost node, in which
;;case we cannot ignore operations for keys bigger than our own.
;;every 

(defn apply-ops-in-path
  "Takes time proportional to the # of operations to apply them directly
   to the data node at the bottom. Each insert will be lg time into the data
   node, and there are at most lg_b(n)*op-buf-max operations to be applied.

   Returns a sorted vector which is the flushed version of the path's terminus
   
   TODO We could reduce this to only do ops of interest"
  [path]
  (let [ops (->> path
                 (into [] (comp (filter core/index-node?)
                                (map :op-buf)))
                 (rseq) ; highest node should be last in seq
                 (apply catvec)
                 (sort-by affects-key)) ;must be a stable sort
        this-node-index (-> path pop peek)
        parent (-> path pop pop peek)
        is-first? (zero? this-node-index)
        ;;We'll need to find the smallest last-key of the left siblings along the path
        [left-sibs-on-path is-last?]
        (loop [path path
               is-last? true
               left-sibs []]
          (if (= 1 (count path)) ; are we at the root?
            [left-sibs is-last?]
            (let [this-node-index (-> path pop peek)
                  parent (-> path pop pop peek)
                  is-first? (zero? this-node-index)
                  local-last? (= (-> parent :children count dec)
                                   this-node-index)]
              (if is-first?
                (recur (pop (pop path)) (and is-last? local-last?) left-sibs)
                (recur (pop (pop path))
                       (and is-last? local-last?)
                       (conj left-sibs
                             (nth (:children parent)
                                  (dec this-node-index))))))))
        left-sibs-min-last (when (seq left-sibs-on-path)
                             (->> left-sibs-on-path
                                  (map core/last-key)
                                  (apply max)))
        left-sib-filter (if left-sibs-min-last
                          (drop-while #(>= 0 (core/compare (affects-key %)
                                                           left-sibs-min-last)))
                          identity)
        data-node (peek path) 
        my-last (core/last-key data-node)
        right-side-filter (if is-last?
                            identity
                            (take-while #(>= 0 (core/compare (affects-key %) my-last))))
        correct-ops (into [] (comp left-sib-filter right-side-filter) ops)

        ;;We include op if leq my left, and not if leq left's left
        ;;TODO we can't apply all ops, we should ensure to only apply ops whose keys are in the defined range, unless we're the last sibling
        ]
   ; (println "left-sibs-min-last" left-sibs-min-last)
   ; (println "is-last?" is-last?)
   ; (println "expanding data node" data-node "with ops" correct-ops)
    (reduce (fn [v op]
              (apply-op-to-vector op v))
            (:children data-node)
            correct-ops)))

#_(clojure.pprint/pprint
  (-> (core/insert (apply core/b-tree (range 6)) -1)
      (core/flush-tree) :tree
      (enqueue [(->InsertOp -1)])  
      (enqueue [(->InsertOp -2)])  
      (enqueue [(->InsertOp 50)])  
      (enqueue [(->InsertOp -4)])  
      (enqueue [(->InsertOp -5)]) ; tree is totally filled up here
  ;    (enqueue [(->InsertOp -8)])
      ))


(defn lookup
  [tree key]
  (let [path (pop (pop (core/lookup-path tree key)))
        expanded (apply-ops-in-path path)
        i (Collections/binarySearch expanded key core/compare)]
    (println expanded)
    (when-not (neg? i)
      (nth expanded i))))

(comment
  (def my-tree (-> (apply core/b-tree (range 30))
      (core/flush-tree) :tree
      (enqueue [(->InsertOp 8.3)])  
      (enqueue [(->InsertOp 6.7)])  
      (enqueue [(->InsertOp 10.5)])  
      (enqueue [(->InsertOp 10.6)])  
      ;(enqueue [(->InsertOp -5)]) ; tree is totally filled up here
  ;    (enqueue [(->InsertOp -8)])
      )) 
  (do (clojure.pprint/pprint my-tree)
  (lookup-fwd-iter my-tree -100))
  (lookup my-tree 50))

(defn forward-iterator
  "Takes the result of a search and returns an iterator going
   forward over the tree. Does lg(n) backtracking sometimes."
  [path]
  (assert (core/data-node? (peek path)))
  (let [first-elements (apply-ops-in-path path)
        next-elements (lazy-seq
                        (when-let [succ (core/right-successor (pop path))]
                          (forward-iterator succ)))]
    (concat first-elements next-elements)))

(defn lookup-fwd-iter
  [tree key]
  (let [path (pop (pop (core/lookup-path tree key)))]
    (when path
      (drop-while (fn [e]
                    (neg? (core/compare e key)))
                  (forward-iterator path)))))

;;TODO implement the op-buf handling in delete in the core.clj
;;op-buf should be sorted first by key, then by ops/order: use a sorted-map w/ vector vals; this would simplify in-buffer operator merging
;;
;;TODO needs testing--nothing is generatively shown to be working
;;
;;Should have both b & op-buf-size parameterizable--annoying refactor
