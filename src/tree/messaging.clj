(ns tree.messaging
  (:refer-clojure :exclude [subvec])
  (:require [tree.core :as core]
            [clojure.pprint :as pp] 
            [clojure.core.rrb-vector :refer (catvec subvec)])
  (:import java.io.Writer
           java.util.Collections)) 

;; An operation is an object with a few functions
;; 1. It has a function that it applies to the tree to apply its effect
;; In the future, it could also have
;; 2. It has a promise which can be filled with the end result
;;       (more memory but faster results for repeat queries)

(defprotocol IOperation
  (affects-key [op] "Which key this affects--currently must be a single key")
  (apply-op-to-coll [op coll] "Applies the operation to the collection")
  (apply-op-to-tree [op tree] "Applies the operation to the tree"))

(defrecord InsertOp [key]
  IOperation
  (affects-key [_] key)
  (apply-op-to-coll [_ set] (conj set key))
  (apply-op-to-tree [_ tree] (core/insert tree key)))

(defrecord DeleteOp [key]
  IOperation
  (affects-key [_] key)
  (apply-op-to-coll [_ set] (disj set key))
  (apply-op-to-tree [_ tree] (core/delete tree key)))

(defmethod print-method InsertOp
  [op ^Writer writer]
  (.write writer "InsertOp")
  (.write writer (str {:key (:key op) " - " (:tag op)})))

(defmethod print-dup InsertOp
  [op ^Writer writer]
  (.write writer "(tree.messaging/->InsertOp ")
  (.write writer (pr-str (:key op)))
  (.write writer ")"))

(defmethod pp/simple-dispatch InsertOp
  [op]
  (print op))

(defmethod print-method DeleteOp
  [op ^Writer writer]
  (.write writer "DeleteOp")
  (.write writer (str {:key (:key op)} " - " (:tag op))))

(defmethod print-dup DeleteOp
  [op ^Writer writer]
  (.write writer "(tree.messaging/->DeleteOp ")
  (.write writer (pr-str (:key op)))
  (.write writer ")"))

(defmethod pp/simple-dispatch DeleteOp
  [op]
  (print op))

(defn enqueue
  "When enqueing "
  ([tree msgs]
   (let [deferred-ops (atom [])
         msg-buffers-propagated (enqueue tree msgs deferred-ops)]
     ;(when (seq @deferred-ops) (println "appyling deferred ops" @deferred-ops))
     (reduce (fn [tree op]
               (apply-op-to-tree op tree))
             msg-buffers-propagated
             @deferred-ops)))
  ([tree msgs deferred-ops]
   ;(println "tree is" (class tree) tree)
   (let [tree (core/resolve tree)]
     (cond
       (core/data-node? tree) ; need to return ops to apply to the tree proper...
       (do (swap! deferred-ops into msgs)
           tree)
       (<= (+ (count msgs) (count (:op-buf tree)))
           (get-in tree [:cfg :op-buf-size])) ; will there be enough space?
       (-> tree
           (core/dirty!)
           (update-in [:op-buf] into msgs))
       :else ; overflow, should be IndexNode
       (do (assert (core/index-node? tree))
           ;(println "overflowing node" (:keys tree) "with buf" (:op-buf tree)
           ;         "with new msgs" msgs
           ;         )
           (loop [[child & children] (:children tree)
                  rebuilt-children []
                  msgs (vec (sort-by affects-key ;must be a stable sort 
                                     (concat (:op-buf tree) msgs)))]
             (let [took-msgs (into []
                                   (take-while #(>= 0 (core/compare
                                                        (affects-key %)
                                                        (core/last-key child))))
                                   msgs)
                   extra-msgs (into []
                                   (drop-while #(>= 0 (core/compare
                                                        (affects-key %)
                                                        (core/last-key child))))
                                   msgs)
                   ;_ (println "last-key:" (core/last-key child))
                   ;_ (println "goes left:" took-msgs)
                   ;_ (println "goes right:" extra-msgs)
                   on-the-last-child? (empty? children)

                   ;; Any changes to the current child?
                   new-child
                   (cond
                     (and on-the-last-child? (seq extra-msgs))
                     (enqueue (core/resolve child)
                              (catvec took-msgs extra-msgs)
                              deferred-ops)
                     (seq took-msgs) ; save a write
                     (enqueue (core/resolve child)
                              took-msgs
                              deferred-ops)
                     :else
                     child)]

               (if on-the-last-child?
                 (-> tree
                     (assoc :children (conj rebuilt-children new-child))
                     (assoc :op-buf [])
                     (core/dirty!))
                 (recur children (conj rebuilt-children new-child) extra-msgs)))))))))

(defn insert
  [tree key]
  (enqueue tree [(assoc (->InsertOp key)
                        :tag (java.util.UUID/randomUUID)
                        )]))

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


(defn apply-ops-in-path
  [path]
  (if (= 1 (count path))
    (:children (peek path))
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
      ;(println "left-sibs-min-last" left-sibs-min-last)
      ;(println "is-last?" is-last?)
      ;(println "expanding data node" data-node "with ops" correct-ops)
      (reduce (fn [coll op]
                (apply-op-to-coll op coll))
              (:children data-node)
              correct-ops))))

(defn lookup
  [tree key]
  (let [path (core/lookup-path tree key)
        expanded (apply-ops-in-path path)
        i (Collections/binarySearch expanded key core/compare)]
    (println expanded)
    (when-not (neg? i)
      (nth expanded i))))

(defn insert
  [tree key]
  (enqueue tree [(->InsertOp key)]))

(defn delete
  [tree key]
  (enqueue tree [(assoc (->DeleteOp key)
                        :tag (java.util.UUID/randomUUID)
                        )]))

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
  (let [path (core/lookup-path tree key)]
    (when path
      (drop-while (fn [e]
                    (neg? (core/compare e key)))
                  (forward-iterator path)))))
