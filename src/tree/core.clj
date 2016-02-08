(ns tree.core
  (:refer-clojure :exclude [compare resolve subvec])
  (:require [clojure.core.rrb-vector :refer (catvec subvec)]))

(def b 3)

(defprotocol IKeyCompare
  (compare [key1 key2]))

(defprotocol INode
  (overflow? [node] "Returns true if this node has too many elements")
  (underflow? [node] "Returns true if this node has too few elements")
  (merge-node [node other] "Combines this node with the other to form a bigger node. We assume they're siblings")
  (last-key [node] "Returns the rightmost key of the node")
  (split-node [node] "Returns a Split object with the 2 nodes that we turned this into")
  (lookup [node key] "Returns the child node which contains the given key")) 

(defrecord Split [left right median])

(defprotocol IResolve
  "This is how we store the children. The indirection enables background
   fetch and decode of the resource."
  (resolve [this]))

(extend-protocol IKeyCompare
  ;; By default, we use the default comparator
  Object
  (compare [key1 key2] (clojure.core/compare key1 key2)))

;; TODO enforce that there always (= (count children) (inc (count keys)))
;;
;; TODO we should be able to find all uncommited data by searching for
;; resolved & unresolved children

(defrecord IndexNode [keys children]
  IResolve
  (resolve [this] this) ;;TODO this is a hack for testing
  INode
  (overflow? [this]
    (>= (count children) (* 2 b)))
  (underflow? [this]
    (< (count children) b))
  (split-node [this]
    (->Split (->IndexNode (subvec keys 0 (dec b))
                          (subvec children 0 b))
             (->IndexNode (subvec keys b)
                          (subvec children b))
             (nth keys (dec b))))
  (merge-node [this other]
    (->IndexNode (catvec (conj keys (last-key (peek children))) (:keys other))
                 (catvec children (:children other))))
  (last-key [this]
    ;;TODO should optimize by caching to reduce IOps (can use monad)
    (last-key (peek children)))
  (lookup [root key]
    (let [x (java.util.Collections/binarySearch keys key compare)]
      (if (neg? x)
        (- (inc x))
        x))))

(defrecord DataNode [children]
  IResolve
  (resolve [this] this) ;;TODO this is a hack for testing
  INode
  ;; Should have between b & 2b-1 children
  (overflow? [this]
    (>= (count children) (* 2 b)))
  (underflow? [this]
    (< (count children) b))
  (last-key [this]
    (peek children))
  (split-node [this]
    (->Split (->DataNode (subvec children 0 b))
             (->DataNode (subvec children b))
             (nth children (dec b))))
  (merge-node [this other]
    (->DataNode (catvec children (:children other))))
  (lookup [root key]
    (let [x (java.util.Collections/binarySearch children key  compare)]
      (if (neg? x)
        (- (inc x))
        x))))

(defn backtrack-up-path-until
  "Given a path (starting with root and ending with an index), searches backwards,
   passing each pair of parent & index we just came from to the predicate function.
   When that function returns true, we return the path ending in the index for which
   it was true, or else we return the empty path"
  [path pred]
  (loop [path path]
    (when (seq path)
      (let [from-index (peek path)
            tmp (pop path)
            parent (peek tmp)]
        (if (pred parent from-index)
          path
          (recur (pop tmp)))))))

(defn right-successor
  "Given a node on a path, find's that node's right successor node"
  [path]
  ;(clojure.pprint/pprint path)
  ;TODO this function would benefit from a prefetching hint
  ;     to keep the next several sibs in mem
  (when-let [common-parent-path
             (backtrack-up-path-until
               path
               (fn [parent index]
                 (< (inc index) (count (:children parent)))))]
    (let [next-index (-> common-parent-path peek inc)
          parent (-> common-parent-path pop peek)
          new-sibling (resolve (nth (:children parent) next-index))
          ;; We must get back down to the data node
          sibling-lineage (into []
                                (comp (take-while #(or (instance? IndexNode %)
                                                       (instance? DataNode %)))
                                      (map resolve))
                                (iterate #(-> % :children first) new-sibling))
          path-suffix (-> (interleave sibling-lineage
                                      (repeat 0))
                          (butlast)) ; butlast ensures we end w/ node
          ]
      (-> (pop common-parent-path)
          (conj next-index)
          (into path-suffix)))))

(defn forward-iterator
  "Takes the result of a search and returns an iterator going
   forward over the tree. Does lg(n) backtracking sometimes."
  [path start-index]
  (let [start-node (peek path)]
    (assert (instance? DataNode start-node))
    (let [first-elements (-> start-node
                             :children ; Get the indices of it
                             (subvec start-index)) ; skip to the start-index
          next-elements (lazy-seq
                          (when-let [succ (right-successor (pop path))]
                            (forward-iterator succ 0)))]
      (concat first-elements next-elements))))

(defn lookup-path
  "Given a B-tree and a key, gets a path into the tree"
  [tree key]
  (loop [path [tree] ;alternating node/index/node/index/node... of the search taken
         cur tree ;current search node
         ]
    (if (seq (:children cur))
      (let [index (lookup cur key)
            child (nth (:children cur) index (peek (:children cur))) ;;TODO what are the semantics for exceeding on the right? currently it's trunc to the last element
            path' (conj path index child)]
        (if (instance? DataNode cur) ;are we done?
          path'
          (recur path' (resolve child))))
      nil)))

(defn lookup-key
  "Given a B-tree and a key, gets an iterator into the tree"
  [tree key]
  (peek (lookup-path tree key)))

(defn lookup-fwd-iter
  [tree key]
  (let [path (lookup-path tree key)
        path (pop path)
        index (peek path)
        path (pop path)]
    (when path
      (forward-iterator path index))))

(defn -insertion-into-sorted-vector
  "Inserts the given key into the sorted vector.
   
   This is like a single insert from insertion sort,
   except that we have rrb-trees, so it's O(lg(n))
   instead :)"
  [v key]
  (let [index (java.util.Collections/binarySearch v key compare)]
    (if (neg? index)
      (let [index (- (inc index))]
        (if (= (count v) index)
          (conj v key)
          (let [left (subvec v 0 index)
                right (subvec v index)]
            (catvec (conj left key) right))))
      ;;This assoc-in should be a no-op, but models how to do the KV update
      ;;if we inserted a new key unconditionally here, that might enable multiple values per key
      (assoc v index key))))

(defn -deletion-from-sorted-vector
  [v key]
  (let [index (java.util.Collections/binarySearch v key compare)]
    (cond
      (neg? index) v ; not found, do nothing
      (zero? index) (subvec v 1) ; if first, no need to concat
      (= (dec (count v)) index) (pop v) ; if last, just pop
      :else (catvec (subvec v 0 index) (subvec v (inc index))))))

(defn insert
  [tree key]
  (let [path (pop (pop (lookup-path tree key))) ; don't care about the found key or its index
        {:keys [children] :or {children []}} (peek path)
        updated-data-node (->DataNode (-insertion-into-sorted-vector children key))]
    (loop [node updated-data-node
           path (pop path)]
      (if (empty? path)
        (if (overflow? node)
          (let [{:keys [left right median]} (split-node node)]
            (->IndexNode [median] [left right]))
          node)
        (let [index (peek path)
              {:keys [children keys] :as parent} (peek (pop path))]
          (if (overflow? node) ; splice the split into the parent
            ;;TODO refactor paths to be node/index pairs or 2 vectors or something
            (let [{:keys [left right median]} (split-node node)
                  new-children (catvec (conj (subvec children 0 index)
                                             left right)
                                       (subvec children (inc index)))
                  new-keys (catvec (conj (subvec keys 0 index)
                                         median)
                                   (subvec keys index))]
              (recur (->IndexNode new-keys new-children) (pop (pop path))))
            (recur (->IndexNode keys (assoc children index node))
                   (pop (pop path)))))))))

(defn delete
  [tree key]
  (let [path (pop (pop (lookup-path tree key))) ; don't care about the found key or its index
        {:keys [children] :or {children []}} (peek path)
        updated-data-node (->DataNode (-deletion-from-sorted-vector children key))]
    (loop [node updated-data-node
           path (pop path)]
      (if (empty? path)
        ;; Check for special root underflow case
        (if (and (instance? IndexNode node) (= 1 (count (:children node))))
          (first (:children node))
          node)
        (let [index (peek path)
              {:keys [children keys] :as parent} (peek (pop path))]
          (if (underflow? node) ; splice the split into the parent
            ;;TODO this needs to use a polymorphic sibling-count to work on serialized nodes
            (let [bigger-sibling-idx
                  (cond
                    (= (dec (count children)) index) (dec index) ; only have left sib
                    (zero? index) 1 ;only have right sib
                    (> (count (:children (nth children (dec index))))
                       (count (:children (nth children (inc index)))))
                    (dec index) ; right sib bigger
                    :else (inc index))
                  node-first? (> bigger-sibling-idx index) ; if true, `node` is left
                  merged (if node-first?
                           (merge-node node (nth children bigger-sibling-idx))
                           (merge-node (nth children bigger-sibling-idx) node))
                  old-left-children (subvec children 0 (min index bigger-sibling-idx))
                  old-right-children (subvec children (inc (max index bigger-sibling-idx)))
                  old-left-keys (subvec keys 0 (min index bigger-sibling-idx))
                  old-right-keys (subvec keys (max index bigger-sibling-idx))]
              (if (overflow? merged)
                (let [{:keys [left right median]} (split-node merged)]
                  (recur (->IndexNode (catvec (conj old-left-keys median)
                                              old-right-keys)
                                      (catvec (conj old-left-children left right)
                                              old-right-children))
                         (pop (pop path))))
                (recur (->IndexNode (catvec old-left-keys old-right-keys)
                                    (catvec (conj old-left-children merged)
                                            old-right-children))
                       (pop (pop path)))))
            (recur (->IndexNode keys (assoc children index node))
                   (pop (pop path)))))))))

(defn empty-b-tree
  []
  (->DataNode []))
