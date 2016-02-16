(ns tree.core
  (:refer-clojure :exclude [compare resolve subvec])
  (:require [clojure.core.rrb-vector :refer (catvec subvec)]
            [clojure.pprint :as pp])
  (:import java.io.Writer
           java.util.Collections))

(def b 3)

(defprotocol IKeyCompare
  (compare [key1 key2]))

(defprotocol IResolve
  "All nodes must implement this protocol. It's includes the minimal functionality
   necessary to avoid resolving nodes unless strictly necessary."
  (last-key [_] "Returns the rightmost key of the node")
  (dirty? [_] "Returns true if this should be flushed")
  (resolve [_] "Returns the INode version of this; could trigger IO"))

(defn tree-node?
  [node]
  (satisfies? IResolve node))

(defprotocol INode
  (overflow? [node] "Returns true if this node has too many elements")
  (underflow? [node] "Returns true if this node has too few elements")
  (merge-node [node other] "Combines this node with the other to form a bigger node. We assume they're siblings")
  (split-node [node] "Returns a Split object with the 2 nodes that we turned this into")
  (lookup [node key] "Returns the child node which contains the given key")) 

(defrecord Split [left right median])

;; TODO maybe this is a good protocol?
;; how to track dirty bits...
;; could model w/ monad (ground state dirty unless loaded from storage),
;;     gets cleaned after each flush-flood
;; flush-flood is the BFS for all dirty nodes; individual nodes must be flushable
;; A storage backend must be able to take a node as arg, serialize, assign addr,
;;    and call "clean" on the node to allow it to have a backing addr
;;
;; Maybe disk backing shouldn't be done as a monad, since that woud double
;; the total IOPS when 2 users of a snapshot need flush/clones
;; (N uses makes N useless copies).
;;
;; So perhaps each node can have a promise, it's address on storage.
;; When someone wants to flush it, they first dump it to disk, then try to clean it.
;; If the clean succeeds, they're done. If the clean fails (i.e. deliver fails),
;; they roll back the write and read the address from the promise.
;;
;; Monads will be reserved for things we want persisted, rather than the
;; in-memory flushing system, which can afford extra communication
;;
;; We can totally rely on a caching layer to manage keeping nodes around for
;; when a single tree is passed to several different consumers. This layer
;; will make it easier ta manage the overall JVM's memory allocation, and
;; it's far simpler than trying to use weak pointers to track unresolved addrs
;; so that we can supply the data to all of them when we fetch it. The cache
;; will also have better GC properties, by not accidentally sticking random
;; tree bits into jvm GC roots that could be held a long time.
;;
;; So flushing writes are shared, but loading from disk is cached instead.
;; Maybe write flushing could itself just be a smaller (or bigger) "write cache"...
;; no, that could be defeated by really big writes, which are already guaranteed
;; to be resident (since they were pending and thus linked to the tree's root).
;;
;; We'll make an IOPS measuring backend, which specifically makes "fake" addrs
;; that keep pointers to the nodes they "stored". Importantly, it needs to record
;; each read and write operation into counters, so that we can run a test
;; and check the total IOPS we spent.
;;
;; The advanced work with this will allow us to define hard IOPS bounds
;; (based on proven data), and then send generated loads to check we stay within
;; our targets.

;; TODO don't know what this protocol should be, or who should implement
;; serialization/flushing-related code
(defprotocol IBackend
  (store-node [this node] "Returns a generated addr where the given node was stored")
  (load-node [this addr] "Returns the node from the given addr"))

(extend-protocol IKeyCompare
  ;; By default, we use the default comparator
  Object
  (compare [key1 key2] (clojure.core/compare key1 key2)))

;; TODO enforce that there always (= (count children) (inc (count keys)))
;;
;; TODO we should be able to find all uncommited data by searching for
;; resolved & unresolved children

(declare data-node dirty!)

(defrecord IndexNode [keys children storage-addr op-buf]
  IResolve
  (dirty? [this] (not (realized? storage-addr)))
  (resolve [this] this) ;;TODO this is a hack for testing
  (last-key [this]
    ;;TODO should optimize by caching to reduce IOps (can use monad)
    (last-key (peek children)))
  INode
  (overflow? [this]
    (>= (count children) (* 2 b)))
  (underflow? [this]
    (< (count children) b))
  (split-node [this]
    (let [median (nth keys (dec b))
          [left-buf right-buf] (split-with #(not (pos? (compare (:key %) median)))
                                           ;;TODO this should use msg/affects-key
                                           (sort-by :key op-buf))]
      (->Split (->IndexNode (subvec keys 0 (dec b))
                            (subvec children 0 b)
                            (promise)
                            (vec left-buf))
               (->IndexNode (subvec keys b)
                            (subvec children b)
                            (promise)
                            (vec right-buf))
               median)))
  (merge-node [this other]
    (->IndexNode (catvec (conj keys (last-key (peek children))) (:keys other))
                 (catvec children (:children other))
                 (promise)
                 (catvec op-buf (:op-buf other))))
  (lookup [root key]
    (let [x (Collections/binarySearch keys key compare)]
      (if (neg? x)
        (- (inc x))
        x))))

(defn index-node?
  [node]
  (instance? IndexNode node))

(defn print-index-node
  "Optionally include"
  [node ^Writer writer fully-qualified?]
  (.write writer (if fully-qualified?
                   (pr-str IndexNode)
                   "IndexNode"))
  (.write writer (str {:keys (:keys node)
                       :children (:children node)})))

(defmethod print-method IndexNode
  [node writer]
  (print-index-node node writer false))

(defmethod print-dup IndexNode
  [node writer]
  (print-index-node node writer true))

(defn node-status-bits
  [node]
  (str "["
       (if (dirty? node) "D" " ")
       "]"))

(defmethod pp/simple-dispatch IndexNode
  [node]
  (let [out ^Writer *out*]
    (.write out "IndexNode")
    (.write out (node-status-bits node))
    (pp/pprint-logical-block
      :prefix "{" :suffix "}"
      (pp/pprint-logical-block
        (.write out ":keys ")
        (pp/write-out (:keys node))
        (pp/pprint-newline :linear))
      (pp/pprint-logical-block
        (.write out ":op-buf ")
        (pp/write-out (:op-buf node))
        (pp/pprint-newline :linear))
      (pp/pprint-logical-block
        (.write out ":children ")
        (pp/pprint-newline :mandatory)
        (pp/write-out (:children node))))))

(defrecord DataNode [children storage-addr]
  IResolve
  (resolve [this] this) ;;TODO this is a hack for testing
  (dirty? [this] (not (realized? storage-addr)))
  (last-key [this]
    (peek children))
  INode
  ;; Should have between b & 2b-1 children
  (overflow? [this]
    (>= (count children) (* 2 b)))
  (underflow? [this]
    (< (count children) b))
  (split-node [this]
    (->Split (data-node (subvec children 0 b))
             (data-node (subvec children b))
             (nth children (dec b))))
  (merge-node [this other]
    (data-node (catvec children (:children other))))
  (lookup [root key]
    (let [x (Collections/binarySearch children key compare)]
      (if (neg? x)
        (- (inc x))
        x))))

(defn data-node
  "Creates a new data node"
  [children]
  (->DataNode children (promise)))

(defn data-node?
  [node]
  (instance? DataNode node))

;(println (b-tree :foo :bar :baz))
;(pp/pprint (apply b-tree (range 100)))
(defn print-data-node
  [node ^Writer writer fully-qualified?]
  (.write writer (if fully-qualified?
                   (pr-str DataNode)
                   "DataNode"))
  (.write writer (str {:children (:children node)})))

(defmethod print-method DataNode
  [node writer]
  (print-data-node node writer false))

(defmethod print-dup DataNode
  [node writer]
  (print-data-node node writer true))

(defmethod pp/simple-dispatch DataNode
  [node]
  (let [out ^Writer *out*]
    (.write out (str "DataNode"
                     (node-status-bits node)))
    (.write out (str {:children (:children node)}))))

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
                                (take-while #(or (index-node? %)
                                                 (data-node? %)))
                                (iterate #(let [c (-> % :children first)]
                                            (if (tree-node? c)
                                              (resolve c)
                                              c))
                                         new-sibling))
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
    (assert (data-node? start-node))
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
            child (if (data-node? cur)
                    child
                    (resolve child))
            path' (conj path index child)]
        (if (data-node? cur) ;are we done?
          path'
          (recur path' child)))
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
  (let [index (Collections/binarySearch v key compare)]
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
  (let [index (Collections/binarySearch v key compare)]
    (cond
      (neg? index) v ; not found, do nothing
      (zero? index) (subvec v 1) ; if first, no need to concat
      (= (dec (count v)) index) (pop v) ; if last, just pop
      :else (catvec (subvec v 0 index) (subvec v (inc index))))))

(defn insert
  [tree key]
  (let [path (pop (pop (lookup-path tree key))) ; don't care about the found key or its index
        {:keys [children] :or {children []}} (peek path)
        updated-data-node (data-node (-insertion-into-sorted-vector children key))]
    (loop [node updated-data-node
           path (pop path)]
      (if (empty? path)
        (if (overflow? node)
          (let [{:keys [left right median]} (split-node node)]
            (->IndexNode [median] [left right] (promise) []))
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
              (recur (-> parent
                         (assoc :keys new-keys
                                :children new-children)
                         (dirty!))
                     (pop (pop path))))
            (recur (-> parent
                       (assoc :children (assoc children index node))
                       (dirty!))
                   (pop (pop path)))))))))

;;TODO: cool optimization: when merging children, push as many operations as you can
;;into them to opportunisitcally minimize overall IO costs

(defn delete
  [tree key]
  (let [path (pop (pop (lookup-path tree key))) ; don't care about the found key or its index
        {:keys [children] :or {children []}} (peek path)
        updated-data-node (data-node (-deletion-from-sorted-vector children key))]
    (loop [node updated-data-node
           path (pop path)]
      (if (empty? path)
        ;; Check for special root underflow case
        (if (and (index-node? node) (= 1 (count (:children node))))
          (first (:children node))
          node)
        (let [index (peek path)
              {:keys [children keys op-buf] :as parent} (peek (pop path))]
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
                                              old-right-children)
                                      (promise)
                                      op-buf)
                         (pop (pop path))))
                (recur (->IndexNode (catvec old-left-keys old-right-keys)
                                    (catvec (conj old-left-children merged)
                                            old-right-children)
                                    (promise)
                                    op-buf)
                       (pop (pop path)))))
            (recur (->IndexNode keys
                                (assoc children index node)
                                (promise)
                                op-buf)
                   (pop (pop path)))))))))

(defn b-tree
  [& keys]
  (reduce insert (data-node []) keys))

(defrecord TestingAddr [last-key node]
  IResolve
  (dirty? [this] false)
  (last-key [_] last-key)
  (resolve [_] node))

(defn print-testing-addr
  [node ^Writer writer fully-qualified?]
  (.write writer (if fully-qualified?
                   (pr-str TestingAddr)
                   "TestingAddr"))
  (.write writer (str {})))

(defmethod print-method TestingAddr
  [node writer]
  (print-testing-addr node writer false))

(defmethod print-dup TestingAddr
  [node writer]
  (print-testing-addr node writer true))

(defmethod pp/simple-dispatch TestingAddr
  [node]
  (let [out ^Writer *out*]
    (.write out (str "TestingAddr"
                     (node-status-bits node)))
    (.write out (str {}))))

(defn dirty!
  "Marks a node as being dirty if it was clean"
  [node]
  (assert (not (instance? TestingAddr node)))
  (assoc node :storage-addr (promise)))

;;TODO make this a loop/recur instead of mutual recursion
(declare flush-tree)

(defn flush-children
  [stats children]
  (reduce (fn [{:keys [children stats]} child]
            (let [{:keys [tree] sub-stats :stats} (flush-tree child stats)]
              {:children (conj children tree)
               :stats sub-stats}))
          {:children [] :stats stats}
          children))

(defn flush-tree
  "Given the tree, finds all dirty nodes, delivering addrs into them.
   Every dirty node also gets replaced with its TestingAddr.
   These form a GC cycle, have fun with the unmanaged memory port :)"
  ([tree]
   (-> (flush-tree tree {:writes 0})
       (update-in [:tree] resolve))) ; root should never be flushed
  ([tree stats]
   (if (dirty? tree)
     (let [{cleaned-children :children
            stats            :stats} (if (data-node? tree)
                                       {:children (:children tree)
                                        :stats stats}
                                       (flush-children stats (:children tree)))
           cleaned-node (assoc tree :children cleaned-children)
           new-addr (->TestingAddr (last-key tree) cleaned-node)]
       (deliver (:storage-addr tree) new-addr)
       (when (not= new-addr @(:storage-addr tree))
         ;;here's where we'd rollback a write
         )
       {:tree new-addr :stats (update-in stats [:writes] inc)})
     {:tree tree :stats stats})))

;; The parts of the serialization system that seem like they're need hooks are:
;; - Must provide a function that takes a node, serializes it, and returns an addr
;; - Must be able to rollback writing an addr
;; - Whatever the addr it returns, it should cache its resolve in-mem somehow
;; - The serialize a node & rollback a node functions should accept a "stats" object as well
;; - The "stats" object must be convertible to a summary or whatever at the end

;; could make a redis backend using refcounting for nodes :)

(comment
  (clojure.pprint/pprint (apply b-tree (range 30)))

  (let [t (:tree (flush-tree (apply b-tree (range 30))))
        t' (reduce insert t [-7 100])]
    (assert (= 0 (:writes (:stats (flush-tree t)))))
    (assert (= (range 30) (lookup-fwd-iter t -1)))
    (pp/pprint t)
    (pp/pprint (insert t -7))
    (pp/pprint t')
    (pp/pprint (flush-tree t'))
    (pp/pprint t')
    )


  (:stats (flush-tree (apply b-tree (range 30))))
  )
