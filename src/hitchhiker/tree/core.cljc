(ns hitchhiker.tree.core
  (:refer-clojure :exclude [compare resolve subvec])
  (:require [clojure.core.rrb-vector :refer [catvec subvec]]
            #?(:clj [clojure.pprint :as pp])
            #?(:clj [clojure.core.async :refer [promise-chan poll! put!]])
            #?(:clj [clojure.core.async :refer [go chan put! <! <!! promise-chan]
                     :as async]
               :cljs [cljs.core.async :refer [chan put! <! promise-chan]
                      :as async])
            #?(:cljs [goog.array])
            #?(:clj [taoensso.nippy :as nippy]))
  #?(:clj (:import java.io.Writer
                   [java.util Arrays Collections]))
  #?(:cljs (:require-macros [cljs.core.async.macros :refer [go]]
                            [hitchhiker.tree.core :refer [go-try <? <?resolve]]))
  #?(:clj (:import [clojure.lang.PersistentTreeMap$BlackVal])))


(def ^:dynamic *async-backend* :none)


;; cljs macro environment

(defn- cljs-env?
  "Take the &env from a macro, and tell whether we are expanding into cljs."
  [env]
  (boolean (:ns env)))

#?(:clj
   (defmacro if-cljs
     "Return then if we are generating cljs code and else for Clojure code.
     https://groups.google.com/d/msg/clojurescript/iBY5HaQda4A/w1lAQi9_AwsJ"
     [then else]
     (if (cljs-env? &env) then else)))


;; core.async helpers

(defn throw-if-exception
  "Helper method that checks if x is Exception and if yes, wraps it in a new
  exception, passing though ex-data if any, and throws it. The wrapping is done
  to maintain a full stack trace when jumping between multiple contexts."
  [x]
  (if (instance? #?(:clj Exception :cljs js/Error) x)
    (throw (ex-info (or #?(:clj (.getMessage ^Exception x)) (str x))
                    (or (ex-data x) {})
                    x))
    x))

#?(:clj
   (defmacro go-try
     "Asynchronously executes the body in a go block. Returns a channel
  which will receive the result of the body when completed or the
  exception if an exception is thrown. You are responsible to take
  this exception and deal with it! This means you need to take the
  result from the channel at some point."
     {:style/indent 1}
     [ & body]
     (case *async-backend*
       :none
       `(if-cljs (throw (ex-info "You need an async backend for cljs." {}))
                 (do ~@body))
       :core.async
       `(if-cljs (cljs.core.async.macros/go
                   (try ~@body
                        (catch js/Error e#
                          e#)))
               (go
                 (try
                   ~@body
                   (catch Exception e#
                     e#)))))))

#?(:clj
   (defmacro <?
     "Same as core.async <! but throws an exception if the channel returns a
throwable error."
     [ch]
     (case *async-backend*
       :none
       `(if-cljs (throw (ex-info "You need an async backend for cljs." {}))
                 ~ch)
       :core.async
       `(if-cljs (throw-if-exception (cljs.core.async/<! ~ch))
                 (throw-if-exception (<! ~ch)))
       )))


#?(:clj
   (defn <??
     "Same as core.async <!! but throws an exception if the channel returns a
throwable error."
     [ch]
     (case *async-backend*
       :none ch

       :core.async
       (throw-if-exception (<!! ch)))))

(defn reduce<
  "Reduces over a sequence s with a go function go-f given the initial value
  init."
  [go-f init s]
  (go-try
   (loop [res init
          [f & r] s]
     (if f
       (recur (<? (go-f res f)) r)
       res))))


;; core code

(defrecord Config [index-b data-b op-buf-size])

(defprotocol IKeyCompare
  (compare [key1 key2]))

(defprotocol IResolve
  "All nodes must implement this protocol. It's includes the minimal functionality
   necessary to avoid resolving nodes unless strictly necessary."
  (index? [_] "Returns true if this is an index node")
  (last-key [_] "Returns the rightmost key of the node")
  (dirty? [_] "Returns true if this should be flushed")
  ;;TODO resolve should be instrumented
  (resolve [_] "Returns the INode version of this node in a go-block; could trigger IO"))



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

;; TODO add full edn support including records
(defn order-on-edn-types [t]
  (cond (map? t) 0
        (vector? t) 1
        (set? t) 2
        (number? t) 3
        (string? t) 4
        (symbol? t) 5
        (keyword? t) 6))

(extend-protocol IKeyCompare
  ;; By default, we use the default comparator
  #?@(:clj
      [Object
       (compare [key1 key2]
                (if (or (= (class key1) (class key2))
                        (= (order-on-edn-types key1)
                           (order-on-edn-types key2)))
                  (try
                    (clojure.core/compare key1 key2)
                    (catch ClassCastException e
                      (- (order-on-edn-types key2)
                         (order-on-edn-types key1))))))
       Double
       (compare [^Double key1 key2]
                (if (instance? Double key2)
                  (.compareTo key1 key2)
                  (try
                    (clojure.core/compare key1 key2)
                    (catch ClassCastException e
                      (- (order-on-edn-types key2)
                         (order-on-edn-types key1))))))
       Long
       (compare [^Long key1 key2]
                (if (instance? Long key2)
                  (.compareTo key1 key2)
                  (try
                    (clojure.core/compare key1 key2)
                    (catch ClassCastException e
                      (- (order-on-edn-types key2)
                         (order-on-edn-types key1))))))]
     :cljs
      [number
       (compare [key1 key2] (cljs.core/compare key1 key2))
       object
       (compare [key1 key2] (cljs.core/compare key1 key2))]))

;; TODO enforce that there always (= (count children) (inc (count keys)))
;;
;; TODO we should be able to find all uncommited data by searching for
;; resolved & unresolved children

(declare data-node dirty!)

(defn index-node-keys
  "Calculates the separating keys given the children of an index node"
  [children]
  (mapv last-key (butlast children)))

(declare ->IndexNode)

(defrecord IndexNode [children storage-addr op-buf cfg]
  IResolve
  (index? [this] true)
  (dirty? [this] (not (poll! storage-addr)))
  (resolve [this] this #_(go this))
  (last-key [this]
    ;;TODO should optimize by caching to reduce IOps (can use monad)
    (last-key (peek children)))
  INode
  (overflow? [this]
    (>= (count children) (* 2 (:index-b cfg))))
  (underflow? [this]
    (< (count children) (:index-b cfg)))
  (split-node [this]
    (let [b (:index-b cfg)
          median (nth (index-node-keys children) (dec b))
          [left-buf right-buf] (split-with #(not (pos? (compare (:key %) median)))
                                           ;;TODO this should use msg/affects-key
                                           (sort-by :key compare op-buf))]
      (->Split (->IndexNode (subvec children 0 b)
                            (promise-chan)
                            (vec left-buf)
                            cfg)
               (->IndexNode (subvec children b)
                            (promise-chan)
                            (vec right-buf)
                            cfg)
              median)))
  (merge-node [this other]
    (->IndexNode (catvec children (:children other))
                 (promise-chan)
                 (catvec op-buf (:op-buf other))
                 cfg))
  (lookup [root key]
    ;;This is written like so because it's performance critical
    (let [l (dec (count children))
          a (object-array l)
          _ (dotimes [i l]
              (aset a i (last-key (nth children i))))
          x #?(:clj (Arrays/binarySearch a 0 l key compare)
               :cljs (goog.array/binarySearch a key compare))]
      (if (neg? x)
        (- (inc x))
        x))))

#?(:clj
   (nippy/extend-freeze IndexNode :b-tree/index-node
                        [{:keys [storage-addr cfg children op-buf]} data-output]
                        (nippy/freeze-to-out! data-output cfg)
                        (nippy/freeze-to-out! data-output children)
                        ;;TODO apparently RRB-vectors don't freeze correctly;
                        ;;we'll force it to a normal vector as a workaround
                        (nippy/freeze-to-out! data-output (into [] op-buf))))

#?(:clj
   (nippy/extend-thaw :b-tree/index-node
                      [data-input]
                      (let [cfg (nippy/thaw-from-in! data-input)
                            children (nippy/thaw-from-in! data-input)
                            op-buf (nippy/thaw-from-in! data-input)]
                        (->IndexNode children nil op-buf cfg))))

(defn index-node?
  [node]
  (instance? IndexNode node))

#?(:clj
   (defn print-index-node
     "Optionally include"
     [node ^Writer writer fully-qualified?]
     (.write writer (if fully-qualified?
                      (pr-str IndexNode)
                      "IndexNode"))
     (.write writer (str {:keys (index-node-keys (:children node))
                          :children (:children node)}))))

#?(:clj
   (defmethod print-method IndexNode
     [node writer]
     (print-index-node node writer false)))

#?(:clj
   (defmethod print-dup IndexNode
     [node writer]
     (print-index-node node writer true)))

#?(:clj
   (defn node-status-bits
     [node]
     (str "["
          (if (dirty? node) "D" " ")
          "]")))

#?(:clj
   (defmethod pp/simple-dispatch IndexNode
     [node]
     (let [out ^Writer *out*]
       (.write out "IndexNode")
       (.write out ^String (node-status-bits node))
       (pp/pprint-logical-block
        :prefix "{" :suffix "}"
        (pp/pprint-logical-block
         (.write out ":keys ")
         (pp/write-out (index-node-keys (:children node)))
         (pp/pprint-newline :linear))
        (pp/pprint-logical-block
         (.write out ":op-buf ")
         (pp/write-out (:op-buf node))
         (pp/pprint-newline :linear))
        (pp/pprint-logical-block
         (.write out ":children ")
         (pp/pprint-newline :mandatory)
         (pp/write-out (:children node)))))))

(defn nth-of-set
  "Like nth, but for sorted sets. O(n)"
  [set index]
  (first (drop index set)))

(defrecord DataNode [children storage-addr cfg]
  IResolve
  (index? [this] false)
  (resolve [this] this #_(go this)) 
  (dirty? [this] (not (poll! storage-addr)))
  (last-key [this]
    (when (seq children)
      (-> children
          (rseq)
          (first)
          (key))))
  INode
  ;; Should have between b & 2b-1 children
  (overflow? [this]
    (>= (count children) (* 2 (:data-b cfg))))
  (underflow? [this]
    (< (count children) (:data-b cfg)))
  (split-node [this]
    (->Split (data-node cfg (into (sorted-map-by compare) (take (:data-b cfg)) children))
             (data-node cfg (into (sorted-map-by compare) (drop (:data-b cfg)) children))
             (nth-of-set children (dec (:data-b cfg)))))
  (merge-node [this other]
    (data-node cfg (into children (:children other))))
  (lookup [root key]
    (let [x #?(:clj (Collections/binarySearch (vec (keys children)) key compare)
               :cljs (goog.array/binarySearch (into-array (keys children)) key compare))]
      (if (neg? x)
        (- (inc x))
        x))))

(defn data-node
  "Creates a new data node"
  [cfg children]
  (->DataNode children (promise-chan) cfg))

(defn data-node?
  [node]
  (instance? DataNode node))

#?(:clj
   (defmacro <?resolve
     "HACK Attempt faster inlined resolve to avoid unnecessary channel ops."
     [e]
     `(if (or (data-node? ~e)
              (index-node? ~e))
        ~e
        (<? (resolve ~e)))))

#?(:clj
   (nippy/extend-freeze DataNode :b-tree/data-node
                        [{:keys [cfg children]} data-output]
                        (nippy/freeze-to-out! data-output cfg)
                        (nippy/freeze-to-out! data-output children)))

#?(:clj
   (nippy/extend-thaw :b-tree/data-node
                      [data-input]
                      (let [cfg (nippy/thaw-from-in! data-input)
                            children (nippy/thaw-from-in! data-input)]
                        (->DataNode children nil cfg))))

;(println (b-tree :foo :bar :baz))
;(pp/pprint (apply b-tree (range 100)))
#?(:clj
   (defn print-data-node
     [node ^Writer writer fully-qualified?]
     (.write writer (if fully-qualified?
                      (pr-str DataNode)
                      "DataNode"))
     (.write writer (str {:children (:children node)}))))

#?(:clj
   (defmethod print-method DataNode
     [node writer]
     (print-data-node node writer false)))

#?(:clj
   (defmethod print-dup DataNode
     [node writer]
     (print-data-node node writer true)))

#?(:clj
   (defmethod pp/simple-dispatch DataNode
     [node]
     (let [out ^Writer *out*]
       (.write out (str "DataNode"
                        (node-status-bits node)))
       (.write out (str {:children (:children node)})))))

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
  (go-try
    (when-let [common-parent-path
               (backtrack-up-path-until
                path
                (fn [parent index]
                  (< (inc index) (count (:children parent)))))]
      (let [next-index (-> common-parent-path peek inc)
            parent (-> common-parent-path pop peek)
            new-sibling (<?resolve (nth (:children parent) next-index))
            ;; We must get back down to the data node
            ;; iterate cannot do blocking operations with core.async, so we use a loop
            sibling-lineage (loop [res [new-sibling]
                                    s new-sibling]
                              (let [c (-> s :children first)
                                    ;_ (prn (type c) (= (class c) clojure.lang.PersistentTreeMap$BlackVal))
                                    c (cond
                                        ;; TODO cleanup path
                                        ;; fast path
                                        (or (index-node? c)
                                            (data-node? c)
                                            #?(:clj (= (class c) clojure.lang.PersistentTreeMap$Black))
                                            #?(:clj (= (class c) clojure.lang.PersistentTreeMap$BlackVal)))
                                        c

                                        (tree-node? c)
                                        (<?resolve c)

                                        :else c)]
                                (if (or (index-node? c)
                                        (data-node? c))
                                  (recur (conj res c) c)
                                  res)))
            path-suffix (-> (interleave sibling-lineage
                                        (repeat 0))
                            (butlast)) ; butlast ensures we end w/ node
            ]
        (-> (pop common-parent-path)
            (conj next-index)
            (into path-suffix))))))




(defn lookup-path
  "Given a B-tree and a key, gets a path into the tree"
  [tree key]
  (go-try
    (loop [path [tree] ;alternating node/index/node/index/node... of the search taken
           cur tree ;current search node
           ]
      (if (seq (:children cur))
        (if (data-node? cur)
          path
          (let [index (lookup cur key)
                child (if (data-node? cur)
                        nil #_(nth-of-set (:children cur) index)
                        (-> (:children cur)
                            ;;TODO what are the semantics for exceeding on the right? currently it's trunc to the last element
                            (nth index (peek (:children cur)))
                            (<?resolve)))
                path' (conj path index child)]
            (recur path' child)))
        nil))))

(defn lookup-key
  "Given a B-tree and a key, gets an iterator into the tree"
  ([tree key]
   (lookup-key tree key nil))
  ([tree key not-found]
   (go-try
     (->
      (-> (<? (lookup-path tree key))
          (peek)
          (<?resolve))
      :children
      (get key not-found)))))

;; this is only for the REPL and testing

(case *async-backend*
  :none
  (do
    (defn forward-iterator
      "Takes the result of a search and returns an iterator going
   forward over the tree. Does lg(n) backtracking sometimes."
      [path start-key]
      (let [start-node (peek path)]
        (assert (data-node? start-node))
        (let [first-elements (-> start-node
                                 :children ; Get the indices of it
                                 (subseq >= start-key)) ; skip to the start-index
              next-elements (lazy-seq
                             (when-let [succ (right-successor (pop path))]
                               (forward-iterator succ start-key)))]
          (concat first-elements next-elements))))


    (defn lookup-fwd-iter
      [tree key]
      (let [path (lookup-path tree key)]
        (when path
          (forward-iterator path key)))))

  :core.async
  (do
    #?(:clj
       (defn chan-seq [ch]
         (when-some [v (<?? ch)]
           (cons v (lazy-seq (chan-seq ch))))))


    (defn forward-iterator
      "Takes the result of a search and puts the iterated elements onto iter-ch
  going forward over the tree as needed. Does lg(n) backtracking sometimes."
      [iter-ch path start-key]
      (go-try
          (loop [path path]
            (if path
              (let  [start-node (peek path)
                     _ (assert (data-node? start-node))
                     elements (-> start-node
                                  :children ; Get the indices of it
                                  (subseq >= start-key))]
                (<? (async/onto-chan iter-ch elements false))
                (recur (<? (right-successor (pop path)))))
              (async/close! iter-ch)))))

    #?(:clj
       (defn lookup-fwd-iter
         "Compatibility helper to clojure sequences. Please prefer the channel
  interface of forward-iterator, as this function blocks your thread, which
  disturbs async contexts and might lead to poor performance. It is mainly here
  to facilitate testing."
         [tree key]
         (let [path (<?? (lookup-path tree key))
               iter-ch (chan)]
           (forward-iterator iter-ch path key)
           (chan-seq iter-ch))))))

(defn insert
  [{:keys [cfg] :as tree} key value]
  (go-try
    (let [path (<? (lookup-path tree key))
          {:keys [children] :or {children (sorted-map-by compare)}} (peek path)
          updated-data-node (data-node cfg (assoc children key value))]
      (loop [node updated-data-node
             path (pop path)]
        (if (empty? path)
          (if (overflow? node)
            (let [{:keys [left right median]} (split-node node)]
              (->IndexNode [left right] (promise-chan) [] cfg))
            node)
          (let [index (peek path)
                {:keys [children keys] :as parent} (peek (pop path))]
            (if (overflow? node) ; splice the split into the parent
              ;;TODO refactor paths to be node/index pairs or 2 vectors or something
              (let [{:keys [left right median]} (split-node node)
                    new-children (catvec (conj (subvec children 0 index)
                                               left right)
                                         (subvec children (inc index)))]
                (recur (-> parent
                           (assoc :children new-children)
                           (dirty!))
                       (pop (pop path))))
              (recur (-> parent
                         ;;TODO this assoc-in seems to be a bottleneck
                         (assoc-in [:children index] node)
                         (dirty!))
                     (pop (pop path))))))))))

;;TODO: cool optimization: when merging children, push as many operations as you can
;;into them to opportunistically minimize overall IO costs

(defn delete
  [{:keys [cfg] :as tree} key]
  (go-try
    (let [path (<? (lookup-path tree key)) ; don't care about the found key or its index
          {:keys [children] :or {children (sorted-map-by compare)}} (peek path)
          updated-data-node (data-node cfg (dissoc children key))]
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
                             (merge-node node (<?resolve (nth children bigger-sibling-idx)))
                             (merge-node (<?resolve (nth children bigger-sibling-idx)) node))
                    old-left-children (subvec children 0 (min index bigger-sibling-idx))
                    old-right-children (subvec children (inc (max index bigger-sibling-idx)))]
                (if (overflow? merged)
                  (let [{:keys [left right median]} (split-node merged)]
                    (recur (->IndexNode (catvec (conj old-left-children left right)
                                                old-right-children)
                                        (promise-chan)
                                        op-buf
                                        cfg)
                           (pop (pop path))))
                  (recur (->IndexNode (catvec (conj old-left-children merged)
                                              old-right-children)
                                      (promise-chan)
                                      op-buf
                                      cfg)
                         (pop (pop path)))))
              (recur (->IndexNode (assoc children index node)
                                  (promise-chan)
                                  op-buf
                                  cfg)
                     (pop (pop path))))))))))

(defn b-tree
  [cfg & kvs]
  (go-try
    (loop [[[k v] & r] (partition 2 kvs)
           t (data-node cfg (sorted-map-by compare))]
      (if k
        (recur r (<? (insert t k v)))
        t)))
  #_(reduce (fn [t [k v]]
            (insert t k v))
          (data-node cfg (sorted-map-by compare))
          (partition 2 kvs)))

(defrecord TestingAddr [last-key node]
  IResolve
  (dirty? [this] false)
  (last-key [_] last-key)
  (resolve [_] node #_(go node)))

#?(:clj
   (defn print-testing-addr
     [node ^Writer writer fully-qualified?]
     (.write writer (if fully-qualified?
                      (pr-str TestingAddr)
                      "TestingAddr"))
     (.write writer (str {}))))

#?(:clj
   (defmethod print-method TestingAddr
     [node writer]
     (print-testing-addr node writer false)))

#?(:clj
   (defmethod print-dup TestingAddr
     [node writer]
     (print-testing-addr node writer true)))

#?(:clj
   (defmethod pp/simple-dispatch TestingAddr
     [node]
     (let [out ^Writer *out*]
       (.write out (str "TestingAddr"
                        (node-status-bits node)))
       (.write out (str {})))))

(defn dirty!
  "Marks a node as being dirty if it was clean"
  [node]
  (assert (not (instance? TestingAddr node)))
  (assoc node :storage-addr (promise-chan)))

;;TODO make this a loop/recur instead of mutual recursion
(declare flush-tree)

(defn flush-children
  [children backend session]
  (go-try
      (loop [[c & r] children
             res []]
        (if-not c
          res
          (recur r (conj res (<? (flush-tree c backend session))))))))

(defprotocol IBackend
  (new-session [backend] "Returns a session object that will collect stats")
  (write-node [backend node session] "Writes the given node to storage, returning a go-block with its assigned address")
  (anchor-root [backend node] "Tells the backend this is a temporary root")
  (delete-addr [backend addr session] "Deletes the given addr from storage"))

(defrecord TestingBackend []
  IBackend
  (new-session [_] (atom {:writes 0}))
  (anchor-root [_ root] root)
  (write-node [_ node session]
    (go-try
      (swap! session update-in [:writes] inc)
      (->TestingAddr (last-key node) node)))
  (delete-addr [_ addr session ]))

(defn flush-tree
  "Given the tree, finds all dirty nodes, delivering addrs into them.
   Every dirty node also gets replaced with its TestingAddr.
   These form a GC cycle, have fun with the unmanaged memory port :)"
  ([tree backend]
   (go-try
     (let [session (new-session backend)
           flushed (<? (flush-tree tree backend session))
           root (anchor-root backend flushed)]
       {:tree (<?resolve root) ; root should never be unresolved for API
        :stats session})))
  ([tree backend stats]
   (go-try
     (if (dirty? tree)
       (let [cleaned-children (if (data-node? tree)
                                (:children tree)
                                ;; TODO throw on nested errors
                                (->> (flush-children (:children tree) backend stats)
                                     <?
                                     catvec))
             cleaned-node (assoc tree :children cleaned-children)
             new-addr (<? (write-node backend cleaned-node stats))]
         (put! (:storage-addr tree) new-addr)
         (when (not= new-addr (<? (:storage-addr tree)))
           (delete-addr backend new-addr stats))
         new-addr)
       tree))))

;; The parts of the serialization system that seem like they're need hooks are:
;; - Must provide a function that takes a node, serializes it, and returns an addr
;; - Must be able to rollback writing an addr
;; - Whatever the addr it returns, it should cache its resolve in-mem somehow
;; - The serialize a node & rollback a node functions should accept a "stats" object as well
;; - The "stats" object must be convertible to a summary or whatever at the end
