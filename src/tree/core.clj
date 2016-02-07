(ns tree.core
  (:refer-clojure :exclude [compare resolve]))

(def b 3)

(defprotocol INodeLookup
  (lookup [node key] "Returns the child node which contains the given key"))

(defprotocol IKeyCompare
  (compare [key1 key2]))

(defprotocol IInsert
  (insert [node index new-child]
          [node index new-child1 median new-child2]
          "Returns one or 2 new nodes, if it had to split"
          ))

(extend-protocol IKeyCompare
  Object
  (compare [key1 key2] (clojure.core/compare key1 key2)))

(defprotocol IResolve
  "This is how we store the children. The indirection enables background
   fetch and decode of the resource."
  (resolve [this]))

(defn scan-children-array
  "This function takes an array of keys. There must be an odd # of elts in it.

   It returns the last index which is less than to the given key,
   unless no such index exists, in which case it returns the greatest index"
  [keys key]
  (let [key-len (count keys)]
    (loop [i 0]
      (if (> key-len i) ;; Are there more elements?
        (let [result (compare (nth keys i) key)]
          (cond
            (neg? result) ;; If current key is smaller, keep scanning
            (recur (inc i))
            (or (zero? result) (pos? result))
            i
            :else
            (throw (ex-info "lol" {:no :darn}))))
        ;; All keys are smaller
        key-len))))

;; TODO enforce that there always (= (count children) (inc (count keys)))
;;
;; TODO we should be able to find all uncommited data by searching for
;; resolved & unresolved children

(defrecord IndexNode [keys children]
  IResolve
  (resolve [this] this) ;;TODO this is a hack for testing
  IInsert
  (insert
    [node index new-child]
    (let [new-children (assoc children index new-child)]
      [(->IndexNode keys new-children)]))
  (insert [node index new-child1 median new-child2]
    (let [new-children (vec (concat (take index children)
                                    [new-child1 new-child2]
                                    (drop (inc index) children)))
          ;;TODO find a better datastructure than vector
          ;;use the vector with lg time split/merge
          new-keys (vec (concat (take index keys)
                                [median]
                                (drop index keys)))]
      (if (>= (count new-children) (* 2 b))
        (let [split-med (nth new-keys (dec b))
              left-index (->IndexNode (vec (take (dec b) new-keys))
                                      (vec (take b new-children)))
              right-index (->IndexNode (vec (drop b new-keys))
                                       (vec (drop b new-children)))
              median (nth new-keys (dec b))]
          [left-index median right-index])
        [(->IndexNode new-keys
                      new-children)])))
  INodeLookup
  (lookup [root key]
    (scan-children-array keys key)))

(defrecord RootNode [keys children]
  IInsert
  (insert
    [node index new-child]
    (let [new-children (assoc children index new-child)]
      [(->RootNode keys new-children)]))
  (insert [node index new-child1 median new-child2]
    ;(println "root recieving split children")
    (let [new-children (vec (concat (take index children)
                                    [new-child1 new-child2]
                                    (drop (inc index) children)))
          ;;TODO find a better datastructure than vector
          ;;use the vector with lg time split/merge
          new-keys (vec (concat (take index keys)
                                [median]
                                (drop index keys)))]
      ;(println "new keys:" new-keys)
      (if (>= (count new-children) (* 2 b))
        (let [split-med (nth new-keys (dec b))
              left-index (->IndexNode (vec (take (dec b) new-keys))
                                      (vec (take b new-children)))
              right-index (->IndexNode (vec (drop b new-keys))
                                       (vec (drop b new-children)))
              median (nth new-keys (dec b))]
          [left-index median right-index])
        [(->RootNode new-keys
                     new-children)])))
  INodeLookup
  (lookup [root key]
    (scan-children-array keys key)))

(defrecord DataNode [children]
  IResolve
  (resolve [this] this) ;;TODO this is a hack for testing
  IInsert
  (insert
    [node index key]
    ;cases:
    ;1. index > children; append to end of children
    ;2. index within children
    ;2.1. If index is equal, skip
    ;2.2 If unequal, slip-insert
    ;3. possibly split
    ;(println "index" index "(count children)" (count children))
    ;(println "key" key "children" children)
    (assert (<= 0 index (count children)) "index have a value out of the defined meaning")
    (let [new-data-children (cond
                              (= index (count children))
                              (conj children key)
                              (= (nth children index) key) ;;TODO this case could bypass
                              children
                              :else
                              (vec (concat (take index children)
                                           [key]
                                           (drop index children))))]
      (if (>= (count new-data-children) (* 2 b))
        [(->DataNode (vec (take b new-data-children)))
         (nth new-data-children (dec b))
         (->DataNode (vec (drop b new-data-children)))]
        [(->DataNode new-data-children)])))
  (insert [node index new-child1 median new-child2]
    (throw (ex-info "impossible--only for index or root nodes" {}))) 
  INodeLookup
  (lookup [root key]
    (loop [i 0]
      (if (= i (count children))
        i
        (let [result (compare key (nth children i))]
          (if (pos? result)
            (recur (inc i))
            i))))))

(defn forward-iterator
  "Takes the result of a search and returns an iterator going
   forward over the tree. Does lg(n) backtracking sometimes."
  [path start-index]
  (let [start-node (peek path)]
    (assert (instance? DataNode start-node))
    (let [first-elements (->> start-node
                              :children ; Get the indices of it
                              (drop start-index)) ; skip to the start-index
          next-elements (lazy-seq
                          (loop [path path]
                            (let [tmp (pop path)]
                              (if-let [cur-index (peek tmp)] ; could we have a sibling?
                                (let [next-index (inc cur-index)
                                      path-prefix (pop tmp)]
                                  ;; Do we have a sibling?
                                  (if (>= next-index (count (:children (peek path-prefix))))
                                    (recur path-prefix)
                                    (let [new-sibling (nth (:children (peek path-prefix))
                                                           next-index)
                                          ;; We must get back down to the data node
                                          sibling-lineage (->> new-sibling
                                                               (iterate #(-> % :children first))
                                                               (take-while #(or (instance? IndexNode %)
                                                                                (instance? DataNode %))))
                                          path-suffix (butlast (interleave sibling-lineage
                                                                           (repeat 0)))]
                                      (forward-iterator 
                                        (into (conj path-prefix
                                                    next-index ; the sibling's index
                                                    )
                                              path-suffix)
                                        0)))) ; always start at the first elt in the node
                                nil ;nowhere to go from the root
                                ))))]
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

(defn insert-key
  [tree new-key]
  ;; The path's structure will be:
  ;; greater-key / greater-index / data-node / index / index-node / index / root
  ;;
  ;; Our goal is to handle the 3 insertion cases:
  ;; for the data-node, the index-node, and the root
  ;;
  ;; For the data-node, we'll smash together the new children in order
  ;; if it's too big, we split & find a new median
  ;; 
  ;; For the index nodes, we'll see if we have 2 children or 1
  ;; if 1, we'll just fix the child pointer and continue
  ;; if 2, we'll smash together the new children, and if too big, we split & find median
  ;;
  ;; For the root, we'll smash it together a last time; if it's too big, we reroot
  ;; otherwise, we're done
  (let [path (lookup-path tree new-key)]
    ;(println "# insert-key")
    ;(clojure.pprint/pprint tree)
    ;(println "Doing insert with path" (map #(:keys % %) path))
    (if path
      (loop [path (next (rseq path))
             new-elts [new-key]]
        ;(println "path is" path)
        (let [insert-index (first path)
              insert-node (fnext path)
              insert-result (apply insert insert-node insert-index new-elts)]
          ;(println "insert result was" insert-result)
          (if (= 2 (count path)) ; we've only got the root node
            (if (= 1 (count insert-result))
              (first insert-result)
              (let [[l m r] insert-result]
                (->RootNode [m] [l r])))
            (recur (nnext path) insert-result))))
      ;; Special case for insert into empty tree, since we can't compute paths yet
      (->RootNode [] [(->DataNode [new-key])]))))

(defn empty-b-tree
  []
  (->RootNode [] [(->DataNode [])]))

#_(let [x [1 2 3 4 5]
        i (scan-children-array x 2.5)]
    (println i)
    (concat (take i x) [2.5] (drop i x))
    )

;(println "insert:" (insert (->DataNode [1 2 3 4]) 2 2.5))
