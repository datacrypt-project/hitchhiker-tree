(ns hitchhiker.konserve
  (:refer-clojure :exclude [resolve subvec])
  (:require [clojure.core.rrb-vector :refer [catvec subvec]]
            [clojure.core.async :refer [chan <! go <!!] :as async]
            [superv.async :refer [<? go-try S]]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [konserve.filestore :refer [new-fs-store]]
            [hasch.core :refer [uuid]]
            [clojure.set :as set]
            [clojure.core.cache :as cache]
            [hitchhiker.tree.core :as core]
            [hitchhiker.tree.messaging :as msg]))


(defn synthesize-storage-addr
  "Given a key, returns a promise containing that key for use as a storage-addr"
  [key]
  (doto (promise)
    (deliver key)))

(defrecord KonserveAddr [store last-key konserve-key storage-addr]
  core/IResolve
  (dirty? [_] false)
  (last-key [_] last-key)
  (resolve [_ S]
    (go-try S
      (-> (<? S (k/get-in store [konserve-key]))
          #_((fn [e] (prn "get-in" e) e))
          (assoc :storage-addr (synthesize-storage-addr konserve-key))
          #_((fn [e] (prn "assoc" e) e))))))


(defrecord KonserveBackend [store]
  core/IBackend
  (new-session [_] (atom {:writes 0
                          :deletes 0}))
  (anchor-root [_ {:keys [konserve-key] :as node}]
    #_(wcar {} (add-to-expiry redis-key (+ 5000 (System/currentTimeMillis))))
    node)
  (write-node [_ node session]
    (go-try S
      (swap! session update-in [:writes] inc)
      (let [pnode (if (core/index-node? node)
                    (-> (assoc node :storage-addr nil)
                        (update :children (fn [cs] (mapv #(assoc % :store nil
                                                                 :storage-addr nil) cs))))
                    (assoc node :storage-addr nil))]
        (let [id (uuid pnode)]
          (<? S (k/assoc-in store [id] pnode))
          (->KonserveAddr store (core/last-key node) id (synthesize-storage-addr id)))))
    #_(let [key (str (java.util.UUID/randomUUID))
          addr (redis-addr (core/last-key node) key)]
      ;(.submit service #(wcar {} (car/set key node)))
      (when (some #(not (satisfies? msg/IOperation %)) (:op-buf node))
        (println (str "Found a broken node, has " (count (:op-buf node)) " ops"))
        (println (str "The node data is " node))
        (println (str "and " (:op-buf node))))
      (wcar {}
            (car/set key node)
            (when (core/index-node? node)
              (add-refs key
                        (for [child (:children node)
                              :let [child-key @(:storage-addr child)]]
                          child-key))))
      (seed-cache! key (doto (promise) (deliver node)))
      addr))
  (delete-addr [_ addr session]
    #_(wcar {} (car/del addr))
    (swap! session update-in :deletes inc)))

(defn get-root-key
  [tree]
  (-> tree :storage-addr (deref 10 nil)))

(defn create-tree-from-root-key
  [store root-key]
  (go-try S
    (let [val (<? S (k/get-in store [root-key]))
          last-key (core/last-key (assoc val :storage-addr (synthesize-storage-addr root-key)))] ; need last key to bootstrap
      (<? S (core/resolve
             (->KonserveAddr store last-key root-key (synthesize-storage-addr root-key))
             S)))))

(defn add-read-handlers [store]
  (swap! (:read-handlers store) merge
         {'hitchhiker.konserve.KonserveAddr
          #(-> % map->KonserveAddr
               (assoc :store store
                      :storage-addr (synthesize-storage-addr (:konserve-key %))))
          'hitchhiker.tree.core.DataNode
          (fn [{:keys [children cfg]}]
            (core/->DataNode (into (sorted-map-by
                                    compare) children)
                             (promise)
                             cfg))
          'hitchhiker.tree.core.IndexNode
          (fn [{:keys [children cfg op-buf]}]
            (core/->IndexNode (->> children
                                   #_((fn [e] (prn "reading" e) e))
                                   catvec)
                              (promise)
                              (catvec op-buf)
                              cfg))
          'hitchhiker.tree.messaging.InsertOp
          msg/map->InsertOp
          'hitchhiker.tree.messaging.DeleteOp
          msg/map->DeleteOp
          'hitchhiker.tree.core.Config
          core/map->Config})
  store)

(comment


  (def store (add-read-handlers (<!! (new-fs-store "/tmp/async-hitchhiker-repl"
                                                   :config {:fsync true}))))



  (def my-tree (<!! (core/flush-tree
                     (time (reduce (fn [t i]
                                        ;                              ^{:break/when (> i 49)}
                                     (<!! (msg/insert t i i)))
                                   (<!! (core/b-tree (core/->Config 17 300 (- 300 17))))
                                   (range 20000)))
                     (->KonserveBackend store)
                     )))



  (def my-tree-updated (<!! (core/flush-tree
                             (<!! (msg/delete (<!! (create-tree-from-root-key store (get-root-key (:tree my-tree)))) 10))
                             (->KonserveBackend store)
                             )))




  (time (<!! (msg/lookup (<!! (create-tree-from-root-key store (get-root-key (:tree my-tree-updated)))) 100)))

(reduce (fn [t i]
          (let [st (System/currentTimeMillis)
                tree (:tree (<!! (core/flush-tree (<!! (msg/insert t i i))
                                                  (->KonserveBackend store))))]
            (when (= (mod i 100) 0)
              (let [delta (- (System/currentTimeMillis)
                             st)]
                (println "Op for" i " took " delta " ms")))
            tree))
        (<!! (core/b-tree (core/->Config 17 300 (- 300 17))))
        (range 20000))


(msg/lookup-fwd-iter (<!! (create-tree-from-root-key store (get-root-key (:tree my-tree))))
                     19899)





)
