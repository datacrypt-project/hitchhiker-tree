(ns hitchhiker.konserve
  (:refer-clojure :exclude [resolve subvec])
  (:require [clojure.core.rrb-vector :refer [catvec subvec]]
            #?(:clj [clojure.core.async :refer [chan promise-chan put!] :as async]
               :cljs [cljs.core.async :refer [chan promise-chan put!] :as async])
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [hasch.core :refer [uuid]]
            [clojure.set :as set]
            #?(:clj [hitchhiker.tree.core :refer [go-try <? <??] :as core]
               :cljs [hitchhiker.tree.core :as core])
            [hitchhiker.tree.messaging :as msg])
  #?(:cljs (:require-macros [hitchhiker.tree.core :refer [go-try <?]])))


(defn synthesize-storage-addr
  "Given a key, returns a promise containing that key for use as a storage-addr"
  [key]
  (doto (promise-chan)
    (put! key)))

(defrecord KonserveAddr [store last-key konserve-key storage-addr]
  core/IResolve
  (dirty? [_] false)
  (last-key [_] last-key)
  (resolve [_]
    (go-try
     (let [ch (k/get-in store [konserve-key])]
       (-> (case core/*async-backend*
             :none (async/<!! ch)
             :core.async (<? ch))
           (assoc :storage-addr (synthesize-storage-addr konserve-key)))))))


(defrecord KonserveBackend [store]
  core/IBackend
  (new-session [_] (atom {:writes 0
                          :deletes 0}))
  (anchor-root [_ {:keys [konserve-key] :as node}]
    node)
  (write-node [_ node session]
    (go-try
      (swap! session update-in [:writes] inc)
      (let [pnode (if (core/index-node? node)
                    (-> (assoc node :storage-addr nil)
                        (update :children (fn [cs] (mapv #(assoc % :store nil
                                                                 :storage-addr nil) cs))))
                    (assoc node :storage-addr nil))]
        (let [id (uuid pnode)
              ch (k/assoc-in store [id] node)]
          (case core/*async-backend*
            :none (async/<!! ch)
            :core.async (<? ch))
          (->KonserveAddr store (core/last-key node) id (synthesize-storage-addr id))))))
  (delete-addr [_ addr session]
    (swap! session update :deletes inc)))

(defn get-root-key
  [tree]
  ;; TODO find out why this is inconsistent
  (or
    (-> tree :storage-addr (async/poll!) :konserve-key)
    (-> tree :storage-addr (async/poll!))))

  (defn create-tree-from-root-key
    [store root-key]
    (go-try
     (let [val (let [ch (k/get-in store [root-key])]
                 (case core/*async-backend*
                   :none (async/<!! ch)
                   :core.async (<? ch)))
            last-key (core/last-key (assoc val :storage-addr (synthesize-storage-addr root-key)))] ; need last key to bootstrap
        (<? (core/resolve
            (->KonserveAddr store last-key root-key (synthesize-storage-addr root-key)))))))


  (defn add-hitchhiker-tree-handlers [store]
    (swap! (:read-handlers store) merge
          {'hitchhiker.konserve.KonserveAddr
            #(-> % map->KonserveAddr
                (assoc :store store
                        :storage-addr (synthesize-storage-addr (:konserve-key %))))
            'hitchhiker.tree.core.DataNode
            (fn [{:keys [children cfg]}]
              (core/->DataNode (into (sorted-map-by
                                      compare) children)
                              (promise-chan)
                              cfg))
            'hitchhiker.tree.core.IndexNode
            (fn [{:keys [children cfg op-buf]}]
              (core/->IndexNode (->> children
                                    vec)
                                (promise-chan)
                              (vec op-buf)
                              cfg))
          'hitchhiker.tree.messaging.InsertOp
          msg/map->InsertOp
          'hitchhiker.tree.messaging.DeleteOp
          msg/map->DeleteOp
          'hitchhiker.tree.core.Config
          core/map->Config})
  (swap! (:write-handlers store) merge
         {'hitchhiker.konserve.KonserveAddr
          (fn [addr]
            (assoc addr
                   :store nil
                   :storage-addr nil))
          'hitchhiker.tree.core.DataNode
          (fn [node]
            (assoc node :storage-addr nil))
          'hitchhiker.tree.core.IndexNode
          (fn [node]
            (-> node
                (assoc :storage-addr nil)
                (update-in [:children]
                           (fn [cs] (map #(assoc % :store nil :storage-addr nil) cs)))))}) 
  store)



(comment


  (def store (add-read-handlers (<!! (new-fs-store "/tmp/async-hitchhiker-repl"
                                                   :config {:fsync true}))))

  (go-try (def store (add-read-handlers (<? (new-mem-store)))))

  (def my-tree (<!! (core/flush-tree
                     (time (reduce (fn [t i]
                                        ;                              ^{:break/when (> i 49)}
                                     (<!! (msg/insert t i i)))
                                   (<!! (core/b-tree (core/->Config 17 300 (- 300 17))))
                                   (range 20000)))
                     (->KonserveBackend store)
                     )))

  (enable-console-print!)

  (go-try (def foo (<? (msg/insert (<? (core/b-tree (core/->Config 17 300 (- 300 17))))
                               1 1))))

  (go-try (def foos (<? (msg/insert (<? (msg/insert (<? (core/b-tree (core/->Config 17 300 (- 300 17))))
                                               1 1))
                               2 2))))

  (go-try (def bar (<? (msg/delete foos 2))))



  (go-try
   (prn "foo"
        (loop [i 0
               t (<? (core/b-tree (core/->Config 17 300 (- 300 17))))]
          (if (= i 2)
            t
            (recur (inc i) (<? (msg/insert t i i)))))))

  (time (reduce (fn [t i]
                  (<!! (msg/insert t i i)))
                (<!! (core/b-tree (core/->Config 17 300 (- 300 17))))
                (range 20000)))



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
