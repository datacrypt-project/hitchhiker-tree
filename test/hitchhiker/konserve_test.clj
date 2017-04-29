(ns hitchhiker.konserve-test
  (:refer-clojure :exclude [vec])
  (:require [clojure.core.rrb-vector :refer [catvec vec]]
            [clojure.test :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [konserve.filestore :refer [new-fs-store delete-store list-keys]]
            [hitchhiker.konserve :as kons]
            [hitchhiker.tree.core :refer [<??] :as core]
            hitchhiker.tree.core-test
            [hitchhiker.tree.messaging :as msg]
            [clojure.core.async :refer [promise-chan]]))

(defn setup-store [folder]
  (let [read-handlers (atom {})
        store (<?? (new-fs-store folder
                                 :read-handlers read-handlers
                                 :config {:fsync false}))]
      (reset! read-handlers {'hitchhiker.konserve.KonserveAddr
                             #(-> % kons/map->KonserveAddr
                                  (assoc :store store
                                         :storage-addr (kons/synthesize-storage-addr (:konserve-key %))))
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
      store))


(deftest simple-konserve-test
  (testing "Insert and lookup"
    (let [folder "/tmp/async-hitchhiker-tree-test"
          store (setup-store folder)
          backend (kons/->KonserveBackend store)
          flushed (<?? (core/flush-tree
                        (time (reduce (fn [t i]
                                        (<?? (msg/insert t i i)))
                                      (<?? (core/b-tree (core/->Config 1 3 (- 3 1))))
                                      (range 1 11)))
                        backend))
          root-key (kons/get-root-key (:tree flushed))
          tree (<?? (kons/create-tree-from-root-key store root-key))]
      (is (= (<?? (msg/lookup tree -10)) nil))
      (is (= (<?? (msg/lookup tree 100)) nil))
      (dotimes [i 10]
        (is (= (<?? (msg/lookup tree (inc i))) (inc i))))
      (is (= (map first (msg/lookup-fwd-iter tree 4)) (range 4 11)))
      (is (= (map first (msg/lookup-fwd-iter tree 0)) (range 1 11)))
      (let [deleted (<?? (core/flush-tree (<?? (msg/delete tree 3)) backend))
            root-key (kons/get-root-key (:tree deleted))
            tree (<?? (kons/create-tree-from-root-key store root-key))]
        (is (= (<?? (msg/lookup tree 2)) 2))
        (is (= (<?? (msg/lookup tree 3)) nil))
        (is (= (<?? (msg/lookup tree 4)) 4)))
      (delete-store folder))))



;; adapted from redis tests

(defn insert
  [t k]
  (msg/insert t k k))

(defn lookup-fwd-iter
  [t v]
  (seq (map first (msg/lookup-fwd-iter t v))))

(defn mixed-op-seq
  "This is like the basic mixed-op-seq tests, but it also mixes in flushes to a konserve filestore"
  [add-freq del-freq flush-freq universe-size num-ops]
  (prop/for-all [ops (gen/vector (gen/frequency
                                   [[add-freq (gen/tuple (gen/return :add)
                                                         (gen/no-shrink gen/int))]
                                    [flush-freq (gen/return [:flush])]
                                    [del-freq (gen/tuple (gen/return :del)
                                                         (gen/no-shrink gen/int))]])
                                 num-ops)]
                
                (let [folder "/tmp/konserve-mixed-workload"
                      store (setup-store folder)
                      _ (assert (empty? (<?? (list-keys store)))
                                "Start with no keys")
                      [b-tree root set]
                      (reduce (fn [[t root set] [op x]]
                                       (let [x-reduced (when x (mod x universe-size))]
                                         (condp = op
                                           :flush (let [flushed (<?? (core/flush-tree t (kons/->KonserveBackend store)))
                                                        t (:tree flushed)]
                                                    #_(when root
                                                      (wcar {} (redis/drop-ref root)))
                                                    #_(println "flush")
                                                    (when-not (:storage-addr t)
                                                      (println "TTT" t flushed root op))
                                                    [t (when (:storage-addr t) (<?? (:storage-addr t))) set])
                                           :add (do #_(println "add") [(<?? (insert t x-reduced)) root (conj set x-reduced)])
                                           :del (do #_(println "del") [(<?? (msg/delete t x-reduced)) root (disj set x-reduced)]))))
                              [(<?? (core/b-tree (core/->Config 3 3 2))) nil #{}]
                              ops)]
                  #_(println "Make it to the end of a test, tree has" (count (lookup-fwd-iter b-tree -1)) "keys left")
                  (let [b-tree-order (lookup-fwd-iter b-tree -1)
                        res (= b-tree-order (seq (sort set)))]
                    #_(wcar {} (redis/drop-ref root))
                    #_(assert (let [ks (wcar {} (car/keys "*"))]
                              (or (empty? ks)
                                  (= ["refcount:expiry"] ks))) "End with no keys")
                    (assert res (str "These are unequal: " (pr-str b-tree-order) " " (pr-str (seq (sort set)))))
                    (delete-store folder)
                    res))))


(defspec test-many-keys-bigger-trees
  100
  (mixed-op-seq 800 200 10 1000 1000))
