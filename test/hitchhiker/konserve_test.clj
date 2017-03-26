(ns hitchhiker.konserve-test
  (:require [clojure.core.rrb-vector :refer [catvec]]
            [clojure.test :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [konserve.filestore :refer [new-fs-store delete-store]]
            [hitchhiker.konserve :as kons]
            [hitchhiker.tree.core :as core]
            hitchhiker.tree.core-test
            [hitchhiker.tree.messaging :as msg]
            [clojure.core.async :refer [<!!]]))

(defn setup-store [folder]
  (let [read-handlers (atom {})
          store (<!! (new-fs-store folder
                                   :read-handlers read-handlers
                                   :config {:fsync true}))]
      (reset! read-handlers {'hitchhiker.konserve.KonserveAddr
                             #(-> % kons/map->KonserveAddr
                                  (assoc :store store
                                         :storage-addr (kons/synthesize-storage-addr (:konserve-key %))))
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
      store))

(deftest simple-konserve-test
  (testing "Insert and lookup"
    (let [folder "/tmp/async-hitchhiker-tree-test"
          store (setup-store folder)
          backend (kons/->KonserveBackend store)
          flushed (<!! (core/flush-tree
                        (time (reduce (fn [t i]
                                        (<!! (msg/insert t i i)))
                                      (<!! (core/b-tree (core/->Config 1 3 (- 3 1))))
                                      (range 1 11)))
                        backend))
          root-key (kons/get-root-key (:tree flushed))
          tree (<!! (kons/create-tree-from-root-key store root-key))]
      (is (= (<!! (msg/lookup tree -10)) nil))
      (is (= (<!! (msg/lookup tree 100)) nil))
      (dotimes [i 10]
        (is (= (<!! (msg/lookup tree (inc i))) (inc i))))
      (is (= (map first (msg/lookup-fwd-iter tree 4)) (range 4 11)))
      (is (= (map first (msg/lookup-fwd-iter tree 0)) (range 1 11)))
      (let [deleted (<!! (core/flush-tree (<!! (msg/delete tree 3)) backend))
            root-key (kons/get-root-key (:tree deleted))
            tree (<!! (kons/create-tree-from-root-key store root-key))]
        (is (= (<!! (msg/lookup tree 2)) 2))
        (is (= (<!! (msg/lookup tree 3)) nil))
        (is (= (<!! (msg/lookup tree 4)) 4)))
      (delete-store folder))))
