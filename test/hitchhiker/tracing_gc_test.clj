(ns hitchhiker.tracing-gc-test
  (:require
   [clojure.set :as set]
   [clojure.test.check :as tc]
   [clojure.test.check.clojure-test :refer [defspec]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [hitchhiker.tree.core :as tree]
   [hitchhiker.tracing-gc :as gc]))

(defn tree-insert
  [t k]
  (tree/insert t k k))

(def gen-tree
  (gen/bind (gen/vector gen/int)
            (fn [vs]
              (let [tree (reduce tree-insert (tree/b-tree (tree/->Config 3 2 1))
                                 vs)]
                (gen/return tree)))))

(defn tree-keys
  [b-tree]
  (tree-seq tree/index-node? :children b-tree))

(defspec unreferenced-keys-are-deleted
  1000
  (prop/for-all [live gen-tree
                 dead gen-tree]
    (let [deleted (atom (set []))
          delete-fn (fn [item]
                       (swap! deleted conj item))]
      (gc/trace-gc! (gc/in-mem-scratch) [live]
                    (-> (concat (tree-keys live)
                                (tree-keys dead))
                        (shuffle))
                    delete-fn)
      (= (set @deleted) (set (tree-keys dead))))))
