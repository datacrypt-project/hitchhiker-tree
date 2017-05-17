(ns hitchhiker.konserve-test
  (:refer-clojure :exclude [vec])
  (:require [clojure.core.rrb-vector :refer [catvec vec]]
            [#?(:clj clojure.test :cljs cljs.test)
             #?(:clj :refer :cljs :refer-macros) [deftest testing run-tests is
                                                  #?(:cljs async)]]
            [clojure.test.check.clojure-test #?(:clj :refer :cljs :refer-macros) [defspec]]
            [clojure.test.check.generators :as gen #?@(:cljs [:include-macros true])]
            [clojure.test.check.properties :as prop #?@(:cljs [:include-macros true])]
            #?(:clj [konserve.filestore :refer [new-fs-store delete-store list-keys]])
            [konserve.memory :refer [new-mem-store]]
            [hitchhiker.konserve :as kons]
            [hitchhiker.tree.core #?(:clj :refer :cljs :refer-macros)
             [<?? <? go-try] :as core]
            [hitchhiker.tree.messaging :as msg]
            [hitchhiker.ops :refer [recorded-ops]]
            #?(:cljs [cljs.core.async :refer [promise-chan] :as async]
               :clj [clojure.core.async :refer [promise-chan] :as async])
            #?(:cljs [cljs.nodejs :as nodejs])))

#?(:cljs
   (do
     (nodejs/enable-util-print!)
     (enable-console-print!)))


(defn iter-helper [tree key]
  (go-try
      (let [iter-ch (async/chan)
            path (<? (core/lookup-path tree key))]
        (msg/forward-iterator iter-ch path key)
        (<? (async/into [] iter-ch)))))


(deftest simple-konserve-test
  (testing "Insert and lookup"
    #?(:cljs
       (async done
        (go-try
         (let [store (<? (new-mem-store))
               backend (kons/->KonserveBackend store)
               init-tree (<? (core/reduce< (fn [t i] (msg/insert t i i))
                                           (<? (core/b-tree (core/->Config 1 3 (- 3 1))))
                                           (range 1 11)))
               flushed (<? (core/flush-tree init-tree backend))
               root-key (kons/get-root-key (:tree flushed))
               tree (<? (kons/create-tree-from-root-key store root-key))]
           (is (= (<? (msg/lookup tree -10)) nil))
           (is (= (<? (msg/lookup tree 100)) nil))
           (dotimes [i 10]
             (is (= (<? (msg/lookup tree (inc i))) (inc i))))
           (is (= (map first (<? (iter-helper tree 4))) (range 4 11)))
           (is (= (map first (<? (iter-helper tree 0))) (range 1 11)))
           (let [deleted (<? (core/flush-tree (<? (msg/delete tree 3)) backend))
                 root-key (kons/get-root-key (:tree deleted))
                 tree (<? (kons/create-tree-from-root-key store root-key))]
             (is (= (<? (msg/lookup tree 2)) 2))
             (is (= (<? (msg/lookup tree 3)) nil))
             (is (= (<? (msg/lookup tree 4)) 4)))
           (done)))))
    #?(:clj
       (let [folder "/tmp/async-hitchhiker-tree-test"
             _ (delete-store folder)
             store (kons/add-hitchhiker-tree-handlers (<?? (new-fs-store folder :config {:fsync false})))
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
         (delete-store folder)))))



;; adapted from redis tests

(defn insert
  [t k]
  (msg/insert t k k))

(comment
  (def recorded-ops (atom []))

  (binding [*print-length* -1]
    (spit "/tmp/ops" (pr-str (vec (take 100 @recorded-ops)))))

  (count @recorded-ops)

  (first @recorded-ops))

(defn ops-test [ops universe-size]
  (go-try
      (let [folder "/tmp/konserve-mixed-workload"
            _ #?(:clj (delete-store folder) :cljs nil)
            store (kons/add-hitchhiker-tree-handlers
                   (<? #?(:clj (new-fs-store folder :config {:fsync false})
                          :cljs (new-mem-store))))
            _ #?(:clj (assert (empty? (<? (list-keys store)))
                              "Start with no keys")
                 :cljs nil)
            ;_ (swap! recorded-ops conj ops)
            [b-tree root set]
            (<? (core/reduce< (fn [[t root set] [op x]]
                                (go-try
                                    (let [x-reduced (when x (mod x universe-size))]
                                      (condp = op
                                        :flush (let [flushed (<? (core/flush-tree t (kons/->KonserveBackend store)))
                                                     t (:tree flushed)]
                                                 [t (<? (:storage-addr t)) set])
                                        :add [(<? (insert t x-reduced)) root (conj set x-reduced)]
                                        :del [(<? (msg/delete t x-reduced)) root (disj set x-reduced)]))))
                              [(<? (core/b-tree (core/->Config 3 3 2))) nil #{}]
                              ops))]
        (let [b-tree-order (map first (<? (iter-helper b-tree -1)))
              res (= b-tree-order (seq (sort set)))]
          (assert res (str "These are unequal: " (pr-str b-tree-order) " " (pr-str (seq (sort set)))))
          #?(:clj (delete-store folder))
          res))))

;; TODO recheck when https://dev.clojure.org/jira/browse/TCHECK-128 is fixed
;; and share clj mixed-op-seq test, remove ops.cljc then.
#?(:cljs
   (deftest manual-mixed-op-seq
     (async done
            (go-try
                (loop [[ops & r] recorded-ops]
                  (when ops
                    (is (<? (ops-test ops 1000)))
                    (recur r)))
              (done)))))


#?(:clj
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
                   (<?? (ops-test ops universe-size)))))


#?(:clj
   (defspec test-many-keys-bigger-trees
     100
     (mixed-op-seq 800 200 10 1000 1000)))




(comment
  ;; macroexpanded
  (defn test-many-keys-bigger-trees
  ([]
   (let [options__29789__auto__ (clojure.test.check.clojure-test/process-options 100)]
     (apply test-many-keys-bigger-trees
            (:num-tests options__29789__auto__)
            (apply concat options__29789__auto__))))
  ([times & {:as quick-check-opts, :keys [seed max-size]}]
   (go-try
       (apply clojure.test.check/quick-check
              times
              (vary-meta (<? (mixed-op-seq 800 200 10 1000 1000))
                         assoc :name (str (quote (mixed-op-seq 800 200 10 1000 1000))))
              (apply concat quick-check-opts)))))

  (<?? (test-many-keys-bigger-trees))
  (macroexpand-1 '(defspec test-many-keys-bigger-trees
                    100
                    (mixed-op-seq 800 200 10 1000 1000))))

#?(:cljs
   (defn ^:export test-all [cb]
     (defmethod cljs.test/report [:cljs.test/default :end-run-tests] [m]
       (cb (clj->js m))
       (if (cljs.test/successful? m)
         (println "Success!")
         (println "FAIL")))
     (run-tests)))
