(ns tree.redis
  (:require [clojure.pprint :as pp]
            [taoensso.carmine :as car :refer (wcar)]
            [tree.core :as core]
            [clojure.core.memoize :as memo]
            [tree.messaging :as msg]))

(def totally-fetch
  (memo/lru (fn [redis-key]
              (loop [i 0]
                (if (= i 1000)
                  (do (println "total fail") (System/exit 1))
                  (let [x (wcar {} (car/get redis-key))]
                    (if x
                      x
                      (do (Thread/sleep 25) (recur (inc i))))))))
            :lru/threshold 400))

(defrecord RedisAddr [last-key redis-key]
  core/IResolve
  (dirty? [_] false)
  (last-key [_] last-key)
  (resolve [_] (let [x (-> (totally-fetch redis-key)
                   (assoc :storage-addr
                          (doto (promise)
                            (deliver redis-key))))]
                 ;(println "Deser:" x)
                 x
                 )))

(defrecord RedisBackend [#_service]
  core/IBackend
  (new-session [_] (atom {:writes 0
                          :deletes 0}))
  (write-node [_ node session]
    (swap! session update-in [:writes] inc)
    (let [key (str (java.util.UUID/randomUUID))]
      ;(.submit service #(wcar {} (car/set key node)))
      (wcar {} (car/set key node))
      (->RedisAddr (core/last-key node) key)))
  (delete-addr [_ addr session]
    (wcar {} (car/del addr))
    (swap! session update-in :deletes inc)))

(defn get-root-key
  [tree]
  (-> tree :storage-addr (deref 10)))

(defn create-tree-from-root-key
  [root-key]
  (let [last-key (core/last-key (wcar {} (car/get root-key)))] ; need last key to bootstrap
    (core/resolve
      (->RedisAddr last-key root-key))))

(comment
  (wcar {} (car/ping) (car/set "foo" "bar") (car/get "foo"))

(println "cleared"
         (wcar {} (apply car/del
                         (count (wcar {} (car/keys "*")))))))

;; Benchmarks:
;; We'll have 2 workloads: in-order (the natural numbers) and random (doubles in 0-1)
;; We'll record 2 things:
;; - Series of timings per 0.1% of inserts
;; - Series of flush cost per X keys
;; The flush batch size should be a factor of b or of n--the benchmarks should
;; see results for both.
;; We'll do this for msg and core versions
;; We'll also benchmark a sortedset
;;
;; We'll look at the plots with log & linear R^2 values
;;
;; There should also be a burn-in test to confirm
;; Will be easier for testing after we add KV. Then build a dataset to store there.
;;

(comment
  (def write-stats (atom []))
  (def roots (atom []))
  (def depth (atom []))

  (println giant-tree)
  (spit "writes.csv" (clojure.string/join "\n" (map :writes @write-stats)))
  (println roots)
  (println depth)
  (/ (* 100000 (count @roots)) 1000000000.0)
(/ 1000000000 10000)

  (def giant-tree
    (reduce (fn [tree [element index]]
                      (let [tree (core/insert tree [element (quot index 1000)])
                            {:keys [tree stats]} (if (zero? (mod index 1000))
                                                   (core/flush-tree tree (->RedisBackend))
                                                   {:tree tree})]
                        (when stats
                          (try
                            (swap! depth conj (count (core/lookup-path tree 0)))
                            (catch Exception e
                              (clojure.pprint/pprint tree)
                              (throw e)))
                          (swap! write-stats conj @stats)
                          (swap! roots conj (-> tree :storage-addr (deref 10 nil))))
                        tree))
                    (core/b-tree (core/->Config 500 700 30))
                    (map vector #_(range) (repeatedly 1000000000 rand) (range))))

  (def iters 100000000)
  (println (first growth-flush-curve))
  (count (msg/lookup-fwd-iter (create-tree-from-root-key (first growth-flush-curve)) -10))
  (println "Total writes" (apply + (second growth-flush-curve)))
  ;1258 for msg
  ;1310 for core
  (do
    (def writes (atom []))
    (def n-seq (atom []))
    )
        (spit "curve.csv" (clojure.string/join "\n" (map #(str %1 "," %2) @writes @n-seq)))
  (deref writes)
  (println growth-flush-curve)
  (def growth-flush-curve
    (future
      (let [tree-atom (atom (core/b-tree (core/->Config 8 9 3)))]
        (dotimes [i iters]
          (let [sample? (zero? (mod i (quot iters 500)))
                tree @tree-atom
                {:keys [tree stats]} (if sample?
                                       (-> tree
                                           (msg/insert (str (rand)))
                                           (core/flush-tree (->RedisBackend) #_(core/->TestingBackend)))
                                       {:tree (msg/insert tree (str (rand)))})]
            (reset! tree-atom tree)
            (when stats
              (swap! writes conj (:writes @stats))
              (swap! n-seq conj i))))
        [(-> @tree-atom :storage-addr deref) writes])))

  (def t 
    (core/flush-tree
      (apply core/b-tree (core/->Config 5 3) (range 100)) (->RedisBackend)))

  (clojure.pprint/pprint t)
  (msg/lookup-fwd-iter (:tree t) -10)

(require '[criterium.core :refer (quick-bench)])
  (quick-bench (apply core/b-tree (core/->Config 70 80 10) (repeatedly 1000 rand)))

  (time (core/flush-tree (time (reduce msg/insert
                           (core/b-tree (core/->Config 17 300 (- 300 17)))
                           (range 10000000))) 
                   (->RedisBackend)
                   ))
  )
