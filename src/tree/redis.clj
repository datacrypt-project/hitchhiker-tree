(ns tree.redis
  (:require [clojure.pprint :as pp]
            [taoensso.carmine :as car :refer (wcar)]
            [taoensso.nippy :as nippy]
            [tree.core :as core]
            [clojure.string :as str]
            [clojure.core.memoize :as memo]
            [clojure.core.cache :as cache]
            [tree.messaging :as msg]))

(defn add-refs
  [node-key children-keys]
  ;(println "Adding refs" node-key children-keys)
  (apply car/eval*
         (str/join
           \newline
           ["local parent = ARGV[1]"
            "for i=2,#ARGV do"
            "  local child = ARGV[i]"
            "  redis.call('incr', child .. ':rc')"
            "  redis.call('rpush', parent .. ':rl', child)"
            "end"])
         0
         node-key
         children-keys))

(defn drop-ref
  [key]
  (car/lua (str/join \newline
                     ["local to_delete = { _:my-key }"
                      "while next(to_delete) ~= nil do"
                      "  local cur = table.remove(to_delete)"
                      "  if redis.call('decr', cur .. ':rc') <= 0 then"
                      "    local to_follow = redis.call('lrange', cur .. ':rl', 0, -1)"
                      "    for i = 1,#to_follow do"
                      "      table.insert(to_delete, to_follow[i])"
                      "    end"
                      "    redis.call('del', cur .. ':rl')"
                      "    redis.call('del', cur .. ':rc')"
                      "    redis.call('del', cur)"
                      "  end"
                      "end"
                      ])
           {} {:my-key key}))

(let [cache (-> {}
                (cache/lru-cache-factory :threshold 10000)
                atom)]
  (defn totally-fetch
    [redis-key]
    (let [run (delay
                (loop [i 0]
                  (if (= i 1000)
                    (do (println "total fail") (System/exit 1))
                    (let [x (wcar {} (car/get redis-key))]
                      (if x
                        x
                        (do (Thread/sleep 25) (recur (inc i))))))))
          cs (swap! cache (fn [c]
                            (if (cache/has? c redis-key)
                              (cache/hit c redis-key)
                              (cache/miss c redis-key run))))
          val (cache/lookup cs redis-key)]
      (if val @val @run)))

  (defn seed-cache!
    [redis-key val]
    (swap! cache cache/miss redis-key val)))

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

(defn synthesize-storage-addr
  "Given a key, returns a promise containing that key for use as a storage-addr"
  [key]
  (doto (promise)
    (deliver key)))

(defrecord RedisAddr [last-key redis-key storage-addr]
  core/IResolve
  (dirty? [_] false)
  (last-key [_] last-key)
  (resolve [_] (let [x (-> (totally-fetch redis-key)
                   (assoc :storage-addr (synthesize-storage-addr redis-key)))]
                 ;(println "Deser:" x)
                 (when (and (core/index-node? x)
                            (some #(not (satisfies? msg/IOperation %)) (:op-buf x)))
                   (println (str "Found a broken node, has " (count (:op-buf x)) " ops"))
                   (println (str "The node data is " x))
                   (println "the node's class is" (class x))
                   (println "and it has keys" (:keys x))
                   (println "And is it an index-node?" (core/index-node? x))
                   (println "It came from" redis-key)
                   (println (str "and " (:op-buf x))))
                 x
                 )))

(comment
  (:cfg (wcar {} (car/get "b89bb965-e584-45a2-9232-5b76bf47a21c")))
  (update-in {:op-buf [1 2 3]} [:op-buf] into [4 5 6])
  )

(defn redis-addr
  [last-key redis-key]
  (->RedisAddr last-key redis-key (synthesize-storage-addr redis-key)))

(nippy/extend-freeze RedisAddr :b-tree/redis-addr
                     [{:keys [last-key redis-key]} data-output]
                     (nippy/freeze-to-out! data-output last-key)
                     (nippy/freeze-to-out! data-output redis-key))

(nippy/extend-thaw :b-tree/redis-addr
                   [data-input]
                   (let [last-key (nippy/thaw-from-in! data-input)
                         redis-key (nippy/thaw-from-in! data-input)]
                     (redis-addr last-key redis-key)))


(defrecord RedisBackend [#_service]
  core/IBackend
  (new-session [_] (atom {:writes 0
                          :deletes 0}))
  (write-node [_ node session]
    (swap! session update-in [:writes] inc)
    (let [key (str (java.util.UUID/randomUUID))
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
      (seed-cache! key node)
      addr))
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
      (->RedisAddr last-key root-key (synthesize-storage-addr root-key)))))

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

(comment

(do
  (wcar {}
      (doseq [k ["foo" "bar" "baz" "quux"]
              e ["" ":rc" ":rs" ":rl"]]
        (car/del (str k e))))

  (do (wcar {} (car/set "foo" 22))
      (wcar {} (car/set "foo:rc" 1))
      (wcar {} (car/set "bar" 33))
      (wcar {} (car/set "baz" "onehundred"))
      (wcar {} (car/set "quux" "teply"))
      (wcar {} (add-refs "baz" ["quux"]))
      (wcar {} (add-refs "foo" ["bar" "baz"])))
  (wcar {} (drop-ref "foo")))
  (doseq [k ["foo" "bar" "baz" "quux"]
          e ["" ":rc" ":rs" ":rl"]]
    (println (str k e) "=" (wcar {} ((if (= e ":rl")
                                       #(car/lrange % 0 -1)
                                       car/get) (str k e))))) 
  (wcar {} (drop-ref "foo"))

  (wcar {} (create-refcounted "foo" 22))

  (wcar {} (car/flushall))
  (count (wcar {} (car/keys "*")))    
  (count (msg/lookup-fwd-iter (create-tree-from-root-key @(:storage-addr (:tree my-tree))) -1))
  (count (msg/lookup-fwd-iter (create-tree-from-root-key @(:storage-addr (:tree my-tree-updated))) -1))
  (def my-tree (core/flush-tree
                 (time (reduce msg/insert
                               (core/b-tree (core/->Config 17 300 (- 300 17)))
                               (range 50000))) 
                 (->RedisBackend)
                 ))
  (def my-tree-updated (core/flush-tree
                         (msg/delete (:tree my-tree) 10)
                         (->RedisBackend)
                         ))
  (wcar {} (car/get (str @(:storage-addr (:tree my-tree)))))
  (wcar {} (car/get (str @(:storage-addr (:tree my-tree-updated)))))
  (wcar {} (car/set "foo" 10))
  (wcar {} (car/get "foo"))
  (wcar {} (drop-ref "foo"))
  (wcar {} (drop-ref @(:storage-addr (:tree my-tree))))
  (wcar {} (drop-ref @(:storage-addr (:tree my-tree-updated))))
  )
