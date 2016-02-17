(ns tree.redis
  (:require [clojure.pprint :as pp]
            [taoensso.carmine :as car :refer (wcar)]
            [tree.core :as core]
            [tree.messaging :as msg]))

(defrecord RedisAddr [last-key redis-key]
  core/IResolve
  (dirty? [_] false)
  (last-key [_] last-key)
  (resolve [_] (-> (wcar {} (car/get redis-key))
                   (assoc :storage-addr
                          (doto (promise)
                            (deliver redis-key))))))

(defrecord RedisBackend []
  core/IBackend
  (new-session [_] (atom {:writes 0
                          :deletes 0}))
  (write-node [_ node session]
    (swap! session update-in [:writes] inc)
    (let [key (str (java.util.UUID/randomUUID))]
      (wcar {} (car/set key node))
      (->RedisAddr (core/last-key node) key)))
  (delete-addr [_ addr session]
    (wcar {} (car/del addr))
    (swap! session update-in :deletes inc)))

(comment
  (wcar {} (car/ping) (car/set "foo" "bar") (car/get "foo")))

(println "cleared"
         (wcar {} (apply car/del
                         (wcar {} (car/keys "*")))))

(comment
  (def t 
    (core/flush-tree
      (apply core/b-tree (core/->Config 5 3) (range 100)) (->RedisBackend)))

  (clojure.pprint/pprint t)
  (msg/lookup-fwd-iter (:tree t) -10)

(require '[criterium.core :refer (quick-bench)])
  (quick-bench (apply core/b-tree (core/->Config 70 80 10) (repeatedly 1000 rand)))

  )
