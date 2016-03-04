(ns hitchhiker.outboard
  (:require [hitchhiker.redis :as redis]
            [hitchhiker.tree.core :as core]
            [hitchhiker.tree.messaging :as msg]))

;;; API
;;; insert -- Inserts key/value pairs into the outboard data storage
;;; delete -- Deletes keys from the outboard data storage
;;; lookup -- Returns the value for the given key, or not-found which defaults to nil
;;; save-as -- Saves the outboard under the given string name.
;;; load -- Loads an outboard from the given string name
;;;
;;; In this API, we are doing operations inline with the main code path
;;; This means it's impossible to guarantee data is flushed every N seconds
;;; And it reduces the opportunities we have to optimize flushing
;;;
;;;
;;; or maybe
;;;
;;; transact! conn txns -- Applies the given transactions to the structure
;;; snapshot conn -- returns an immutable snapshot of the structure
;;; extend-lifetime snapshot until-when -- ensures the snapshot will not be GCed until the specified time (default to 5s)
;;; lookup/lookup-fwd-iter snapshot -- operates on the snapshot
;;; speculate snapshot txns -- returns a new snapshot with the txns applied to it
;;; save-as snapshot new-name -- saves the given snapshot to the given name
;;; create new-name -- creates a new empty structure at the given name
;;; open name -- returns a connection to the named structure
;;;
;;; This API makes it easy to optimize the IO due to flushing, since
;;; we have complete control over when it happens.
;;; It is similar to the Datomic API, but it remains to decide what transactions look like

(defrecord Outboard [tree ops-since-flush])

(defonce ^:private refcount-expiry-thread (redis/start-expiry-thread!))

(defn- maybe-flush
  "Flushes the given outboard if it's been enough ops"
  [outboard]
  (if (> (:ops-since-flush outboard) 1000)
    (-> outboard
        (assoc :ops-since-flush 0)
        (update-in [:tree] core/flush-tree (redis/->RedisBackend)))
    outboard))

;;TODO maybe I could just make an "entry" object, and then grab ptrs
;;into the tree w/ lookup-fwd-iter to find the associated values :)

(defn insert
  "Inserts key/value pairs into the outboard data storage"
  [outboard k v & kvs]
  (let [tree (:tree outboard)
        tree' (if (and (seq kvs) (even? (count kvs)))
                (loop [tree (msg/insert tree k v)
                       [k v & kvs] kvs]
                  (if k
                    (msg/insert tree k v)
                    tree))
                (msg/insert tree k v))
        num-ops (inc (count ks))]
    (-> outboard
        (assoc :tree tree')
        (update-in [:ops-since-flush] + num-ops)))) 

(defn delete
  "Deletes keys from the outboard data storage"
  [outboard k & ks]
  (let [tree (:tree outboard)
        tree' (if (seq ks)
                (reduce msg/delete tree (cons k ks))
                (msg/delete tree k))
        num-ops (inc (count ks))]
    (-> outboard
        (assoc :tree tree')
        (update-in [:ops-since-flush] + num-ops))))

(defn lookup
  "Returns the value for the given key, or not-found which defaults to nil"
  ([outboard k]
   (msg/lookup (:tree outboard) k))
  ([outboard k not-found]
   (or (msg/lookup (:tree outboard) k) not-found)))

(defn create
  []
  "Returns a new, empty outboard"
  (->Outboard (core/b-tree (core/->Config 30 600 870)) 0))

(defn save-as
  "Saves the outboard under the given string name."
  [outboard name]
  )

(defn load-from
  "Loads an outboard from the given string name"
  [name]
  )
