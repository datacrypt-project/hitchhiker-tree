(ns hitchhiker.jdbc
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [hitchhiker.tree.core :as core]
            [hitchhiker.tree.messaging :as msg]
            [clojure.core.cache :as cache]
            [taoensso.nippy :as nippy])
  (:import [java.sql SQLException]))

;;; References in a Relational DB
;;;
;;; The SQLite backend uses a simple relational model to keep track of
;;; keys and their references. Each key is listed in hh_keys, and whenever
;;; we'd like to have some key point to another, we call add-refs with the
;;; "pointer" key and a list of pointee keys. For each pointee, add-refs will
;;; add a `(pointer, pointee)` tuple in hh_refs.
;;;
;;; hh_keys
;;;  k the name of the key
;;;  v a binary blob representing the value of `k`
;;;
;;; hh_refs
;;;  pointer the name of the pointer key
;;;  pointee the name of the pointee key
;;;
;;; To delete a key, use `drop-key` which also takes care of deleting any
;;; keys that are only hanging around because they point to the key being
;;; deleted.
;;;

(defn underscore [x]
  (str/replace (str x) "-" "_"))

(def schema
  {:hh-key (jdbc/create-table-ddl :hh-key
             [[:k :string "primary key"]
              [:v :blob]]
             {:entities underscore})

   :hh-ref (jdbc/create-table-ddl :hh-ref
             [[:pointer :string "references hh_key(k) on delete cascade"]
              [:pointee :string "references hh_key(k) on delete cascade"]]
             {:entities underscore})

   :hh-ref-by-pointer "create index if not exists hh_ref_by_pointer on hh_ref (pointer);"
   :hh-ref-by-pointee "create index if not exists hh_ref_by_pointee on hh_ref (pointee);"})

(def query
  {:table-exists? "select 1 from sqlite_master where type='table' and name=?"
   :index-exists? "select 1 from sqlite_master where type='index' and name=?"
   :find-key      "select * from hh_key where k=?"
   :dead-keys     "select k
                     from hh_key
                    where k not in ( select pointee from hh_ref )"})

(defn drop-key [db key]
  (jdbc/delete! db :hh-key ["k = ?" key]
                {:entities underscore})

  (let [dead-keys (jdbc/query db (query :dead-keys))]
    (doseq [{:keys [k] :as dead-key} dead-keys]
      (drop-key db k))))


(defn db-spec [subname]
  {:classname   "org.sqlite.JDBC"
   :subprotocol "sqlite"
   :subname     subname})

(defn table-exists? [db table]
  (let [qry (fn [args]
              (jdbc/query db args))]
    (-> [(query :table-exists?) (underscore table)]
        qry
        (not-empty))))

(defn index-exists? [db idx]
  (let [qry (fn [args]
              (jdbc/query db args))]
    (-> [(query :index-exists?) (underscore idx)]
        qry
        (not-empty))))

(defn create-table [db tbl]
  (try
    (jdbc/execute! db tbl)
    (catch SQLException e
      (when-not (re-matches #"table (.*) already exists" (.getMessage e))
        (throw (ex-info "failed to create table"
                        {:ddl tbl}
                        e))))))

(defn create-index [db idx]
  (try
    (jdbc/execute! db idx)
    (catch SQLException e
      (when-not (re-matches #"index (.*) already exists" (.getMessage e))
        (throw (ex-info "failed to create index"
                        {:idx idx}
                        e))))))

(defn ensure-schema [db]
  (let [ensure (fn [{:keys [items exists? create!]}]
                 (doseq [item items]
                   (when-not (exists? db (underscore (name item)))
                     (create! db (or (get schema item)
                                     (throw (ex-info "tried to create unknown item"
                                                     {:item item})))))))]

    (jdbc/execute! db "pragma foreign_keys=on;")

    (ensure {:items [:hh-key :hh-ref]
             :exists? table-exists?
             :create! create-table})

    (ensure {:items [:hh-ref-by-pointer :hh-ref-by-pointee]
             :exists? index-exists?
             :create! create-index})

    db))

(defonce ^:private db-registry (atom {}))

(defn find-db [subname]
  (get @db-registry subname))

(defn create-db [subname]
  (-> {:connection (jdbc/get-connection (db-spec subname))}
      (ensure-schema)))

(defn find-or-create-db [subname]
  (or (find-db subname)
      (create-db subname)))

(defn add-node [db {:keys [k v] :as node}]
  (try
    (jdbc/insert! db :hh-key {:k k :v (nippy/freeze v)} {:entities underscore})
    (catch SQLException e
      (throw (ex-info "failed to add node"
                      {:node node
                       :db db} e)))))

(defn list-keys [db]
  (jdbc/query db "select k from hh_key"))

(defn delete-key [db k]
  (jdbc/delete! :hh-key ["k = ?" k]))

(defn add-refs [db {:keys [pointer pointees]}]
  (let [mk-ref (fn [pointee]
                 [pointer pointee])]
    (try
      (jdbc/insert-multi! db :hh-ref (for [pointee pointees]
                                       {:pointer pointer
                                        :pointee pointee})
                          {:entities underscore})
      (catch Exception e
        (throw (ex-info "Failed to link pointer with pointees"
                        {:pointer pointer
                         :pointee pointees}
                        e))))))

(defn synthesize-storage-addr
  "Given a key, returns a promise containing that key for use as a storage-addr"
  [key]
  (doto (promise)
    (deliver key)))

;;; TODO: I believe using a dynamic var to hold the DB is a bit of anti-pattern but
;;;       not sure how to avoid it and still support caching as it behaves in the
;;;       redis backend.
;;;
(def ^:dynamic *db*)

(let [cache (-> {}
                (cache/lru-cache-factory :threshold 10000)
                atom)]
  (defn seed-cache! [sqlite-key val]
    (swap! cache cache/miss sqlite-key val))

  (defn io-fetch [sqlite-key]
    (let [run (delay
               (-> (jdbc/query *db* [(query :find-key) sqlite-key])
                   :v
                   nippy/thaw))
          cs (swap! cache (fn [c]
                            (if (cache/has? c sqlite-key)
                              (cache/hit c sqlite-key)
                              (cache/miss c sqlite-key run))))
          val (cache/lookup cs sqlite-key)]
      (if val @val @run))))

(defrecord SQLiteAddr [last-key sqlite-key storage-addr]
  core/IResolve
  (dirty? [_] false)
  (last-key [_] last-key)
  (resolve [_] (-> (io-fetch sqlite-key)
                   (assoc :storage-addr (synthesize-storage-addr sqlite-key)))))

(defn sqlite-addr
  [last-key sqlite-key]
  (->SQLiteAddr last-key sqlite-key
                (synthesize-storage-addr sqlite-key)))

(nippy/extend-thaw :b-tree/sqlite-addr
  [data-input]
  (let [last-key (nippy/thaw-from-in! data-input)
        sqlite-key (nippy/thaw-from-in! data-input)]
    (sqlite-addr last-key sqlite-key)))

(nippy/extend-freeze SQLiteAddr :b-tree/sqlite-addr
  [{:keys [last-key sqlite-key]} data-output]
  (nippy/freeze-to-out! data-output last-key)
  (nippy/freeze-to-out! data-output sqlite-key))

(nippy/extend-thaw :b-tree/sqlite-addr
  [data-input]
  (let [last-key (nippy/thaw-from-in! data-input)
        redis-key (nippy/thaw-from-in! data-input)]
    (sqlite-addr last-key redis-key)))

(defrecord JDBCBackend [db]
  core/IBackend
  (new-session [_] (atom {:writes 0
                          :deletes 0}))
  (anchor-root [_ {:keys [sqlite-key] :as node}]
    node)
  (write-node [_ node session]
    (swap! session update-in [:writes] inc)
    (let [key (str (java.util.UUID/randomUUID))
          addr (sqlite-addr (core/last-key node) key)]

      (when (some #(not (satisfies? msg/IOperation %)) (:op-buf node))
        (println (str "Found a broken node, has " (count (:op-buf node)) " ops"))
        (println (str "The node data is " node))
        (println (str "and " (:op-buf node))))

      (jdbc/with-db-transaction [tx db]
        (binding [*db* tx]
          (add-node db {:k key, :v node})
          (when (core/index-node? node)
            (add-refs db {:pointer key
                          :pointees (for [pointee (:pointees node)
                                          :let [pointee-key @(:storage-addr pointee)]]
                                      pointee-key)}))))

      (seed-cache! key (doto (promise)
                         (deliver node)))
      addr))
  (delete-addr [_ addr session]
    (delete-key db addr)
    (swap! session update-in :deletes inc)))

(defn get-root-key
  [tree]
  (-> tree :storage-addr (deref 10 nil)))

(defn create-tree-from-root-key
  [db root-key]
  (let [last-key (core/last-key
                  (-> (jdbc/find-by-keys db :hh-key {:k root-key}
                                         {:entities underscore})
                      first
                      :v
                      nippy/thaw))]
    (core/resolve
     (->SQLiteAddr last-key root-key (synthesize-storage-addr root-key)))))


(comment

  (defn insert [t v]
    (msg/insert t v v))

  (jdbc/with-db-connection [conn (assoc db :subname "yolo.sqlite") ]
    (setup conn)

    (def my-tree
      (let [b-tree (core/b-tree (core/->Config 17 300 (- 300 17)))]
        (core/flush-tree
         (reduce insert b-tree (range 50000))
         (->SQLiteBackend conn)))))


  (jdbc/with-db-connection [db (db-spec "yolo.sqlite")]
    (-> (jdbc/find-by-keys db :hh-key {:k "c6afddfe-f641-49f9-8789-4493ffa41c1c"}
                           {:entities underscore})
        first
        :v))


  (jdbc/with-db-connection [conn (assoc db :subname "yolo.sqlite")]
    (-> (create-tree-from-root-key conn @(:storage-addr (:tree my-tree)))
        (msg/lookup-fwd-iter 1)
        (count)))

  )
