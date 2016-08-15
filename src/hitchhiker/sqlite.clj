(ns hitchhiker.sqlite
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [hitchhiker.tree.core :as core]
            [hitchhiker.tree.messaging :as msg]
            [clojure.core.cache :as cache]
            [taoensso.nippy :as nippy])
  (:import [java.sql SQLException]))

(defn underscore [x]
  (str/replace (str x) "-" "_"))

(def schema
  {:hh-key (jdbc/create-table-ddl :hh-key
             [[:k :string "primary key"]
              [:v :blob]]
             {:entities underscore})

   :hh-ref (jdbc/create-table-ddl :hh-ref
             [[:parent :string "references hh_key(k) on delete cascade"]
              [:child :string "references hh_key(k) on delete cascade"]]
             {:entities underscore})

   :hh-ref-by-parent "create index if not exists hh_ref_by_parent on hh_ref (parent);"
   :hh-ref-by-child "create index if not exists hh_ref_by_child on hh_ref (child);"})

(def query
  {:table-exists? "select 1 from sqlite_master where type='table' and name=?"
   :index-exists? "select 1 from sqlite_master where type='index' and name=?"
   :find-key      "select * from hh_key where k=?"})

(defn drop-ref [db key]
  (jdbc/delete! db :hh-key ["k = ?" key]))

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

    (ensure {:items [:hh-ref-by-parent :hh-ref-by-child]
             :exists? index-exists?
             :create! create-index})))

(defn add-node [db {:keys [k v] :as node}]
  (try
    (jdbc/insert! db :hh-key {:k k :v (nippy/freeze v)} {:entities underscore})
    (catch SQLException e
      (throw (ex-info "failed to add node"
                      {:node node
                       :db db} e)))))

(defn delete-key [db k]
  (jdbc/delete! :hh-key ["k = ?" k]))

(defn add-refs [db {:keys [parent children]}]
  (let [mk-ref (fn [child]
                 [parent child])]
    (jdbc/insert-multi! db :hh-ref (for [child children]
                                     {:parent parent
                                      :child child})
                        {:entities underscore})))

(defn synthesize-storage-addr
  "Given a key, returns a promise containing that key for use as a storage-addr"
  [key]
  (doto (promise)
    (deliver key)))

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

(defrecord SQLiteBackend [db]
  core/IBackend
  (new-session [_] (atom {:writes 0
                          :deletes 0}))
  (anchor-root [_ {:keys [sqlite-key] :as node}]
    ;; TODO: figure out how redis gc relates to SQL
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
            (add-refs db {:parent key
                          :children (for [child (:children node)
                                          :let [child-key @(:storage-addr child)]]
                                      child-key)}))))

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
