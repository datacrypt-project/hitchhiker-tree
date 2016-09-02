(ns hitchhiker.s3
  (:require [amazonica.aws.s3 :as s3]
            [clojure.string :refer [split]]
            [hitchhiker.tree.core :as core]
            [taoensso.nippy :as nippy])
  (:import [com.google.common.io ByteStreams]
           [java.util UUID]
           [java.io ByteArrayInputStream]))

;;; Main node data stored in uuid key
;;;   bucket-name/8E3806E4-865D-43C8-A823-9CCF7D9D88CB
;;; References from node -> node uses sub object:
;;;   bucket-name/F9FD37E5-5BF3-4681-93A0-A56E9823A068/->2793532B-8FC9-4E0D-8CD4-19AE87F3133E
;;; References to node uses sub object:
;;;   bucket-name/0834FB96-CC7A-4531-8A8E-B9FB811F5D5B/<-A38D2654-EDF2-460A-9D88-388DEC77AA05

(defrecord S3Addr [last-key bucket key storage-addr]
  core/IResolve
  (dirty? [_] false)
  (last-key [_] last-key)
  (resolve [_]
    (with-open [in (:input-stream (s3/get-object bucket key))]
      (nippy/thaw (ByteStreams/toByteArray in)))))

(defn synthesize-storage-addr
  [bucket key]
  (doto (promise)
    (deliver {:bucket bucket :key key})))

(defn s3-addr
  [last-key bucket key]
  (->S3Addr last-key bucket key (synthesize-storage-addr bucket key)))

(nippy/extend-thaw :b-tree/s3-addr
                   [data-input]
                   (let [last-key (nippy/thaw-from-in! data-input)
                         bucket (nippy/thaw-from-in! data-input)
                         key (nippy/thaw-from-in! data-input)]
                     (s3-addr last-key bucket key)))

(nippy/extend-freeze S3Addr :b-tree/s3-addr
                     [{:keys [last-key bucket key]} data-output]
                     (nippy/freeze-to-out! data-output last-key)
                     (nippy/freeze-to-out! data-output bucket)
                     (nippy/freeze-to-out! data-output key))

(defn write-object
  [bucket key bytes]
  (println "write-object" bucket key (count bytes))
  (s3/put-object :bucket-name bucket
                 :key key
                 :metadata {:content-length (count bytes)}
                 :input-stream (ByteArrayInputStream. bytes)))

(defn delete-object
  [bucket key]
  (doall (for [other-key (map :key (:object-summaries
                                    (s3/list-objects :bucket-name bucket
                                                     :prefix (str key "/->"))))]
           (s3/delete-object :bucket-name bucket
                             :key (str (last (split other-key "/->")) "/<-" key))
           ; TODO: delete other-key if no refs?
           ))
  (doall (for [other-key (map :key (:object-summaries
                                    (s3/list-objects :bucket-name bucket
                                                     :prefix (str key "/<-"))))]
           (s3/delete-object :bucket-name bucket
                             :key (str (last (split other-key "/<-")) "/->" key))))
  (s3/delete-object :bucket-name bucket :key key))

(defn add-refs
  [node-key child-keys]
  (doall
   (for [{:keys [bucket key]} child-keys]
     (do
       (write-object bucket (str node-key "/->" key) (byte-array 0))
       (write-object bucket (str key "/<-" node-key) (byte-array 0))))))

(defrecord S3Backend [#_service bucket]
  core/IBackend
  (new-session [_]
    (atom {:writes 0 :deletes 0}))
  (anchor-root [_ node]
    ;; maybe we could use versioning and object expiration to
    ;; handle this? For now don't expire anything :-P
    node)
  (write-node [_ node session]
    (swap! session update-in [:writes] inc)
    (let [key (UUID/randomUUID)
          addr (s3-addr (core/last-key node) bucket key)]
      (write-object bucket key (nippy/freeze node))
      (when (core/index-node? node)
        (add-refs key
                  (for [child (:children node)
                        :let [child-key @(:storage-addr child)]]
                    child-key)))
      addr))
  (delete-addr [_ addr session]
    (swap! session update-in [:deletes] inc)
    (delete-object (:bucket addr) (:key addr))))
