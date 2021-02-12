(ns event-processor.processor.locking.locks
  (:require [event-processor.utils.logging :as log]))

(defmacro with-lock
  "Acquire an advisory lock on the database, and run function, then release the lock.
   Block if lock is taken."
  [database lock-id & body]
  `(let [db# (:handle ~database)
         datasource# (:datasource db#)
         connection# (.getConnection datasource#)]
     (try
       (log/log-debug
         {:lock-id ~lock-id}
         "Acquiring lock")
       (let [get-lock-statement#
             (.prepareStatement
               connection#
               "SELECT pg_try_advisory_lock(?)")
             result-set# (doto
                          get-lock-statement#
                          (.setInt 1 ~lock-id)
                          .executeQuery)
             acquired# (doto
                         result-set#
                         .first
                         (.getBoolean 1))]
         (when acquired#
           ~@body))
       (finally
         (do
           (log/log-debug
             {:lock-id ~lock-id}
             "Releasing lock")
           (let [release-lock-statement#
                 (.prepareStatement
                   connection#
                   "SELECT pg_advisory_unlock(?)")]
             (doto
               release-lock-statement#
               (.setInt 1 ~lock-id)
               .execute)))))))
