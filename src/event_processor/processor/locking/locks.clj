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

             _# (.setInt get-lock-statement# 1 ~lock-id)
             result-set# (.executeQuery get-lock-statement#)

             _# (.first result-set#)
             acquired# (.getBoolean result-set# 1)

             _# (.close result-set#)]

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
