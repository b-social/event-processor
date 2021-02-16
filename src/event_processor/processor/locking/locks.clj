(ns event-processor.processor.locking.locks
  (:require [event-processor.utils.logging :as log]))

(defmacro with-lock
  "Try to acquire a Postgres advisory lock on the database.
  If successful, run `body`, otherwise skip."
  [database lock-id & body]
  `(let [db# (:handle ~database)
         datasource# (:datasource db#)
         release-lock# (fn [connection#]
                         (let [release-lock-statement#
                               (.prepareStatement
                                 connection#
                                 "SELECT pg_advisory_unlock(?)")]
                           (.setInt release-lock-statement# 1 ~lock-id)
                           (.execute release-lock-statement#)
                           (.close connection#)
                           nil))]
     (when-let [connection# (try
                              (.getConnection datasource#)
                              (catch Exception e#
                                (log/log-warn
                                  {:lock-id   ~lock-id
                                   :exception e#}
                                  "Failed to secure the connection object")
                                nil))]
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
             (log/log-debug
               {:lock-id ~lock-id}
               "Lock acquired")
             ~@body
             (log/log-debug
               {:lock-id ~lock-id}
               "Releasing lock")
             (release-lock# connection#)))

         (catch Exception e#
           (log/log-warn
             {:lock-id   ~lock-id
              :exception e#}
             "Failed run the acquire lock method")
           (release-lock# connection#))))))
