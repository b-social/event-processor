(ns event-processor.processor.locking.locks
  (:require [event-processor.utils.logging :as log]))

(defn acquire-lock
  [connection lock-id]
  (log/with-timings "Acquiring Lock" {}
                    (let [get-lock-statement#
                          (.prepareStatement
                            connection
                            "SELECT pg_try_advisory_lock(?)")

                          _# (.setInt get-lock-statement# 1 lock-id)
                          result-set# (.executeQuery get-lock-statement#)

                          _# (.first result-set#)
                          acquired (.getBoolean result-set# 1)

                          _# (.close result-set#)]
                      acquired)))

(defn release-lock
  [connection lock-id]
  (log/with-timings "Releasing Lock" {}
                    (let [release-lock-statement#
                          (.prepareStatement
                            connection
                            "SELECT pg_advisory_unlock(?)")]
                      (.setInt release-lock-statement# 1 lock-id)
                      (.execute release-lock-statement#)
                      nil)))

(defn get-connection
  [datasource lock-id]
  (log/with-timings "Getting Connection" {}
                    (try
                      (.getConnection datasource)
                      (catch Exception e
                        (log/log-warn
                          {:lock-id     lock-id
                           :exception   e
                           :stack-trace (.getStackTrace e)}
                          "Failed to secure the connection object")
                        nil))))

(defmacro with-lock
  "Try to acquire a Postgres advisory lock on the database.
  If successful, run `body`, otherwise skip."
  [database lock-id & body]
  `(let [db# (:handle ~database)
         datasource# (:datasource db#)]
     (when-let [connection# (get-connection datasource# ~lock-id)]
       (try
         (log/log-debug
           {:lock-id ~lock-id}
           "Acquiring lock")
         (when-let [acquired# (acquire-lock connection# ~lock-id)]
           (log/log-debug
             {:lock-id ~lock-id}
             "Lock acquired")
           (try
             ~@body
             (finally
               (log/log-debug
                 {:lock-id ~lock-id}
                 "Releasing lock")
               (release-lock connection# ~lock-id))))
         (catch Exception e#
           (log/log-warn
             {:lock-id   ~lock-id
              :exception e#}
             "Failed run the acquire lock method"))
         (finally
           (.close connection#))))))
