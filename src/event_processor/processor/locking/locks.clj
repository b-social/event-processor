(ns event-processor.processor.locking.locks
  (:require [hugsql.core :as hugsql]
            [event-processor.utils.logging :as log]))

(declare get-lock release-lock)
(hugsql/def-db-fns "event_processor/processor/locking/locks.sql")

(defmacro with-lock
  "Acquire an advisory lock on the database, and run function, then release the lock.
   Block if lock is taken."
  [database lock-id & body]
  `(try
     (log/log-debug
       {:lock-id ~lock-id}
       "Acquiring lock")
     (try
       (get-lock (:handle ~database) {:lock_id ~lock-id})
       (catch Exception exception#
         (throw (ex-info "Error getting locks"
                  {:lock_id ~lock-id :lock-error true}
                  exception#))))
     ~@body
     (finally
       (do
         (log/log-debug
           {:lock-id ~lock-id}
           "Releasing lock")
         (release-lock (:handle ~database) {:lock_id ~lock-id})))))
