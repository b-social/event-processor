(ns event-processor.processor.locking.locks
  (:require [hugsql.core :as hugsql]))

(declare get-lock release-lock)
(hugsql/def-db-fns "event_processor/processor/locking/locks.sql")

(defmacro with-lock
  "Acquire an advisory lock on the database, and run function, then release the lock.
   Block if lock is taken."
  [database & body]
  `(try
        (get-lock (:handle ~database))
        ~@body
        (finally (release-lock (:handle ~database)))))
