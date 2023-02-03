(ns event-processor.test-support.postgres.database
  (:import [com.opentable.db.postgres.embedded EmbeddedPostgres]))

(defn with-database
  [database]
  (fn [run-tests]
    (reset! database (.start (EmbeddedPostgres/builder)))
    (try
      (run-tests)
      (finally
        (.close @database)))))
