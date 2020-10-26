(ns event-processor.processor.component
  (:require [com.stuartsierra.component :as component]
            [event-processor.utils.logging :as log]
            [clojure.java.jdbc :as jdbc]
            [event-processor.processor.protocols
             :refer [on-processing-complete
                     get-unprocessed-events group-unprocessed-events-by handle-event]]
            [event-processor.processor.locking.locks :refer [with-lock get-lock release-lock]]))

(defn- milliseconds [millis] millis)

(defmacro ^:no-doc every [millis & body]
  `(while (not (.isInterrupted (Thread/currentThread)))
     ~@body
     (Thread/sleep ~millis)))

(defn- process-events-once
  [{:keys [database event-processor event-handler configuration]
    :as   processor}]
  (with-lock database
    (:db-lock-id configuration)
    (log/log-debug
      {:event-processor event-processor}
      "Checking for un-processed event batch.")
    (let [all-events (get-unprocessed-events event-handler processor)
          events-per (group-by
                       #(group-unprocessed-events-by event-handler processor %)
                       all-events)]
      (doseq [events (vals events-per)]
        (try
          (doseq [event events
                  :let [event-context
                        {:event-processor event-processor}]]
            (jdbc/with-db-transaction [transaction (:handle database)]
              (let [database (assoc database
                               :handle
                               transaction)
                    processor (assoc processor
                                :database
                                database)]
                (handle-event event-handler
                  processor
                  event
                  event-context)
                (on-processing-complete event-handler
                  processor
                  event
                  event-context))))
          (catch Throwable exception
            (log/log-error
              {:event-processor event-processor}
              "Something went wrong in event processor."
              exception)))))))

(defn- process-events-forever
  [{:keys [configuration event-processor]
    :as   processor}]
  (let [{:keys [interval]} configuration
        processor-as-map (into {} [processor])]
    (log/log-info
      {:event-processor event-processor
       :configuration   configuration}
      "Initialising event processor.")
    (every
      (milliseconds interval)
      (try
        (process-events-once processor-as-map)
        (catch Throwable exception
          (if (-> exception ex-data :lock-error)
            (throw exception)
            (log/log-error
              {:event-processor event-processor}
              "Something went wrong finding unprocessed events."
              exception)))))))

(defrecord Processor
           [event-processor db-lock-id]
  component/Lifecycle
  (start [component]
    (log/log-info {:event-processor event-processor}
      "Starting event processor.")
    (let [processor (future (process-events-forever component))]
      (assoc component :processor processor)))

  (stop [component]
    (when-let [processor (:processor component)]
      (future-cancel processor))
    (dissoc component :processor)))

(defn ^:no-doc new-processor
  [event-processor]
  (map->Processor {:event-processor event-processor}))