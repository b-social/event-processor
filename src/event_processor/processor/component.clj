(ns event-processor.processor.component
  (:require
    [clojure.java.jdbc :as jdbc]
    [com.climate.claypoole :as cp]
    [com.stuartsierra.component :as component]
    [event-processor.processor.locking.locks :refer [with-lock]]
    [event-processor.processor.protocols
     :refer [on-processing-complete
             get-unprocessed-events group-unprocessed-events-by handle-event]]
    [event-processor.utils.logging :as log]))

(defn- milliseconds [millis] millis)

(defmacro ^:no-doc every [millis & body]
  `(while (not (.isInterrupted (Thread/currentThread)))
     ~@body
     (Thread/sleep ~millis)))

(defn- handle-events-group
  [{:keys [database event-processor event-handler]
    :as processor}
   events]
  (try
    (doseq [event events
            :let [event-context
                  {:event-processor event-processor}]]
      (jdbc/with-db-transaction
        [transaction (:handle database)]
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
        exception))))

(defn- process-events-once
  [{:keys [database
           event-processor
           event-handler
           configuration
           thread-pool]
    :as processor}]
  (with-lock
    database
    (:db-lock-id configuration)
    (log/log-debug
      {:event-processor event-processor}
      "Checking for un-processed event batch.")
    (let [grouped-events (->> (get-unprocessed-events event-handler processor)
                           (group-by (partial group-unprocessed-events-by event-handler processor))
                           (vals))]
      (if thread-pool
        (->> grouped-events
          (cp/pmap thread-pool (partial handle-events-group processor))
          (doall))
        (->> grouped-events
          (map (partial handle-events-group processor))
          (doall))))))

(defn- process-events-forever
  [{:keys [configuration event-processor]
    :as processor}]
  (let [{:keys [interval]} configuration
        processor-as-map (into {} [processor])]
    (log/log-info
      {:event-processor event-processor
       :configuration configuration}
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
           [event-processor
            db-lock-id
            configuration]
  component/Lifecycle
  (start [component]
    (log/log-info {:event-processor event-processor}
      "Starting event processor.")
    (let [{:keys [threads]} configuration
          thread-pool (when (> threads 1)
                        (log/log-info {:threads threads}
                          "Starting thread-pool")
                        (cp/threadpool threads))
          component (assoc component :thread-pool thread-pool)
          processor (future (process-events-forever component))]
      (assoc component :processor processor)))

  (stop [component]
    (when-let [processor (:processor component)]
      (future-cancel processor))
    (when-let [thread-pool (:thread-pool component)]
      (log/log-info {} "Shutdown thread-pool")
      (cp/shutdown thread-pool))
    (dissoc component :processor :thread-pool)))

(defn ^:no-doc new-processor
  [event-processor]
  (map->Processor {:event-processor event-processor}))