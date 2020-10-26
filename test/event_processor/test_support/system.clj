(ns event-processor.test-support.system
  (:require
   [com.stuartsierra.component :as component]

   [event-processor.test-support.database :as db]
   [configurati.core :as conf]
   [event-processor.processor.system :as processors]

   [event-processor.processor.protocols :refer [EventProcessor
                                                get-unprocessed-events
                                                group-unprocessed-events-by
                                                handle-event
                                                on-processing-complete]]))

(defn with-system-lifecycle [system-atom]
  (fn [f]
    (try
      (do
        (reset! system-atom (component/start-system @system-atom))
        (f))
      (finally
        (reset! system-atom (component/stop-system @system-atom))))))

(defn stub-get-unprocessed-events [_ _] [])
(defn stub-group-unprocessed-events-by [_ _ _])
(defn stub-handle-event [_ _ _ _])
(defn stub-on-processing-complete [_ _ _ _])

(deftype AtomEventHandler
         [atom]
  EventProcessor

  (get-unprocessed-events [this processor]
    (stub-get-unprocessed-events this processor))
  (group-unprocessed-events-by [this processor event]
    (stub-group-unprocessed-events-by this processor event))
  (handle-event [this processor event event-context]
    (stub-handle-event this processor event event-context))
  (on-processing-complete [this processor event event-context]
    (stub-on-processing-complete this processor event event-context)))

(defn new-system
  ([] (new-system {}))
  ([configuration-overrides]
   (merge
     (component/system-map
       :database-configuration
       (conf/resolve
         (:database
          configuration-overrides
          (db/database-configuration
            (select-keys configuration-overrides [:database-port]))))
       :database
       (component/using
         (db/new-database)
         {:configuration
          :database-configuration})

       :event-handler
       (AtomEventHandler. (atom []))
       :atom
       (atom [])

       :name (or (:name configuration-overrides) "test-system"))

     (processors/new-system
       configuration-overrides
       {:processor-identifier    :event-processor
        :database                :database
        :event-handler           :event-handler
        :additional-dependencies {:atom :atom}}))))

(defn new-test-system [configuration]
  (new-system
    (merge
      {:main-processing-enabled? true}
      configuration)))
