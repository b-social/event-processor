(ns event-processor.processor.system
  (:require
    [clojure.string :as str]
    [com.stuartsierra.component :as component]
    [configurati.core :as conf]
    [event-processor.processor.component :as two-stage]
    [event-processor.processor.configuration :as processor-configuration]
    [event-processor.utils.logging :as log]))

(defn- ->keyword
  [parts]
  (keyword (str/join parts)))

(defn new-system
  "Creates a new event processor.

   Does nothing if processing is not enabled.

   * Processor identifier can be specified (defaults to :main).
   * Configuration prefix can be specified (defaults to :service).

   All system map keys can be overridden or they default where applicable:

   * database: database (mandatory)
   * event-handler: {processor-identifier}-event-handler (mandatory)
   * processing-enabled: {processor-identifier}-processing-enabled?
   * processor-configuration: {processor-identifier}-processor-configuration
   * processor: {processor-identifier}-processor

   Optional provide a map of system keys that are used as additional dependencies to the component

   e.g.

   ````
   (processors/new-system
     configuration-overrides
     {:processor-identifier    :event-processor
      :database                :database
      :event-handler           :event-handler
      :additional-dependencies {:atom :atom}})
   ````

   Nothing is done with the event if an event-handler is not defined.

   ````
  (deftype AtomEventHandler
    [atom]
    EventHandler

    (get-unprocessed-events [_ {:keys [database] :as processor}]
      (lookup-unprocessed-events database))
    (group-unprocessed-events-by [_ _ event]
      (get-group-key event))
    (handle-event [_ processor {:keys [topic payload]} _]
      (vent/react-to all {:channel topic :payload payload} processor))
    (on-processing-complete [_ {:keys [database]} {:keys [payload]} _]
      (mark-processed database payload)))
  ````"
  [configuration-overrides
   {:keys [database processor-identifier configuration-prefix additional-dependencies
           processing-enabled
           processor-configuration processor event-handler]
    :or {database :database
         processor-identifier :event-processor
         configuration-prefix :service
         additional-dependencies {}}}]
  (let [processor-name
        (name processor-identifier)
        event-handler
        (or event-handler (->keyword [processor-name "-event-handler"]))
        processing-enabled
        (or processing-enabled (->keyword [processor-name "-processing-enabled?"]))
        processor-configuration
        (or processor-configuration (->keyword [processor-name "-processor-configuration"]))
        processor
        (or processor (->keyword [processor-name "-processor"]))
        processing-enabled?
        (get configuration-overrides processing-enabled true)]
    (log/log-info
      {processing-enabled processing-enabled?}
      "Processing enabled?")
    (when processing-enabled?
      (component/system-map
        processor-configuration
        (conf/resolve
          (processor configuration-overrides
            (processor-configuration/processor-configuration
              configuration-prefix
              processor-identifier
              configuration-overrides)))

        processor
        (component/using
          (two-stage/new-processor processor-identifier)
          (merge
            {:configuration processor-configuration
             :database database
             :event-handler event-handler}
            additional-dependencies))))))
