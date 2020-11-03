(ns event-processor.processor.protocols)

(defprotocol EventProcessor
  "A handler that is called at certain points in an events lifecycle.
  The passed processor contains all the configured dependencies."
  :extend-via-metadata true
  (get-unprocessed-events [this processor]
    "A callback to get all un-processed events")
  (group-unprocessed-events-by [this processor event]
    "Callback to get the key for which to process an event")
  (handle-event [this processor event event-context]
    "A callback for processing an event")
  (on-processing-complete [this processor event event-context]
    "A callback for when an event has finished processing"))
