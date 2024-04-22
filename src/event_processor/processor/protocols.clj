(ns event-processor.processor.protocols
  (:require [com.kroo.service-base.event-processor.protocols :as p]))

;; re-exporting to ease transition, protocol is the same
(def EventProcessor p/EventProcessor)
