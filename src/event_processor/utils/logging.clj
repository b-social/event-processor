(ns event-processor.utils.logging
  (:require
    [cambium.core :as log])
  (:import (java.time Instant)))

(defn ^:no-doc get-error-context [context ^Throwable exception]
  (let [exception-class-name (.getCanonicalName (class exception))
        exception-stacktrace (map str (.getStackTrace exception))
        exception-description (str exception)]
    (merge
      {:exception-type exception-class-name
       :exception-message (ex-message exception)
       :exception-stacktrace exception-stacktrace
       :exception-description exception-description
       :exception exception}
      context)))

(defmacro log-debug
  [context formatted-string]
  `(log/log :debug ~context nil ~formatted-string))

(defmacro log-info [context formatted-string]
  `(log/log :info ~context nil ~formatted-string))

(defmacro log-warn [context formatted-string]
  `(log/log :warn ~context nil ~formatted-string))

(defmacro log-error
  ([context formatted-string]
    `(log/log :error ~context (:exception ~context) ~formatted-string))
  ([context formatted-string exception]
    `(log-error
       (get-error-context ~context ~exception)
       ~formatted-string)))

(defmacro with-timings
  [block-name ctx & body]
  `(let [start-time# (.toEpochMilli (Instant/now))
         ret# ~@body
         end-time# (.toEpochMilli (Instant/now))]
     (log-debug
       {:ctx ~ctx
        :start-time start-time#
        :end-time end-time#
        :elapsed (- end-time# start-time#)}
       (format "Ran %s" ~block-name))
     ret#))
