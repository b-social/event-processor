(ns ^:no-doc event-processor.processor.configuration
  (:require
    [configurati.core
     :refer [define-configuration
             define-configuration-specification
             with-parameter
             with-source
             with-specification
             with-key-fn
             multi-source
             env-source
             map-source]]
    [configurati.key-fns :refer [remove-prefix]]))

(defn processor-configuration-specification [processor-type]
  (let [prefix (str (name processor-type) "-processor")]
    (letfn [(prefixed-parameter [parameter-name]
              (keyword (str prefix "-" (name parameter-name))))]
      (define-configuration-specification
        (with-key-fn (remove-prefix (keyword prefix)))
        (with-parameter (prefixed-parameter :timeout)
          :type :integer :default 5000)
        (with-parameter (prefixed-parameter :interval)
          :type :integer :default 1000)
        (with-parameter (prefixed-parameter :schema-version)
          :type :integer :default 1)
        (with-parameter :db-lock-id
          :type :integer :default 1)
        (with-parameter :threads
          :type :integer :default 1)))))

(defn processor-configuration [configuration-prefix processor-type overrides]
  (define-configuration
    (with-source
      (multi-source
        (map-source overrides)
        (env-source :prefix configuration-prefix)))
    (with-specification
      (processor-configuration-specification processor-type))))
