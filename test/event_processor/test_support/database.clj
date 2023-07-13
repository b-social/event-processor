(ns event-processor.test-support.database
  (:require
    [com.stuartsierra.component :as component]
    [configurati.core
     :refer [define-configuration
             with-specification
             with-source
             with-key-fn
             map-source
             with-parameter
             define-configuration-specification]]
    [configurati.key-fns :refer [remove-prefix]])
  (:import [com.zaxxer.hikari HikariConfig HikariDataSource]
    [java.io Closeable]))

(def database-configuration-specification
  (define-configuration-specification
    (with-key-fn (remove-prefix :database))
    (with-parameter :database-host)
    (with-parameter :database-port :type :integer)
    (with-parameter :database-name)
    (with-parameter :database-user)
    (with-parameter :database-password)))

(defn database-configuration
  [overrides]
  (define-configuration
    (with-specification database-configuration-specification)
    (with-source
      (map-source
        (merge
          {:database-host "localhost"
           :database-name "postgres"
           :database-user "postgres"
           :database-password "postgres"}
          overrides)))))

(defn datasource-for [{:keys [user password host port name]}]
  (let [hikari-datasource-config
        (doto (HikariConfig.)
          (.setUsername user)
          (.setPassword password)
          (.setJdbcUrl (format "jdbc:postgresql://%s:%s/%s"
                         host port name)))

        hikari-datasource
        (HikariDataSource. hikari-datasource-config)]
    hikari-datasource))

(defrecord Database
           [configuration handle]
  component/Lifecycle

  (start [component]
    (assoc component :handle {:datasource (datasource-for configuration)}))

  (stop [component]
    (let [^Closeable datasource (get-in component [:handle :datasource])]
      (when datasource
        (.close datasource))
      (assoc component :handle nil))))

(defn new-database []
  (map->Database {}))