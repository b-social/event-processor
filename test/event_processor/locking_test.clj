(ns event-processor.locking-test
  (:require
    [clojure.test :refer [use-fixtures deftest testing is]]
    [com.stuartsierra.component :as component]
    [event-processor.test-support.system :refer
     [new-test-system with-system-lifecycle stub-get-unprocessed-events
      stub-group-unprocessed-events-by stub-handle-event
      stub-on-processing-complete]]
    [event-processor.test-support.postgres.database :as database]
    [event-processor.test-support.conditional-execution :refer [do-until]]
    [spy.core :as spy]
    [clojure.set :refer [union]])
  (:import (com.opentable.db.postgres.embedded EmbeddedPostgres)))

(defn unique-entries [& entries-collections]
  (apply union (map set entries-collections)))

(def get-event-handler first)

(let [database (atom nil)
      test-system-1 (atom nil)
      test-system-2 (atom nil)]
  (use-fixtures :each
    (database/with-database database)
    (with-system-lifecycle test-system-1 database)
    (with-system-lifecycle test-system-2 database))

  (deftest multiple-systems-should-not-process-concurrently
    (let [event {:message   "event"
                 :group-key "group-key"}
          unprocessed-events (atom [event])

          remove-unprocessed-event (fn [completed]
                                     (reset! unprocessed-events
                                       (remove #(= % completed) @unprocessed-events)))

          mock-get-unprocessed-events (spy/mock (fn [_ _]
                                                  @unprocessed-events))
          mock-group-unprocessed-events-by (spy/mock (fn [_ _ event] (:group-key event)))
          mock-handle-event (spy/spy)
          mock-on-processing-complete
          (spy/mock (fn [_ _ event _]
                      (do (remove-unprocessed-event event)
                          (Thread/sleep 10000000000))))]

      (with-redefs [stub-get-unprocessed-events mock-get-unprocessed-events
                    stub-group-unprocessed-events-by mock-group-unprocessed-events-by
                    stub-handle-event mock-handle-event
                    stub-on-processing-complete mock-on-processing-complete]

        (Thread/sleep 2000)

        (testing "event processing only called from one event handler"
          (is (= 1 (count (unique-entries
                            (map get-event-handler
                              (spy/calls stub-get-unprocessed-events))
                            (map get-event-handler
                              (spy/calls mock-group-unprocessed-events-by))
                            (map get-event-handler
                              (spy/calls mock-handle-event))
                            (map get-event-handler
                              (spy/calls mock-on-processing-complete))))))))))

  (deftest second-system-should-run-processor-if-first-errors
    (let [event {:message   "event"
                 :group-key "group-key"}
          unprocessed-events (atom [event])

          remove-unprocessed-event (fn [completed] (reset! unprocessed-events
                                                     (remove #(= % completed) @unprocessed-events)))

          captured-handler (atom nil)

          mock-get-unprocessed-events (spy/mock (fn [actual-event-handler _]
                                                  (if @captured-handler
                                                    @unprocessed-events
                                                    (do
                                                      (reset! captured-handler actual-event-handler)
                                                      (throw Exception)))))
          mock-group-unprocessed-events-by (spy/mock (fn [_ _ event] (:group-key event)))
          mock-handle-event (spy/spy)
          mock-on-processing-complete
          (spy/mock (fn [_ _ event _] (remove-unprocessed-event event)))]

      (with-redefs [stub-get-unprocessed-events mock-get-unprocessed-events
                    stub-group-unprocessed-events-by mock-group-unprocessed-events-by
                    stub-handle-event mock-handle-event
                    stub-on-processing-complete mock-on-processing-complete]

        (do-until
          #(spy/called-at-least-n-times? stub-get-unprocessed-events 2)
          {:matcher true?
           :timeout 10000})

        (testing "other processing only called from one event handler"
          (is (= 1 (count (unique-entries
                            (map get-event-handler
                              (spy/calls mock-group-unprocessed-events-by))
                            (map get-event-handler
                              (spy/calls mock-handle-event))
                            (map get-event-handler
                              (spy/calls mock-on-processing-complete))))))))))

  (deftest systems-with-different-lock-ids-run-independently
    (let [test-system-3 (atom (new-test-system
                                {:database-port (.getPort ^EmbeddedPostgres @database)
                                 :db-lock-id 987654}))
          event {:message   "event"
                 :group-key "group-key"}
          unprocessed-events (atom [event])

          remove-unprocessed-event (fn [completed] (reset! unprocessed-events
                                                     (remove #(= % completed) @unprocessed-events)))

          mock-get-unprocessed-events (spy/mock (fn [_ _] @unprocessed-events))
          mock-group-unprocessed-events-by (spy/mock (fn [_ _ event] (:group-key event)))
          mock-handle-event (spy/mock (fn [_ _ _ _] (Thread/sleep 11000)))
          mock-on-processing-complete
          (spy/mock (fn [_ _ event _] (remove-unprocessed-event event)))]

      (try
        (do
          (reset! test-system-3 (component/start-system @test-system-3))

          (with-redefs [stub-get-unprocessed-events mock-get-unprocessed-events
                        stub-group-unprocessed-events-by mock-group-unprocessed-events-by
                        stub-handle-event mock-handle-event
                        stub-on-processing-complete mock-on-processing-complete]

            (do-until
              #(count (unique-entries
                        (map get-event-handler
                          (spy/calls stub-get-unprocessed-events))
                        (map get-event-handler
                          (spy/calls mock-group-unprocessed-events-by))
                        (map get-event-handler
                          (spy/calls mock-handle-event))
                        (map get-event-handler
                          (spy/calls mock-on-processing-complete))))
              {:matcher #(= 2 %)
               :timeout 10000})
            (let [unique-event-handlers (unique-entries
                                          (map get-event-handler
                                            (spy/calls stub-get-unprocessed-events))
                                          (map get-event-handler
                                            (spy/calls mock-group-unprocessed-events-by))
                                          (map get-event-handler
                                            (spy/calls mock-handle-event))
                                          (map get-event-handler
                                            (spy/calls mock-on-processing-complete)))]

              (testing "event processor from test-system 3 tries to process events"
                (is (some #(= (:event-handler @test-system-3) %)
                      unique-event-handlers))))))
        (finally
          (reset! test-system-3 (component/stop-system @test-system-3)))))))