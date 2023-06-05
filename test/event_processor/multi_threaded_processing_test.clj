(ns event-processor.multi-threaded-processing-test
  (:require
    [clojure.test :refer :all]
    [event-processor.processor.protocols :refer [EventProcessor]]
    [event-processor.test-support.conditional-execution :refer [do-until]]
    [event-processor.test-support.postgres.database :as database]
    [event-processor.test-support.system :refer
     [with-system-lifecycle stub-get-unprocessed-events
      stub-group-unprocessed-events-by stub-handle-event
      stub-on-processing-complete]]
    [spy.core :as spy]))

; Same tests as in processing_test.clj by configured to run
; on a system with multiple threads (parallel groups of records)
(let [database (atom nil)
      test-system (atom nil)]
  (use-fixtures :each
    (database/with-database database)
    (with-system-lifecycle test-system database {:threads 10}))

  (deftest processes-single-event
    (let [^EventProcessor event-handler (:event-handler @test-system)
          event {:message "event"
                 :group-key "group-key"}
          unprocessed-events (atom [event])
          remove-unprocessed-event (fn [completed] (reset! unprocessed-events
                                                     (remove #(= % completed) @unprocessed-events)))
          mock-get-unprocessed-events (spy/mock (fn [_ _] @unprocessed-events))
          mock-group-unprocessed-events-by (spy/mock (fn [_ _ event] (:group-key event)))
          mock-handle-event (spy/spy)
          mock-on-processing-complete
          (spy/mock (fn [_ _ event _] (remove-unprocessed-event event)))]
      (with-redefs [stub-get-unprocessed-events mock-get-unprocessed-events
                    stub-group-unprocessed-events-by mock-group-unprocessed-events-by
                    stub-handle-event mock-handle-event
                    stub-on-processing-complete mock-on-processing-complete]

        (do-until
          #(spy/called-once? mock-on-processing-complete)
          {:matcher true?
           :timeout 10000})

        (testing "has called handle-event"
          (is (spy/called-once? mock-handle-event))
          (let [[actual-event-handler _ actual-event _]
                (spy/first-call mock-handle-event)]
            (is (= actual-event-handler event-handler))
            (is (= actual-event event))))

        (testing "has called on-processing-complete"
          (is (spy/called-once? mock-on-processing-complete))
          (let [[actual-event-handler _ actual-event _]
                (spy/first-call mock-on-processing-complete)]
            (is (= actual-event-handler event-handler))
            (is (= actual-event event)))))))

  (deftest processes-multiple-events-from-same-group
    (let [^EventProcessor event-handler (:event-handler @test-system)
          event-1 {:message "event-1"
                   :group-key "group-key-1"}
          event-2 {:message "event-2"
                   :group-key "group-key-1"}

          unprocessed-events (atom [event-1 event-2])
          remove-unprocessed-event (fn [completed] (reset! unprocessed-events
                                                     (remove #(= % completed) @unprocessed-events)))
          mock-get-unprocessed-events (spy/mock (fn [_ _] @unprocessed-events))
          mock-group-unprocessed-events-by (spy/mock (fn [_ _ event] (:group-key event)))
          mock-handle-event (spy/mock (fn [_ _ event _]
                                        (when (= event event-1)
                                          (throw (Exception. "bad event")))))
          mock-on-processing-complete
          (spy/mock (fn [_ _ event _] (remove-unprocessed-event event)))]
      (with-redefs [stub-get-unprocessed-events mock-get-unprocessed-events
                    stub-group-unprocessed-events-by mock-group-unprocessed-events-by
                    stub-handle-event mock-handle-event
                    stub-on-processing-complete mock-on-processing-complete]

        (Thread/sleep 10000)

        (testing "has called handle-event once"
          (is (spy/called-at-least-once? mock-handle-event))
          (let [[actual-event-handler _ actual-event _] (spy/first-call mock-handle-event)]
            (is (= actual-event-handler event-handler))
            (is (= actual-event event-1))))

        (testing "has not called on-processing-complete"
          (is (spy/not-called? mock-on-processing-complete))))))

  (deftest processes-multiple-events-from-different-groups
    (let [event-1 {:message "event-1"
                   :group-key "group-key-1"}
          event-2 {:message "event-2"
                   :group-key "group-key-2"}

          unprocessed-events (atom [event-1 event-2])
          remove-unprocessed-event (fn [completed] (reset! unprocessed-events
                                                     (remove #(= % completed) @unprocessed-events)))
          mock-get-unprocessed-events (spy/mock (fn [_ _] @unprocessed-events))
          mock-group-unprocessed-events-by (spy/mock (fn [_ _ event] (:group-key event)))
          mock-handle-event (spy/mock (fn [_ _ event _]
                                        (when (= event event-1)
                                          (throw (Exception. "bad event")))))
          mock-on-processing-complete
          (spy/mock (fn [_ _ event _] (remove-unprocessed-event event)))]
      (with-redefs [stub-get-unprocessed-events mock-get-unprocessed-events
                    stub-group-unprocessed-events-by mock-group-unprocessed-events-by
                    stub-handle-event mock-handle-event
                    stub-on-processing-complete mock-on-processing-complete]

        (do-until
          #(spy/called-once? mock-on-processing-complete)
          {:matcher true?
           :timeout 10000})

        (testing "has called handle-event"
          (is (= {event-1 1
                  event-2 1}
                (->> (spy/calls mock-handle-event)
                  (mapv (fn [call]
                          (nth call 2)))
                  (frequencies)))))

        (testing "has called on-processing-complete on event-2"
          (is (spy/called-once? mock-on-processing-complete))
          (let [[_ _ actual-event _] (spy/first-call mock-on-processing-complete)]
            (is (= actual-event event-2)))))

      (let [mock-handle-event (spy/mock (fn [_ _ _ _]))
            mock-on-processing-complete
            (spy/mock (fn [_ _ event _] (remove-unprocessed-event event)))]
        (with-redefs [stub-get-unprocessed-events mock-get-unprocessed-events
                      stub-group-unprocessed-events-by mock-group-unprocessed-events-by
                      stub-handle-event mock-handle-event
                      stub-on-processing-complete mock-on-processing-complete]

          (do-until
            #(spy/called-once? mock-on-processing-complete)
            {:matcher true?
             :timeout 10000})

          (testing "has called handle-event on previously incomplete event"
            (is (spy/called-once? mock-handle-event))
            (let [[_ _ actual-event _] (spy/first-call mock-handle-event)]
              (is (= actual-event event-1))))

          (testing "has called on-processing-complete on event-1"
            (is (spy/called-once? mock-on-processing-complete))
            (let [[_ _ actual-event _] (spy/first-call mock-on-processing-complete)]
              (is (= actual-event event-1)))))))))
