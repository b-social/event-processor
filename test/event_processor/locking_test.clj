(ns event-processor.locking-test
  (:require
    [clojure.test :refer [use-fixtures deftest testing is]]
    [event-processor.test-support.system :refer
     [new-test-system with-system-lifecycle stub-get-unprocessed-events
      stub-group-unprocessed-events-by stub-handle-event
      stub-on-processing-complete]]
    [event-processor.test-support.postgres.database :as database]
    [event-processor.test-support.conditional-execution :refer [do-until]]
    [spy.core :as spy]
    [clojure.set :refer [union]]))


(defn unique-entries [& entries-collections]
  (apply union (map set entries-collections)))

(def get-event-handler first)

(let [database (database/new-database)
      test-system-1 (atom (new-test-system database))
      test-system-2 (atom (new-test-system database))]
  (use-fixtures :each
    (database/with-database database)
    (with-system-lifecycle test-system-1)
    (with-system-lifecycle test-system-2))

  (deftest multiple-systems-should-not-process-concurrently
    (let [event {:message   "event"
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

        (doall (map println (spy/calls stub-get-unprocessed-events)))

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

  (deftest multiple-systems-should-not-process-concurrently
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
          #(spy/called-once? mock-on-processing-complete)
          {:matcher true?
           :timeout 10000})

        (testing "get-unprocessed-events called from both handlers"
          (is (= 2 (count (unique-entries
                            (map get-event-handler
                              (spy/calls stub-get-unprocessed-events)))))) )

        (testing "other processing only called from one event handler"
          (is (= 1 (count (unique-entries
                            (map get-event-handler
                              (spy/calls mock-group-unprocessed-events-by))
                            (map get-event-handler
                              (spy/calls mock-handle-event))
                            (map get-event-handler
                              (spy/calls mock-on-processing-complete)))))))))))