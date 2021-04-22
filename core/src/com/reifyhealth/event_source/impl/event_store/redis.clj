(ns com.reifyhealth.event-source.impl.event-store.redis
  (:require [taoensso.carmine :as redis]
            [com.reifyhealth.event-source.impl.event-store :as es]
            [meta-merge.core :as mm])
  (:import (java.io Closeable)
           (java.time Instant)
           (java.util.concurrent ScheduledThreadPoolExecutor TimeUnit)
           (clojure.lang ExceptionInfo)))

(def ^:private stream-prefix "es:stream/")

(def ^:private meta-prefix "es:meta/")

(def ^:private snapshot-prefix "es:snapshot/")

(def ^:private all-events-stream-id "all-events")

(def ^:private initial-version "0-0")

(defn- parse-version
  [v]
  (if-let [[_ base batch] (re-matches #"((?:[^\-]|[0-9])+)\-([0-9]+)" v)]
    {:base-version  (Long/parseLong base)
     :batch-version (Long/parseLong batch)}
    (throw (ex-info "Version string does not conform to expected format"
                    {:version-string v}))))

(defn- inc-versions
  [current-version event-count]
  (let [{:keys [base-version]} (parse-version current-version)
        new-base (inc base-version)]
    (mapv #(str new-base "-" %) (range event-count))))

(defn- read-stream->events
  [read-stream-resp]
  (mapv
    (fn [[_ [_ meta _ event]]]
      (assoc event :meta meta))
    ((comp second first) read-stream-resp)))

;; TODO:
;; allow these to be configurable
(def ^:private +subscriber-pool-size+ (int 10))
(def ^:private +subscriber-initial-delay-millis+ 5000)
(def ^:private +subscriber-delay-millis+ 1000)

(defn- subscription-exists?
  [conn stream sub-name]
  (try
    (some
      (fn [[_ group-name]] (= sub-name group-name))
      ;; throws "ERR no such key" if the stream doesn't exist
      (redis/wcar conn (redis/xinfo "GROUPS" stream)))
    (catch ExceptionInfo _)))

(defn- get-raw-events
  [conn subscriber-name stream]
  ;; For now, we only have a single consumer per subscription, but we could
  ;; facilitate multiple consumers per subscription in the future for perf
  (let [consumer (str subscriber-name "-0")]
    (into []
          ;; The below redis call returns:
          ;; [[["stream" [["1619034749935-0" ["meta" {} "data" {}]]]]]
          ;;  [["stream" [["1619035040818-0" ["meta" {} "data" {}]]]]]]
          (comp (map first)
                (mapcat (fn [[_ raw-events]] raw-events)))
          (redis/wcar
            conn
            ;; Retrieve any previously un-acked messages
            (redis/xreadgroup "GROUP" subscriber-name consumer "STREAMS" stream "0")
            ;; Plus any new messages
            (redis/xreadgroup "GROUP" subscriber-name consumer "STREAMS" stream ">")))))

(defn- get-subscription-runnable
  [conn {:keys [subscriber-name stream]} handler]
  (fn []
    (try
      (doseq [[ack-id [_ _ _ event]] (get-raw-events conn subscriber-name stream)]
        (try
          (handler event)
          (catch Throwable e
            ;; TODO:
            ;; Add proper logger
            ;; Add error callback and/or retry capabilities
            (println "Received error processing event with handler, skipping")
            (clojure.stacktrace/print-stack-trace e)))
        (redis/wcar conn (redis/xack stream subscriber-name ack-id)))
      (catch Throwable e
        ;; TODO:
        ;; Add proper logger
        (println "Received unexpected error processing event, will retry")
        (clojure.stacktrace/print-stack-trace e)))))

(defrecord RedisEventStore [conn subscriber-pool subscriptions]
  es/EventStore
  (initial-version [_] initial-version)

  (append-to-stream [_ stream-id txn-id expected-version events]
    (let [meta-k
          (str meta-prefix stream-id)

          stream-k
          (str stream-prefix stream-id)

          all-events-stream-k
          (str stream-prefix all-events-stream-id)

          [_ {:keys [current-version last-txn-id]
              :or {current-version initial-version}
              :as current-stream-meta}]
          (redis/wcar conn (redis/watch meta-k) (redis/get meta-k))]
      ;; If the txn-id matches, we have a duplicate submission
      ;; Return successful to treat it as an idempotent operation
      (or (when (= txn-id last-txn-id)
            (try (redis/wcar conn (redis/unwatch)) (catch Throwable _))
            current-stream-meta)
          ;; If the versions are the same, then we can proceed with processing
          (when (= expected-version current-version)
            ;; If there were no changes to the stream, the call wil be
            ;; successful and redis will return:
            ;;
            ;;   ["OK" "QUEUED" "QUEUED" "QUEUED" ... ["OK" "{xadd-id}" "{meta}"]]
            ;;
            ;; The last element of the vector is the result of the transaction: a
            ;; vector of the result of each transacted operation.
            ;;
            ;; If the transaction fails, the result will be:
            ;;
            ;;   ["OK" "QUEUED" "QUEUED" "QUEUED" ... nil]
            ;;
            ;; and processing will continue to the last form in the outer `or`
            (let [event-versions  (inc-versions current-version (count events))
                  event-meta      (mapv #(hash-map :ts (Instant/now) :version %)
                                        event-versions)
                  new-stream-meta {:current-version (last event-versions)
                                   :last-txn-id     txn-id}]
              (when (-> (redis/wcar
                            conn
                            (concat
                              [(redis/multi)
                               (redis/set meta-k new-stream-meta)]
                              (into []
                                    (mapcat
                                      (fn [e m]
                                        [(redis/xadd stream-k (:version m)
                                                     "meta"  m
                                                     "event" e)
                                         (redis/xadd all-events-stream-k "*"
                                                     "meta"  m
                                                     "event" e)])
                                      events
                                      event-meta))
                              [(redis/get meta-k)
                               (redis/exec)]))
                          last
                          some?)
                  (mapv #(assoc %1 :meta %2) events event-meta))))
          ;; If we've reached this far, then either the expected version
          ;; did not match the current version, or there was a concurrent
          ;; operation that failed the event transaction. In either case,
          ;; we invoke our optimistic concurrency control and throw a
          ;; concurrent operation exception to the caller to decide on how to
          ;; proceed.
          (do (try (redis/wcar conn (redis/unwatch)) (catch Throwable _))
              (throw (ex-info "Stream version does not match expected version"
                              {:type :concurrent-operation-exception
                               :stream-id stream-id}))))))

  (read-stream [_ stream-id]
    (read-stream->events
      (redis/wcar
        conn
        (redis/xread "STREAMS" (str stream-prefix stream-id) initial-version))))

  (read-stream [_ stream-id start-version]
    (read-stream->events
      (redis/wcar
        conn
        (redis/xread "STREAMS" (str stream-prefix stream-id) start-version))))

  (read-stream [_ stream-id start-version limit]
    (read-stream->events
      (redis/wcar
        conn
        (redis/xread
          "COUNT" limit "STREAMS" (str stream-prefix stream-id) start-version))))

  (subscribe [this subscriber-name event-handler]
    (es/subscribe this subscriber-name event-handler nil))

  (subscribe [_ subscriber-name event-handler {:keys [start-from stream-id]
                                               :or {stream-id all-events-stream-id}}]
    ;; First check if we already have a subscription for this subscriber-name
    ;; and create it if it doesn't already exist.
    (when-not (subscription-exists?
                conn (str stream-prefix stream-id) subscriber-name)
      (redis/wcar
        conn
        (redis/xgroup
          "CREATE"
          (str stream-prefix stream-id)
          subscriber-name
          (if (= start-from :latest) "$" "0")
          "MKSTREAM")))
    ;; Next, schedule the subscription with the thread pool
    (let [sub {:subscriber-name subscriber-name
               :stream (str stream-prefix stream-id)}
          fut (.scheduleWithFixedDelay
                ^ScheduledThreadPoolExecutor subscriber-pool
                ^Runnable (get-subscription-runnable conn sub event-handler)
                ^Long +subscriber-initial-delay-millis+
                ^Long +subscriber-delay-millis+
                TimeUnit/MILLISECONDS)]
      ;; Finally, save our subscription state
      (swap! subscriptions
             mm/meta-merge
             {(str stream-prefix stream-id)
              {subscriber-name (merge sub {:future fut})}}))
    subscriber-name)

  (save-snapshot [_ stream-id snapshot]
    (redis/wcar conn (redis/set (str snapshot-prefix stream-id) snapshot)))

  (get-snapshot [_ stream-id]
    (redis/wcar conn (redis/get (str snapshot-prefix stream-id))))

  Closeable
  (close [_]
    (.shutdown ^ScheduledThreadPoolExecutor subscriber-pool)
    (reset! subscriptions {})))

(defn new-redis-event-store
  ([]
   (new-redis-event-store {} {:uri "redis://127.0.0.1:6379"}))
  ([{:keys [pool spec]}]
   (new-redis-event-store pool spec))
  ([pool spec]
   (->RedisEventStore {:pool pool :spec spec}
                      (ScheduledThreadPoolExecutor. +subscriber-pool-size+)
                      (atom {}))))
