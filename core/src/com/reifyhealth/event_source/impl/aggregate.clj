(ns com.reifyhealth.event-source.impl.aggregate
  (:require [meta-merge.core :as mm]
            [com.reifyhealth.event-source.impl.registrar :as reg]
            [com.reifyhealth.event-source.impl.event-store :as es]
            [com.reifyhealth.event-source.impl.event :as event])
  (:import (java.time Instant)
           (clojure.lang ExceptionInfo)))

;;
;; Aggregate
;;
;; {:meta {:ts 0 :version 0}
;;  :aggregate-name :foo
;;  :data {}}

;;
;; API
;;

(defmulti apply-event*
  (fn [event-name _aggregate-state _event-data]
    event-name))

(defmethod apply-event* :default
  [_ aggregate-state event]
  (mm/meta-merge aggregate-state event))

(defn get-snapshot
  [aggregate-name aggregate-id]
  (let [{:keys [snapshot]} (reg/get-aggregate-conf aggregate-name)
        event-store        (:event-store (reg/get-application))
        stream-id          (event/get-stream-id aggregate-name aggregate-id)
        curr-snapshot      (or (and snapshot
                                    (es/get-snapshot event-store stream-id))
                               {:meta {:ts (Instant/MIN)
                                       :version (es/initial-version event-store)}
                                :aggregate-name aggregate-name
                                :data nil})]
    (reduce
      (fn [agg-snapshot {:keys [event-type meta data]}]
        (-> agg-snapshot
            (assoc :meta meta)
            (update :data #(apply-event* event-type % data))))
      curr-snapshot
      (es/read-stream
        event-store
        stream-id
        (get-in curr-snapshot [:meta :version])))))
