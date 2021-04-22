(ns com.reifyhealth.event-source.impl.event
  (:require [clojure.spec.alpha :as s]
            [com.reifyhealth.event-source.impl.registrar :as reg])
  (:import (clojure.lang Named)
           (java.time Instant)))

;;
;; Event (user)
;;
;; [:event-type {}]
;;
;; Event (internal)
;;
;; {:meta {:ts 0 :version 0}
;;  :event-type :et
;;  :data {}}

;;
;; Spec
;;

(s/def ::event-type keyword?)

(s/def ::data (s/nilable map?))

(s/def ::ts #(instance? Instant %))

(s/def ::version string?)

(s/def ::meta (s/keys :req-un [::ts ::version]))

(defmulti user-event-spec first)
(defmethod user-event-spec :default
  [[event-type _]]
  (let [data-spec (or (some-> (reg/get-event-conf event-type) :spec)
                      ::data)]
    (s/cat :event-type keyword? :data (s/nilable data-spec))))

(s/def ::user-event
  (s/multi-spec user-event-spec (fn [gv _] gv)))

(s/def ::event
  (s/or :internal-event
        (s/keys :req-un [::event-type]
                :opt-un [::data ::meta])

        :user-event
        ::user-event))

(s/def ::event-coll
  (s/coll-of ::event))

;;
;; Internal
;;

(defn- ->str-id
  [x]
  (cond (string? x)         x
        (instance? Named x) (str (namespace x) "." (name x))
        :else               (str x)))

;;
;; API
;;

(defn parse-events
  [events]
  (let [events (if (and (sequential? events)
                        (or (sequential? (first events))
                            (map? (first events))))
                 events
                 [events])
        parsed (s/conform ::event-coll events)]
    (if (= parsed ::s/invalid)
      (throw (ex-info "Event malformed"
                      {:explain-data (s/explain-data ::event-coll events)}))
      (mapv second parsed))))

(defn get-stream-id
  [aggregate-name aggregate-id]
  (str (->str-id (reg/get-application))
       ":"
       (->str-id aggregate-name)
       ":"
       (->str-id aggregate-id)))
