(ns com.reifyhealth.event-source.impl.interceptor
  (:require [com.reifyhealth.event-source.impl.aggregate :as aggregate]
            [com.reifyhealth.event-source.impl.event :as event]
            [com.reifyhealth.event-source.impl.event-store :as es]
            [com.reifyhealth.event-source.impl.registrar :as reg]
            [io.pedestal.interceptor :as interceptor]
            [io.pedestal.interceptor.chain :as int-chain]
            [clojure.spec.alpha :as s])
  (:import (java.util UUID)))

;;
;; Context Map
;;
;; {:command-conf       {}
;;  :command-data       {}
;;  :state              {}
;;  :events             []
;;  :meta               {}
;;  :event-store-client {}}

(defn get-command-handler
  [f]
  (interceptor/interceptor
    {:name  ::command-handler
     :enter (fn [{:keys [state command-data] :as ctx}]
              (let [event-response (f state command-data)]
                (assoc ctx
                  :events (if (empty? event-response)
                            []
                            (event/parse-events event-response)))))}))

(defn- apply-events
  [state events]
  (reduce
    (fn [state {:keys [event-type data]}]
      (aggregate/apply-event* event-type state data))
    state
    events))

(def context-interceptor
  (interceptor/interceptor
    {:name
     ::context-interceptor

     :enter
     (fn [{:keys [command-data]
           {:keys [id-field]
            {:keys [aggregate-name]} :aggregate} :command-conf
           :as ctx}]
       (let [{:keys [meta data]} (aggregate/get-snapshot
                                   aggregate-name
                                   (get command-data id-field))]
         (-> ctx
             (assoc-in [:meta aggregate-name] meta)
             (assoc-in [:state aggregate-name] data))))

     :leave
     (fn [{:keys [event-store-client meta state events]
           {{:keys [aggregate-name id-field spec]} :aggregate} :command-conf}]
       (if (not-empty events)
         (let [agg    (apply-events (get state aggregate-name) events)
               agg-id (get agg id-field)
               txn-id (str (UUID/randomUUID))]
           (when-not (s/valid? spec agg)
             (throw (ex-info "Applying events results in an invalid aggregate state"
                             {:explain-data (s/explain-data spec agg)})))
           (es/append-to-stream
             event-store-client
             (event/get-stream-id aggregate-name agg-id)
             txn-id
             (get-in meta [aggregate-name :version])
             events))
         []))

     :error
     (fn [_ctx e]
       ;; TODO:
       ;; Consider if we should add any additional error handling is needed if
       ;; an exception is thrown during processing, or if raising to the caller
       ;; is sufficient
       (throw e))}))

(defn execute-command!
  [{:keys [interceptors handler] :as cmd-conf} data]
  (let [interceptor-chain (concat [context-interceptor]
                                  interceptors
                                  [(get-command-handler handler)])]
    (int-chain/execute
      {:command-conf       cmd-conf
       :command-data       data
       :state              {}
       :events             nil
       :meta               {}
       :event-store-client (:event-store (reg/get-application))}
      interceptor-chain)))
