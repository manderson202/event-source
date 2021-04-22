(ns com.reifyhealth.event-source.core
  "Provides the API for defining an event sourced application"
  (:require [clojure.spec.alpha :as s]
            [com.reifyhealth.event-source.impl.aggregate :as aggregate]
            [com.reifyhealth.event-source.impl.dispatch :as dispatch]
            [com.reifyhealth.event-source.impl.registrar :as reg]
            [com.reifyhealth.event-source.impl.subscription :as subscription]
            [com.reifyhealth.event-source.impl.event-store.redis :as es-redis])
  (:import (java.io Closeable)))

;;
;; Private
;;

(defn- namespaced-keyword
  [n]
  (when-not (or (symbol? n) (keyword? n))
    (throw (ex-info "Name should be a symbol or keyword" {:name n})))
  (let [ns-str     (or (namespace n) (-> *ns* ns-name name))
        name-str   (name n)]
    (keyword ns-str name-str)))

(defn- parse-def*
  [n & forms]
  (let [doc        (when (string? (first forms)) (first forms))
        raw-config (if (string? (first forms)) (rest forms) forms)
        _          (when (and (pos? (count raw-config)) (odd? (count raw-config)))
                     (throw (ex-info "Must supply even number of config parameters"
                                     {:config raw-config})))
        config     (into {} (comp (partition-all 2) (map vec)) raw-config)]
    (merge config {:def-name (namespaced-keyword n) :doc doc})))

;;
;; API: Definition
;;

;; TODO
;; Add :snapshot config, default false, to enable snapshotting of agg state.
;; if true, it should create a subscription that will update the snapshot

(defmacro defaggregate
  "Define a new Aggregate in the system with the given name, which should
  be globally unique. It is recommended to use a namespaced keyword.
  Can optionally specify a documentation string and configuration
  parameters:

    - :spec     : should reference a valid Clojure Spec, if not specified
                  the aggregate name will be used to look up a spec by
                  the same name, else `any?` -- effectively no spec.

    - :id-field : the field that should uniquely identify the aggregate
                  if not provided, will default to `:id`

  Example:

  (defaggregate ::bank-account
    \"Represents a bank account\"
    :id-field :some-id     ;; would not be required if the agg uses `:id`
    :spec     ::some-spec) ;; would not be required if (s/def ::bank-account ...)
  "
  {:arglists '([name doc-string? & config])}
  [n & forms]
  (let [{:keys [def-name doc id spec]} (apply parse-def* n forms)]
    `(reg/register-aggregate
       ~def-name
       ~{:id-field (or id :id)
         :doc      doc
         :spec     (or spec
                       (when (s/get-spec def-name) def-name)
                       any?)
         :snapshot false})))

(defmacro defcommand
  "Define a new Command with the given name, which should be globally unique.
  It is recommended to use a namespaced keyword. Can optionally specify a
  documentation string. Supports the following configuration parameters:

    - :on           : REQUIRED parameter that should reference an aggregate
                      name already defined that the command affects

    - :emits        : REQUIRED parameter of events emitted by this command.
                      Can either be a vector of keyword event-names, which
                      will be used to look up the event specs, or a map of
                      event-name to Clojure spec.

    - :id-field     : the field to use to uniquely identify the aggregate. If
                      unspecified, will use the defaggregate id-field

    - :spec         : should reference a valid Clojure Spec, if not specified
                      the command name will be used to look up a spec by
                      the same name, else `any?` -- effectively no spec.

    - :interceptors : an optional vector of interceptors to run upon command
                      handling.

  Finally, define the 2-arity command handler function that will be run when
  the command is received. This function should be referentially transparent.
  If side effects are needed, EG: the command augmented with data from another
  aggregate, use interceptors.

  The function will receive a context map that will contain, at minimum,
  aggregate-name -> latest aggregate state. The context map can also contain any
  other custom keys added by interceptors. The second parameter will contain the
  command data.

  The function should return one or more event tuples: `[event-name {}]`, which
  will be written to the event stream. They will be applied to the aggregate
  using the `source-aggregate` reducing function in the order returned.

  Example:

  ;; Defines a command `::deposit-money` on the `::bank-account` aggregate. It
  ;; emits `::money-deposited` events. It assumes specs have been defined with
  ;; the same names as the command and events.
  ;;
  (defcommand ::deposit-money
    :on    ::bank-account
    :emits [::money-deposited]
    (fn [{::keys [bank-account] :as ctx} command-data] ...)
  "
  {:arglists '([name doc-string? & config])}
  [n & forms]
  (let [{:keys [def-name doc on emits id spec interceptors]}
        (apply parse-def* n (butlast forms))

        handler
        (last forms)]
    `(let [aggregate# (reg/get-aggregate-conf ~on)
           events#    ~(if (map? emits)
                         emits
                         `(into {}
                                (map
                                  (fn [x#]
                                    [x# (or (when (s/get-spec x#) x#)
                                            any?)]))
                                ~emits))]
       (when (nil? aggregate#)
         (throw (ex-info "Aggregate not found" {:aggregate ~on})))
       (reg/register-command
         ~def-name
         {:id-field     (or ~id (:id-field aggregate#))
          :doc          ~doc
          :spec         (or ~spec
                            (when (s/get-spec ~def-name) ~def-name)
                            ~any?)
          :aggregate    ~on
          :events       (mapv ~first events#)
          :handler      ~handler
          :interceptors ~interceptors}
         (into {} (map (fn [[e# s#]] [e# {:spec s# :command ~def-name}])) events#)))))

(defmacro source-aggregate-with
  "Define a reducing function to \"source\" the aggregate for events with the
  given `event-name`. This is optional and if not specified a default will be
  used, which uses a deep-merge reducer. In most cases this should be
  sufficient, but if not use this to define a custom source fn. The fn will
  receive the current aggregate data as the first arg and the event data to
  be applied to it as the second arg.

  NOTE: the function must be referentially transparent. Also, great care should
  be taken in maintaining backwards compatibility since this function will be
  used to source the current state of the aggregate across all past events."
  [event-name f]
  `(defmethod aggregate/apply-event* ~event-name
     [_# aggregate# event#]
     (~f aggregate# event#)))

(defmacro subscribe-event
  "Define a new event subscription for the event with the specified name.
  Can optionally provide a documentation string. The following configurations
  are supported:

    - :name       : name for the subscription

    - :start-from : where the subscriber should start from, defaults to
                    `:origin`, but can also be `:latest` if it is only
                    desired to process any new events from definition
                    time (eg: if you add a new subscriber to send emails
                    after the system has been running and don't want
                    to send emails on past events)

  Finally, the event handler function should be provided. It is a single-arity
  function that receives the event data. It will be called at some point after
  event submission and all operations should be considered eventually
  consistent.

  Example:

  (subscribe-event ::money-deposited
    \"Notify the user when money is deposited in their account\"
    :name       ::deposit-notification
    :start-from :latest
    (fn [event] ...))
  "
  {:arglists '([event-name doc-string? & config])}
  [n & forms]
  (let [{:keys [def-name doc name start-from]}
        (apply parse-def* n (butlast forms))

        handler
        (last forms)]
    `(reg/register-event-subscription
       ~def-name
       ~{:name        (or name (str (gensym "sub")))
         :doc         doc
         :start-from  (or start-from :origin)
         :handler     handler})))

;; TODO:
;; Interceptor definition functions plus some default utility interceptors

;;
;; API: Runtime
;;

(defn dispatch!
  "Takes a command name and optional payload (some commands may not require
  data) and synchronously dispatches the command to be executed against the
  aggregate. Returns a vector of events that resulted from running the command.

  If there is a concurrent request on the aggregate with the same ID one will
  complete and the other will throw a concurrent operation exception. This
  allows command handlers to assume they have the latest state when they
  emit their event via opportunistic concurrency control. It is up to the user
  on how to handle these exceptions. It may simply be enough to retry again or
  to pass the exception up the stack if the originator needs to consider new
  information before resubmitting the command.
  "
  ([command-name]
   (dispatch! command-name nil))
  ([command-name data]
   (dispatch/dispatch-command
     (namespaced-keyword command-name)
     data)))

;; TODO:
;; A ring implementation for event-source that allows dispatching based on
;; ring-compliant requests and returning events as responses.

;; TODO:
;; Should consider adding a more robust query API
(defn get-aggregate
  "Returns the current state of the aggregate with the given id"
  [aggregate-name aggregate-id]
  (:data (aggregate/get-snapshot aggregate-name aggregate-id)))

;;
;; API: Lifecycle
;;

(defrecord EventSourceApp [app-name ^Closeable event-store]
  Closeable
  (close [_]
    (.close event-store)))

(defn start-application!
  "Starts a new event-source application with the given name and configuration.
  The configuration map currently accepts the following:

    - :event-store : should reference a configuration map for the event-store
                     implementation to use. Must include a valid :type key
                     for a supported store (currently, :redis) and any other
                     configurations relevant for the store.

  Returns an EventSourceApp object that can be passed to `stop-application!` on
  shutdown.
  "
  [app-name {:keys [event-store] :as app-config}]
  (let [event-store-impl
        (case (:type event-store)
          :redis (es-redis/new-redis-event-store event-store)
          (throw
            (ex-info (str "Event store with type " (:type event-store)
                          " not supported")
                     {:app-name app-name :app-config app-config})))

        app
        (->EventSourceApp app-name event-store-impl)]
    (reg/register-application app)
    (subscription/start-subscriptions!)
    app))

(defn stop-application!
  [app]
  (.close ^Closeable app)
  (reg/register-application nil))
