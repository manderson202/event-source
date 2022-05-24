(ns getting-started
  "A simple Getting Started example using a contrived banking application.
  The intent is to share an intuition for the system rather than represent
  a true real-world application.
  "
  (:require [clojure.spec.alpha :as s]
            [event-source.core :as es])
  (:import (java.util UUID)))

;;
;; First we define our data model for the system. These specs represent
;; the entity types in our system and can be thought of in three categories:
;;
;; - Aggregates: a top-level domain entity that should be transactionally
;;   consistent with its state. It can reference other aggregates via ID.
;;
;; - Commands: operations that modify the state of an aggregate. These are
;;   named in the imperative and represent a desired action to take
;;
;; - Events: the results of commands. Events represent a completed operation
;;   and are named in the past tense.
;;

(s/def ::account-id string?)
(s/def ::account-type #{:checking :savings})
(s/def ::balance float?)
(s/def ::amount ::balance)

;; The Bank Account aggregate

(s/def ::bank-account
  (s/keys :req-un [::account-id
                   ::account-type
                   ::balance]))

;; The following commands are available on a Bank Account

(s/def ::open-account
  (s/keys :req-un [::account-type]))

(s/def ::change-account-type
  (s/keys :req-un [::account-id ::account-type]))

(s/def ::deposit-money
  (s/keys :req-un [::account-id ::amount]))

;; The following events are emitted for the above commands

(s/def ::account-opened ::bank-account)

(s/def ::account-type-changed ::change-account-type)

(s/def ::money-deposited
  (s/keys :req-un [::account-id ::amount]))

;; Now that the specs have been defined for the entities in the system we can
;; register each Aggregate, Command, and Event by defining them.

;; Here we define the Bank Account aggregate. Since we named it the same as
;; the spec we do not need to define the spec separately (however, the option
;; is available by adding a :spec option below).
(es/defaggregate ::bank-account
  "Represents a bank account. Can be either a Checking or Savings account"
  :id :account-id)

;; The open-account command is defined here. We provide a description, the
;; aggregate it should operate on, and the event(s) it emits. Finally, our
;; command handler function, which takes the aggregate state (in our case
;; there is none, since this is a brand new account), and the command data,
;; which in this case contains the account-type to open. We return an
;; account-opened event. Note, each command handler can return 0 or more
;; events (events are `[event-name data]` tuples).
;;
;; Like with the defaggregate above, we could have also defined an :id field
;; if it were different on the command than the aggregate, and a spec, if we
;; had not used the spec name as the command name. Finally, we could have
;; defined an interceptors vector, which we will discuss later.
;;
;; The command handler should be a referentially transparent function. If
;; side effects are needed (eg: making a call to another aggregate to get
;; information) they should be done in interceptors and provided as context
;; to the command function.
(es/defcommand ::open-account
  "Opens a new bank account of the given account type"
  :on    ::bank-account
  :emits [::account-opened]
  (fn [_ {:keys [account-type]}]
    [::account-opened
     {:account-id   (str (UUID/randomUUID))
      :account-type account-type
      :balance      0.}]))

;; In this command definition, the function leverages the current state to make
;; decisions on handling. This also demonstrates it is possible for a command
;; to not emit any events.
(es/defcommand ::change-account-type
  "Allows changing the type of account between checking and savings"
  :on    ::bank-account
  :emits [::account-type-changed]
  (fn [{{:keys [account-id account-type]} ::bank-account} cmd-data]
    (when (not= account-type (:account-type cmd-data))
      [::account-type-changed
       {:account-id account-id :account-type (:account-type cmd-data)}])))

;; We don't need to do spec validation in command handlers as it has already
;; been done at command dispatch. At this point, all state should be valid
;; per its spec. We can, however, throw errors if there are business invariants
;; violated.
(es/defcommand ::deposit-money
  "Deposit money to an account"
  :on    ::bank-account
  :emits [::money-deposited]
  (fn [{{:keys [account-id]} ::bank-account} {:keys [amount]}]
    (if (neg? amount) ;; perhaps we should consider adding this check to the spec?
      (throw (ex-info "Deposit amount can't be negative" {:amount amount}))
      [::money-deposited {:account-id account-id :amount amount}])))

;; The next step after defining our commands is considering how the events
;; generated will be applied to the aggregate state. In most cases this should
;; be designed as a simple "fold-left" merge across the events into the
;; aggregate. This is the default case and if that is sufficient, nothing
;; further is needed for command definition.
;;
;; However, if there needs to be different logic in how to apply an event
;; you will need to register a handler that will be used for "sourcing" an
;; aggregate for a particular event type. This handler has two important
;; characteristics: first, like command handlers, it should be referentially
;; transparent. It must act only on the arguments provided. Second, it should
;; be backwards compatible. This function will be used to source aggregates
;; across all past events and so this function must be able to support all
;; changes over time.

;; When depositing money we can't just merge the new state in, we must
;; add the deposited amount to the balance, so we define a custom source
;; function for this event.
(es/source-aggregate-with ::money-deposited
  (fn [aggregate {:keys [amount] :as _event}]
    (update aggregate :balance + amount)))

;; The final piece to define are event subscriptions. These are event handlers
;; that are run asynchronously and receive new events as they are emitted.
;; This allows for stateful actions: invoking another command, writing state
;; to an external data store, generating emails or other notifications, etc.
;;
;; We will most likely include some standard event handlers in a proper library
;; such as "write state to postgres" or "emit events to Kafka", but the
;; options are wide open for what a subscriber can do.

;; For our application we want to notify users whenever money is deposited
;; we will simulate this with a message to the console. We use the
;; :start-from key to tell the system we only want to send notifications for
;; new events (and not notify for all the old events in the system). The
;; default for this key is :origin.
(es/subscribe-event ::money-deposited
  "Notify the user when money is deposited in their account"
  :name       ::deposit-notification
  :start-from :latest
  (fn [{:keys [amount] :as _event}]
    (println "Received deposit of" amount "! Nice!")))

;; Now that all of the pieces are defined, we can launch the application.
;; Uncomment below to run the app. In a real application this state
;; would be managed with something like mount or component, but here we
;; just manage it with defs to play around in the REPL.

;; Note: please ensure you have Redis running locally before launching!!

(comment

  (def app
    (es/start-application!
      ::my-banking-app
      {:event-store
       {:type :redis
        :pool {}
        :spec {:uri "redis://127.0.0.1:6379"}}}))

  ;; Examples of dispatching commands to open an account and deposit money.
  ;; The result of a call to dispatch is the generated events.

  ;(def account-id
  ;  (-> (es/dispatch! ::open-account {:account-type :checking})
  ;      first
  ;      :data
  ;      :account-id))
  ;
  ;(es/dispatch! ::deposit-money {:account-id account-id :amount 25.17})
  ;
  ;(es/get-aggregate ::bank-account account-id)

  ;; When finished, shut down the app
  ;(es/stop-application! app)

  ;; You can clean up your redis by going to `redis-cli` and running FLUSHALL
  ;; (beware, this will remove *all* keys from the entire DB, so only use
  ;; if this is a local testing DB)

  )
