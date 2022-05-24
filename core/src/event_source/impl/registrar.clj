(ns event-source.impl.registrar
  (:require [meta-merge.core :as mm]))

;;
;; Registrar maintains a state map in the `registry` atom as follows:
;;
;; {:application app       ;; The initialized EventStoreApp
;;
;;  :aggregates
;;  {:aggregate-name
;;   {:spec     nil        ;; The aggregate spec
;;    :id-field nil        ;; The field used to uniquely identify the aggregate
;;    :doc      nil        ;; The aggregate's doc string
;;    :snapshot false      ;; Whether to take regular snapshots of the aggregate for perf
;;    :commands []}}       ;; A vector of :command-names on the aggregate
;;
;;  :commands
;;  {:command-name
;;   {:spec         nil    ;; The command spec
;;    :id-field     nil    ;; The field that references the aggregate's unique id
;;    :aggregate    nil    ;; The :aggregate-name
;;    :events       []     ;; A vector of :event-names produced by the command
;;    :handler      nil    ;; The command handler fn (takes aggregate, command-data)
;;    :interceptors []}}   ;; A vector of interceptors to run for the command
;;
;;  :events
;;  {:event-name
;;   {:spec          nil   ;; The event spec
;;    :command       nil   ;; The :command-name that emits this event
;;    :subscriptions {}}}} ;; A map of event subscription maps
;;
(def ^:private registry (atom {}))

;;
;; Registration
;;

(defn register-application
  [app]
  (swap! registry assoc :application app)
  app)

(defn register-aggregate
  [n aggregate]
  (swap! registry update-in [:aggregates n] #(mm/meta-merge % aggregate))
  n)

(defn register-command
  [n {:keys [aggregate] :as command} events]
  (swap! registry mm/meta-merge {:aggregates {aggregate {:commands [n]}}
                                 :commands   {n command}
                                 :events     events})
  n)

(defn register-event-subscription
  [n {:keys [name] :as sub}]
  (swap! registry update-in [:events n :subscriptions name] #(conj % sub))
  n)

;;
;; Get Registry State
;;

(defn get-application
  []
  (get @registry :application))

(defn get-aggregate-conf
  [n]
  (some-> (get-in @registry [:aggregates n])
          (assoc :aggregate-name n)))

(defn get-command-conf
  [n]
  (some-> (get-in @registry [:commands n])
          (assoc :command-name n)
          (update :aggregate get-aggregate-conf)))

(defn get-event-conf
  [n]
  (some-> (get-in @registry [:events n])
          (assoc :event-name n)
          (update :command get-command-conf)))

(defn get-all-event-conf
  []
  (mapv get-event-conf (keys (:events @registry))))
