(ns com.reifyhealth.event-source.impl.dispatch
  (:require [clojure.spec.alpha :as s]
            [com.reifyhealth.event-source.impl.interceptor :as interceptor]
            [com.reifyhealth.event-source.impl.registrar :as reg]))

(defn dispatch-command
  [cmd-name data]
  (when-not (some? (reg/get-application))
    (throw (ex-info "Application must be started before dispatching commands"
                    {:command-name cmd-name})))
  (if-let [{:keys [spec] :as cmd-conf} (reg/get-command-conf cmd-name)]
    (do (when-not (s/valid? spec data)
          (throw (ex-info (str "Command data does not match spec for command " cmd-name)
                          (s/explain-data spec data))))
        (interceptor/execute-command! cmd-conf data))
    (throw (ex-info (str "No command registered for " cmd-name " did you `defcommand`?")
                    {:command-name cmd-name}))))
