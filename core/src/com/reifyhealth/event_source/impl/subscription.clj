(ns com.reifyhealth.event-source.impl.subscription
  (:require [com.reifyhealth.event-source.impl.event-store :as es]
            [com.reifyhealth.event-source.impl.registrar :as reg]))

(defn- parse-sub-name
  [n]
  (cond (string? n)
        n

        (or (qualified-symbol? n)
            (qualified-keyword? n))
        (str (namespace n) "/" (name n))

        (or (symbol? n)
            (keyword? n))
        (name n)

        :else
        (str n)))

(defn start-subscriptions!
  []
  (doseq [{:keys [event-name subscriptions]} (reg/get-all-event-conf)]
    (doseq [{:keys [name start-from handler]} (flatten (vals subscriptions))]
      (es/subscribe
        (:event-store (reg/get-application))
        (parse-sub-name name)
        (fn [{:keys [event-type data]}]
          (when (= event-name event-type)
            (handler data)))
        {:start-from start-from}))))
