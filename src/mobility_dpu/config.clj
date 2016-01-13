(ns mobility-dpu.config
  (:require
    [environ.core :refer [env]]
    [aprint.core :refer [aprint]]
    [taoensso.timbre :as timbre])
  )

(timbre/refer-timbre)



(def default
  {
   ; mobility summary config
   :filter-walking-speed   9
   :n-meters-of-gait-speed 50
   :quantile-of-gait-speed 0.9
   :max-human-speed 7

   ; mongodb stuff
   :mongodb-uri                "mongodb://127.0.0.1/omh"

   ; where to write the log file
   :log-file               "/var/log/ohmage-dpu/clojure.log"

   ; the shim sync stuff
   :shim-endpoint          "http://ohmage-shim:8084"
   :sync-tasks             {:fitbit ["STEPS" "ACTIVITY"]}

   ; mobility datapoint version
   :mobility-datapoint-version "2.0"

   ; gmap api key
   :gmap-geo-coding-server-key "YOUR_GMAP_API_KEY"
   })

(defn assoc-env [config env]
  (cond-> config
      (env :mongodb-uri)
      (assoc :mongodb-uri (env :mongodb-uri))
      (env :log-file)
      (assoc :log-file (env :log-file))
      (env :gmap-geo-coding-server-key)
      (assoc :gmap-geo-coding-server-key (env :gmap-geo-coding-server-key))
      (env :shim-endpoint)
      (assoc :shim-endpoint (env :shim-endpoint))
      (env :sync-tasks)
      (assoc :sync-tasks
             (->> (clojure.string/split (env :sync-tasks) #",")
                  (map #(clojure.string/split % #":") )
                  (map (fn [[provider type]] {(keyword provider) [(clojure.string/upper-case type)]}))
                  (apply merge-with concat)
                  ))
      )
  )

; parse the environment variable or use the default configuration
(def config
  (delay
    (let [config (-> default
                     (assoc-env env))]
      config
      )

    )
  )
