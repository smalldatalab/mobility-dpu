(ns mobility-dpu.config
  (:require [cheshire.core :as json])
  )


(def default
  {
   ; mobility summary config
   :filter-walking-speed   9
   :n-meters-of-gait-speed 50
   :quantile-of-gait-speed 0.9
   :max-human-speed 7

   ; mongodb stuff
   :mongodb                nil
   :dbname                 "omh"

   ; where to write the log file
   :log-file               "/var/log/ohmage-dpu/mobility-dpu.log"

   ; the shim sync stuff
   :shim-endpoint          "http://localhost:8083"
   :sync-tasks             {:fitbit ["STEPS" "ACTIVITY"]}

   ; mobility datapoint version
   :mobility-datapoint-version "2.0"
   })


; parse the config.json file or use the default configuration
(def config
  (delay
    (merge
      default
      (if (.exists (clojure.java.io/as-file "config.json"))
        (json/parse-string (slurp "config.json") true)
        ))

    )
  )
