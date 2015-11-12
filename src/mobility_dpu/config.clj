(ns mobility-dpu.config
  (:require [cheshire.core :as json])
  )


(def default
  {:filter-walking-speed   9
   :n-meters-of-gait-speed 50
   :quantile-of-gait-speed 0.9
   :max-human-speed 7
   :shim-endpoint          "http://ohmage-shim:8084"
   :mongodb                nil
   :sync-tasks             {:fitbit ["STEPS" "ACTIVITY"]}
   :log-file               "/var/log/ohmage-dpu/clojure.log"
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
