(ns mobility-dpu.main
  (:gen-class)
  (:require [mobility-dpu.ios]
            [taoensso.timbre :as timbre]
            [mobility-dpu.android]
            [mobility-dpu.mobility :as mobility]
            [mobility-dpu.temporal :refer [dt-parser]]
            [mobility-dpu.moves-sync :as moves])
  (:use [mobility-dpu.protocols]
        [mobility-dpu.android]
        [mobility-dpu.ios]
        [mobility-dpu.database]
        [aprint.core])

  )


(timbre/refer-timbre)
(def db (mongodb "omh" "dataPoint"))

(defn -main
  "The application's main function"
  [& args]
  (loop []
    (doseq [user (users db)
            source-fn [#(mobility/get-datapoints % (->AndroidUserDatasource % db))
                       #(mobility/get-datapoints % (->iOSUserDatasource % db))
                       #(moves/get-datapoints %)]]

      (try (doseq [datapoint (source-fn user)]
             (info "Save data for " user " " (get-in datapoint [:body :date]) " " (get-in datapoint [:body :device]))
             (save db datapoint))
           (catch Exception e (error e)))
      )
    (Thread/sleep 60000)
    (recur)
    )

  )








