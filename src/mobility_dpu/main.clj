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

; config logger
(timbre/refer-timbre)
(timbre/set-config! [:appenders :spit :enabled?] true)

(def db (mongodb "omh" "dataPoint"))

(defn -main
  "The application's main function"
  [& args]
  (loop []
    (doseq [; run dpu for specifc users (if args are set) or all the users in the db
            user (or (seq args) (users db))
            ; functions to generate datapoints from different sources: Android, iOS, and Moves App
            source-fn [#(mobility/get-datapoints % (->AndroidUserDatasource % db))
                       #(mobility/get-datapoints % (->iOSUserDatasource % db))
                       #(moves/get-datapoints %)]]
      (try (doseq [datapoint (source-fn user)]
             (info "Save data for " user " "
                   (get-in datapoint [:body :date]) " "
                   (get-in datapoint [:body :device]))
             (save db datapoint))
           (catch Exception e (error e)))
      )
    (Thread/sleep 60000)
    (recur)
    )

  )








