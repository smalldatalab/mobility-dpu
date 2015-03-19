(ns mobility-dpu.main
  (:gen-class)
  (:require [mobility-dpu.ios]
            [mobility-dpu.summary :as summary]
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


(def user "google:104731613567102516833")

(def db (mongodb "omh" "dataPoint"))

(doseq [user (users db)]
  (let [datapoints
        (concat
          (mobility/get-datapoints user (->AndroidUserDatasource user db))
          (mobility/get-datapoints user (->iOSUserDatasource user db))
          (moves/get-datapoints user)
          )]
    (doseq [dp datapoints]
      (save db dp)
      )
    )
  )








