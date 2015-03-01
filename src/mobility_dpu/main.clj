(ns mobility-dpu.main
  (:gen-class)
  (:require [monger.core :as mg]
            [monger.collection :as mc]
            [mobility-dpu.ios]
            [mobility-dpu.core :refer [segmentation segments->datapoints]]
            [taoensso.timbre :as timbre]
            [mobility-dpu.android])
  (:import (mobility_dpu.android AndroidSegmentor)
           (mobility_dpu.ios iOSSegmentor)
           ))


(timbre/refer-timbre)


(defn -main
  "The application's main function. Run Android and iOS segmentor for each user's data."
  [& args]

  (let [conn (mg/connect)
        db   (mg/get-db conn "omh")
        coll "dataPoint"]
    (loop []
      (let [
            ; segmentors for Android and iOS
            segmentors [(AndroidSegmentor. db coll) (iOSSegmentor. db coll)]
            ; get all the users
            users (mc/distinct db coll "user_id" {})
            ]
        ; run each segmentor over each user's data
        (doseq [segmentor segmentors user users]
          (info user segmentor)
          (try
            (let [segments (segmentation segmentor user)
                  datapoints (segments->datapoints segmentor user segments)]
              ; save (or replace) the generated data points to mongo db
              (doseq [dp datapoints]
                (mc/save-and-return db coll dp)
                )
              )
            (catch Exception e (error e)))
          )
        )
      (Thread/sleep 300000)
      (recur)

      )
    )
  )




