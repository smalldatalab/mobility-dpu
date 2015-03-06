(ns mobility-dpu.main
  (:gen-class)
  (:require [monger.core :as mg]
            [monger.collection :as mc]
            [mobility-dpu.ios]
            [mobility-dpu.temporal :as temporal]
            [mobility-dpu.core :refer [segmentation]]
            [mobility-dpu.summary :as summary]
            [mobility-dpu.datapoint :as datapoint]
            [taoensso.timbre :as timbre]
            [mobility-dpu.android])
  (:import (mobility_dpu.android AndroidSegmentor)
           (mobility_dpu.ios iOSSegmentor)
           (mobility_dpu.core Segmentor)
           ))


(timbre/refer-timbre)
(def conn (mg/connect))
(def db (mg/get-db conn "omh"))
(def coll "dataPoint")
(def segmentors [(AndroidSegmentor. db coll) (iOSSegmentor. db coll)])

(defn -main
  "The application's main function. Run Android and iOS segmentor for each user's data."
  [& args]

  (loop []
    (let [; get all the users
          users (mc/distinct db coll "user_id" {})]
      ; run each segmentor over each user's data
      (doseq [^Segmentor segmentor segmentors user users]
        (info user segmentor)
        (try
          (let [source (.source-name segmentor)
                ; get segments
                segments (segmentation segmentor user)]
            ; group segments by day
            (doseq [[date daily-segments] (temporal/group-by-day segments)]
              (let [creation-date-time (:end (last daily-segments))
                    ; compute daily summary
                    summaries (summary/summarize date daily-segments)]
                ; save summary
                (mc/save db coll (datapoint/daily-summary-datapoint user  source date summaries creation-date-time))
                ; save daily segments
                (mc/save db coll (datapoint/daily-segments-datapoint user source date daily-segments creation-date-time))
                )
              )
            )
          (catch Exception e (error e)))
        )
      )
    (Thread/sleep 300000)
    (recur)

    )
  )




