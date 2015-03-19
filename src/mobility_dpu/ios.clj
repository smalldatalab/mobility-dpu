(ns mobility-dpu.ios
  (:require [clj-time.core :as t]
            [mobility-dpu.temporal :refer [dt-parser]])
  (:use [mobility-dpu.protocols])
  (:import (mobility_dpu.protocols LocationSample)))


(def activity-mapping
  {:transport :in_vehicle
   :cycling :on_bicycle
   :cycle :on_bicycle
   :run :on_foot
   :walk :on_foot
   :still :still
   :unknown :unknown}
  ; map iOS probability to numerical value
)

(def prob-mapping
  {:low 0.5
   :high  0.99
   :medium 0.75
   })
(defrecord iOSActivitySample [timestamp sensed-act confidence]
  ActivitySampleProtocol
  (prob-sample-given-state [_ state]
    ; emission probability (i.e. prob of observing y given x=activity) where x is the real hidden state
    ; current implementation use
    ; 1) the sampled confidence if x == the sampled activity,
    ; 2) and (1-confidence) / 3 for the other cases
    (let []
      (if (= sensed-act state)
        confidence
        (/ (- 1 confidence) 3))
      )
    )
  TimestampedProtocol
  (timestamp [_] timestamp)
  )



(defrecord iOSUserDatasource [user  db]
  UserDataSourceProtocol
  (source-name [_] "iOS")
  (activity-samples [_]
    (for [datapoint (filter :activities (query db "cornell" "mobility-stream-iOS" user))]

      (let [{:keys [activity confidence]} (first (:activities (body datapoint)))]
        (iOSActivitySample. (timestamp datapoint)
                            ((keyword activity) activity-mapping)
                            ((keyword confidence) prob-mapping)
                            )
        )
      )
    )
  (location-samples [_]
    (for [datapoint (filter :location (query db "cornell" "mobility-stream-iOS" user))]

      (let [{:keys [accuracy horizontal_accuracy latitude longitude]} (:location (body datapoint)) ]
        (LocationSample. (timestamp datapoint)
                            latitude
                            longitude
                            (or accuracy horizontal_accuracy)
                            )
        )
      )
    )
  )

(defrecord iOSMergedEpisode [episodes]
  EpisodeProtocol
  (state [_] (state (first episodes)))
  (start [_] (start (first episodes)))
  (end [_] (end (last episodes)))
  (location-trace [_] (mapcat location-trace episodes))
  )

(comment
  (defn post-process [episodes]
    (loop [[next & rest] (rest episodes)
           cur-group [(first episodes)]
           ret [] ]
      (if head
        (if
          (and (= (state head) (state next) :still)
               (< (t/in-minutes (t/interval (end head) (start next))) 90))
          (recur
            rest-segs
            ret)
          (recur n-seg
                 rest-segs
                 (conj ret seg))
          )
        ret)
      ))
  )

