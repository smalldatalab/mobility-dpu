(ns mobility-dpu.ios
  (:require [clj-time.core :as t]
            [mobility-dpu.temporal :refer [dt-parser]]
            [mobility-dpu.spatial :as spatial])
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
    {:pre [(and sensed-act confidence)]}
    ; 1) and if state == the sensed activity state, use the sample confidence
    ; 2) otherwise, prob = (1-confidence) / 3 for the other three states
    (if (= sensed-act state)
      confidence
      (/ (- 1 confidence) 3)))
  TimestampedProtocol
  (timestamp [_] timestamp)
  )



(defrecord iOSUserDatasource [user  db]
  UserDataSourceProtocol
  (source-name [_] "iOS")
  (activity-samples [this]
    (let [dp->sample
          (fn [dp] "convert a raw data point dp to activity sample record"
            (let [{:keys [activity confidence]} (first (:activities (body dp)))]
              (iOSActivitySample.
                (timestamp dp)
                (activity-mapping (keyword activity))
                (prob-mapping (keyword confidence)))))
          ; convert all data points
          samples(->> (query db "cornell" "mobility-stream-iOS" user)
                       (filter (comp :activities body))
                       (map dp->sample)
                       (sort-by timestamp))]

      ; FIXME there should be a more computation efficient way to address this issue ....
      ; Fill in "STILL gaps"
      ; iOS would only return an activity sample every 2 hours if the user is being still for a long time.
      ; Therefore. one need to fill in the gap between two "STILL" samples here if the gap is no more than 2-hour long.
      (flatten
        (for [[cur next] (partition 2 1 samples)]
          (if (and (= (:sensed-act cur) (:sensed-act next) :still)
                   (< (t/in-hours (t/interval (timestamp cur) (timestamp next))) 2))
            ; iteratively fill the gap by replicating the current sample with +6minutes offset util the gap is filled
            (let [timeseries (iterate #(assoc % :timestamp (t/plus (timestamp %) (t/minutes 6))) cur)
                  filler (take-while #(t/before? (timestamp %) (timestamp next)) timeseries)]
              (cons cur filler))
            [cur]
            )
          ))

      )
    )
  (location-samples [_]

    (for [datapoint (filter (comp :location body) (query db "cornell" "mobility-stream-iOS" user))]

      (let [{:keys [accuracy horizontal_accuracy latitude longitude]} (:location (body datapoint)) ]
        (LocationSample. (timestamp datapoint)
                            latitude
                            longitude
                            (or accuracy horizontal_accuracy))
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

