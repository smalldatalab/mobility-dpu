(ns mobility-dpu.ios
  (:require [clj-time.core :as t]
            [mobility-dpu.temporal :refer [dt-parser]]
            [mobility-dpu.spatial :as spatial]
            [clj-time.coerce :as c])
  (:use [mobility-dpu.protocols])
  (:import (mobility_dpu.protocols LocationSample StepSample)
           (org.joda.time DateTime)))


(def activity-mapping
  {:transport :in_vehicle
   :cycling   :on_bicycle
   :cycle     :on_bicycle
   :run       :on_foot
   :walk      :on_foot
   :still     :still
   :unknown   :unknown}

  )

; map iOS probability to numerical value
(def prob-mapping
  {:low    0.5
   :high   0.99
   :medium 0.75
   })
(defrecord iOSActivitySample [timestamp sensed-act confidence]
  ActivitySampleProtocol
  (prob-sample-given-state [_ state]
    ; Return P(HiddenState=state/Observation)
    {:pre [(and sensed-act confidence)]}
    ; 1) and if state == the sensed activity state, use the sample confidence
    ; 2) otherwise, prob = (1-confidence) / 3 for the other three states
    (cond
      (= sensed-act state)
        confidence
      (= sensed-act :unknown)
        0.25
      :default
        (/ (- 1 confidence) 3)

      ))
  TimestampedProtocol
  (timestamp [_] timestamp)
  )



(defrecord iOSUserDatasource [user db]
  UserDataSourceProtocol
  (source-name [_] "iOS")
  (activity-samples [_]
    (let [dp->sample
          (fn [dp] "convert a raw data point dp to activity sample record"
            (let [{:keys [activity confidence]} (first (:activities (body dp)))]
              (iOSActivitySample.
                (timestamp dp)
                (activity-mapping (keyword activity))
                (prob-mapping (keyword confidence)))))

          ; convert all activity samples to data points
          samples (->> (query db "cornell" "mobility-stream-iOS" user)
                       (filter (comp :activities body))
                       (map dp->sample)
                       )

          location-derived-samples
                    (->>
                       (query db "cornell" "mobility-stream-iOS" user)
                       (filter (comp :location body))
                       (map (fn [dp]
                              (iOSActivitySample.
                                (timestamp dp)
                                :unknown
                                0.99)
                              ))
                               )
          samples (->> (concat samples location-derived-samples)
                       (sort-by timestamp))
          ]

      ; FIXME there should be a more computation efficient way to address this issue ....
      ; Fill in "STILL gaps" with dummy STILL data points.
      ; iOS only returns an activity sample every 4 hours or longer
      ; if the user is being still for an extended period of time.
      ; Therefore, here we fill in the gap between two "STILL" samples
      ; with dummy STILL samples as long as the gap is no more than 4-hour long.
      (flatten
        (for [[cur next] (partition 2 1 samples)]
          (if (and (= (:sensed-act cur) (:sensed-act next) :still)
                   (< (t/in-hours (t/interval (timestamp cur) (timestamp next))) 4))
            ; iteratively fill the gap by replicating the current sample with +6minutes offset util the gap is filled
            (let [timeseries (iterate #(assoc % :timestamp (t/plus (timestamp %) (t/minutes 4))) cur)
                  filler (take-while #(t/before? (timestamp %) (timestamp next)) timeseries)]
              (cons cur filler))
            [cur]
            )
          ))

      )
    )
  (location-samples [_]
    (for [datapoint (filter (comp :location body) (query db "cornell" "mobility-stream-iOS" user))]
      (let [{:keys [accuracy horizontal_accuracy latitude longitude]} (:location (body datapoint))]
        (LocationSample. (timestamp datapoint)
                         latitude
                         longitude
                         (or accuracy horizontal_accuracy))
        )
      )
    )
  (steps-samples [_]
    (for [datapoint (filter (comp :step_count :pedometer_data body) (query db "cornell" "mobility-stream-iOS" user))]
      (let [zone (.getZone ^DateTime (timestamp datapoint))
            {:keys [end_date floors_ascended floors_descended step_count start_date distance]} (:pedometer_data (body datapoint))]
        (StepSample.
          (.withZone (c/from-string start_date) zone)
          (.withZone (c/from-string end_date) zone)
          step_count
          )
        )
      )

    )
  (step-supported? [_]
    true)
  (raw-data [_]
    (concat (query db "cornell" "mobility-stream-iOS" user))
    )
  )

(defrecord iOSMergedEpisode [episodes]
  EpisodeProtocol
  (state [_] (state (first episodes)))
  (start [_] (start (first episodes)))
  (end [_] (end (last episodes)))
  (location-trace [_] (mapcat location-trace episodes))
  )

