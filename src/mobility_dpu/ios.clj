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
    ; Use both activity samples and moving speed derived from location samples to infer mobility status.
    (concat
      (for [datapoint (filter (comp :activities body) (query db "cornell" "mobility-stream-iOS" user))]

        (let [{:keys [activity confidence]} (first (:activities (body datapoint)))]
          (iOSActivitySample. (timestamp datapoint)
                              ((keyword activity) activity-mapping)
                              ((keyword confidence) prob-mapping)

                              )
          )
        )
      (let [loc-samples (sort-by timestamp (location-samples this))
            loc-samples (filter #(< (accuracy %) 80) loc-samples)
            millis-diff (fn [[l1 l2]] (t/in-millis (t/interval (timestamp l1) (timestamp l2))))
            paired-loc-samples (partition 2 1 loc-samples)
            paired-loc-samples (filter (fn [pair] (< 30000 (millis-diff pair) 180000)) paired-loc-samples)]
        (for [[l1 l2 :as pair] paired-loc-samples]
          (let [speed (* 1000 1000
                         (/ (spatial/haversine l1 l2)
                            (millis-diff pair))) ]
            (iOSActivitySample. (timestamp l2)
                                (cond
                                  (> speed 4) :in_vehicle
                                  (> speed 1.5) :on_foot
                                  :default :still
                                  )
                                 0.4
                                )
            )


          )

        )
      )

    )
  (location-samples [_]
    (for [datapoint (filter (comp :location body) (query db "cornell" "mobility-stream-iOS" user))]

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

