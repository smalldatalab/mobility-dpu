(ns mobility-dpu.android
  (:require [mobility-dpu.temporal :refer [dt-parser]])
  (:use [mobility-dpu.protocols])
  (:import (mobility_dpu.protocols LocationSample)))
(defn normalize-activity-name [activity-name]
  (let [act (keyword (clojure.string/lower-case activity-name))]
    (if (= act :walking)
      :on_foot
      act))
  )
(defn datapoint->activity-prob-map [datapoint]
  (let [act-list (second (first (body datapoint)))]
    (into {} (map #(vector (normalize-activity-name (:activity %))
                           (:confidence %)) act-list))
    )
  )
(defrecord AndroidActivitySample [timestamp activity-prob-map]
  ActivitySampleProtocol
  (prob-sample-given-state [_ state]
    "Each hidden state E's prob = prob of E given in the observation + a portion of prob of Tilting & Unknown prob
      Still state will get larger portion from Unknown prob, but smaller portion from Tilting prob"
    (let [tilting (/ (or (:tilting activity-prob-map) 0) 7.0)
          unknown (/ (or (:unknown activity-prob-map) 0) 5.0)]
      (/ (+ (if (= state :still)
              (* 2 unknown) unknown)
            (if (= state :still)
              tilting (* 2 tilting))
            (* (or (state activity-prob-map) 0) 0.99)
            1)
         100.0)
      ))
  TimestampedProtocol
  (timestamp [_] timestamp)
  )
(defrecord AndroidUserDatasource [user db]
  UserDataSourceProtocol
  (source-name [_] "Android")
  (activity-samples [_]
    (for [datapoint (query db "omh" "mobility" user)]
        (AndroidActivitySample. (timestamp datapoint)
                                (datapoint->activity-prob-map datapoint))

      )
  )
  (location-samples [_]
    (for [datapoint (query db "omh" "location" user)]
      (LocationSample. (timestamp datapoint)
                      (:latitude (body datapoint))
                      (:longitude (body datapoint))
                      (:accuracy (body datapoint)))
      )
    )
  )

