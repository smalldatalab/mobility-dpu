(ns mobility-dpu.android
  (:require [mobility-dpu.temporal :refer [dt-parser]]
            [schema.core :as s]
            [clj-time.core :as t]
            [mobility-dpu.mobility :as mobility]
            [clj-time.coerce :as c])
  (:use [mobility-dpu.protocols])
  (:import (mobility_dpu.protocols LocationSample)
           (org.joda.time DateTime)))
(defn normalize-activity-name [activity-name]
  (let [act (keyword (clojure.string/lower-case activity-name))]
    (if (= act :walking)
      :on_foot
      act))
  )
(defn datapoint->activity-prob-map [datapoint]
  (let [act-list (second (first (:body datapoint)))]
    (into {} (map #(vector (normalize-activity-name (:activity %))
                           (:confidence %)) act-list))
    )
  )


(s/defrecord AndroidActivitySample [timestamp :- (s/protocol t/DateTimeProtocol)
                                    activity-prob-map]
  ActivitySampleProtocol
  (prob-sample-given-state [_ state]
    "P(Hidden state = E | The observation)
          = The probability of state E reported by the Android + a portion of prob of Tilting & Unknown prob
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
  (user [_] user)
  (extract-episodes [_]
    (let [activity-samples (for [datapoint (concat (query db "io.smalldatalab" "mobility-android-activity-stream" user) ; Mobility after 3.0
                                                   (query db "omh" "mobility" user)) ; old Mobility data
                                 ]
                             (AndroidActivitySample.
                               (timestamp datapoint)
                               (datapoint->activity-prob-map datapoint)
                               )
                             )
          location-samples (for [datapoint (concat (query db "io.smalldatalab" "mobility-android-location-stream" user)
                                                   (query db "omh" "location" user))]
                             (LocationSample. (if-let [more-accurate-time (:timestamp (:body datapoint))]
                                                (DateTime. more-accurate-time (.getZone ^DateTime (timestamp datapoint)))
                                                (timestamp datapoint))
                                              (:latitude (:body datapoint))
                                              (:longitude (:body datapoint))
                                              (:accuracy (:body datapoint)))
                             )
          ]
      (mobility/mobility-extract-episodes activity-samples location-samples nil)
      )
    )

  ; Android does not support steps count

  (step-supported? [_]
    false)
  (last-update [_]
    (let [times (->>
                  [(last-time db "io.smalldatalab" "mobility-android-activity-stream" user) ; Mobility after 3.0
                   (last-time db "omh" "mobility" user)
                   (last-time db "io.smalldatalab" "mobility-android-location-stream" user)
                   (last-time db "omh" "location" user)]
                   (filter identity)
                  )

          ]
      (if (seq times)
        (->> times
             (map c/to-long)
             (apply max)
             (c/from-long)
             )

        )
      )

    )
  )

