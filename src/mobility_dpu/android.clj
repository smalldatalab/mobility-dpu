(ns mobility-dpu.android
  (:require [mobility-dpu.temporal :refer [dt-parser]]
            [schema.core :as s]
            [clj-time.core :as t]
            [mobility-dpu.mobility :as mobility]
            [clj-time.coerce :as c])
  (:use [mobility-dpu.protocols])
  (:import (mobility_dpu.protocols LocationSample)
           (org.joda.time DateTime DateTimeZone)))
(defn normalize-activity-name [activity-name]
  (keyword (clojure.string/lower-case activity-name))
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
    (let [unknown (/ (or (:unknown activity-prob-map) 0) 5.0)]
      (/ (+ (if (= state :still)
              (* 2 unknown) unknown)
            (* (or (state activity-prob-map) 0) 0.99)
            1)
         100.0)
      ))
  TimestampedProtocol
  (timestamp [_] timestamp)
  )




(defrecord AndroidUserDatasource [user db]
  UserDataSourceProtocol
  (database [_] db)
  (source-name [_] "Android")
  (user [_] user)
  (extract-episodes [this]
    (let [activity-samples
          (for [datapoint (query db "io.smalldatalab" "mobility-android-activity-stream" user)
                ]
            (AndroidActivitySample.
              (timestamp datapoint)
              (datapoint->activity-prob-map datapoint)
              )
            )

          location-samples
          (for [datapoint (query db "io.smalldatalab" "mobility-android-location-stream" user)]
            (LocationSample. (if-let [more-accurate-time (:timestamp (:body datapoint))]
                               (DateTime. more-accurate-time (.getZone ^DateTime (timestamp datapoint)))
                               (timestamp datapoint))
                             (:latitude (:body datapoint))
                             (:longitude (:body datapoint))
                             (:accuracy (:body datapoint)))
            )
          step-samples
          (for [datapoint (query db "io.smalldatalab" "mobility-android-step-stream" user)]
            (let [z ^DateTimeZone  (.getZone (timestamp datapoint))
                  {:keys [start end steps]} (:body datapoint)]
              (->StepSample (DateTime. start z)
                            (DateTime. end z)
                            steps
                            )
              )

            )
          offload-fn
          (fn [until]
            (offload-data db "io.smalldatalab" {:$regex "^mobility-android-" } user until)
            )
          ]
      (mobility/mobility-extract-episodes this offload-fn activity-samples location-samples step-samples)
      )
    )
  (step-supported? [_]
    true)
  (last-update [_]
    (or (last-time db "io.smalldatalab"  {:$regex "^mobility-android-" } user) (c/from-long 0))

    )
  (purge-raw-trace [_ until]
    (remove-until db "io.smalldatalab" {:$regex "^mobility-android-" } user until)
    )
  )

