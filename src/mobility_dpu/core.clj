(ns mobility-dpu.core
  (:require [cheshire.core]
            [cheshire.custom]
            [taoensso.timbre :as timbre]
            [clj-time.coerce :as c]
            [clj-time.core :as t]
            [mobility-dpu.hmm :as hmm]
            [monger.core :as mg]
            [monger.collection :as mc]
            [monger.query :as mq]
            [monger.operators :as mo]
            )
  (:use [aprint.core])
  (:import (org.joda.time DateTime)
           (org.joda.time.format ISODateTimeFormat DateTimeFormatter DateTimeFormat)))


(timbre/refer-timbre)
 (defn dt-parser [dt]
    (let [try-parse #(try (DateTime/parse %1 (.withOffsetParsed ^DateTimeFormatter %2)) (catch IllegalArgumentException _ nil))]
      (or (try-parse dt (ISODateTimeFormat/dateTime))
          (try-parse dt (ISODateTimeFormat/dateTimeNoMillis))
          (try-parse dt (DateTimeFormat/forPattern "yyyy-MM-dd'T'HH:mmZZ"))
          )
      )
)


(defn android-mobility [dat]
      (let [; initial transition probability
            transition-matrix
               {[:still :still] 0.9 [:on_foot :on_foot] 0.9 [:in_vehicle :in_vehicle] 0.9 [:on_bicycle :on_bicycle] 0.9
               [:still :on_foot]  0.09 [:still :in_vehicle] 0.005 [:still :on_bicycle] 0.005
               [:on_foot :still]  0.06 [:on_foot  :in_vehicle] 0.02 [:on_foot  :on_bicycle] 0.02
               [:in_vehicle :still]  0.005 [:in_vehicle :on_foot] 0.09 [:in_vehicle :on_bicycle] 0.005
               [:on_bicycle :still]  0.005 [:on_bicycle :on_foot] 0.09 [:on_bicycle :in_vehicle] 0.005}
            ; emission probability (i.e. prob of observing y given x=activity) where x is the real (but unknown) state
            emission-prob
              (fn [act y]
                (let [tilting (/ (or (:tilting y) 0) 7.0)
                      unknown (/ (or (:unknown y) 0) 5.0)]
                  (/ (+ (if (= act :still)
                          (* 2 unknown) unknown)
                        (if (= act :still)
                          tilting (* 2 tilting))
                        (* (or (act y) 0) 0.99)
                        1)

                     100.0)
                  )

                )
            ; initial pi
            pi {:still 0.91 :on_foot 0.03 :in_vehicle 0.03 :on_bicycle 0.03}
            ; convert timestamp
            dat (map #(assoc % :timestamp (c/to-date-time (:timestamp %))) dat)
            ; convert to format that hmm uses
            dat (apply vector (map (fn [d]
                                   (assoc d :data
                                       (into {} (map #(vector (keyword (:activity %)) (:confidence %)) (:data d))))
                                   ) dat))
            segments (hmm/downsample-and-segment dat)]
        (hmm/to-inferred-segments segments transition-matrix emission-prob pi)
        ))




(comment
  (and (= sensed-act :unknown) (= speed nil))
  0.25
  (and (= sensed-act :unknown) (> speed 8))
  ({:still 0.01 :on_foot 0.19 :in_vehicle 0.70 :on_bicycle 0.10} hidden-act)
  (and (= sensed-act :unknown) (> speed 3))
  ({:still 0.01 :on_foot 0.24 :in_vehicle 0.60 :on_bicycle 0.15} hidden-act)
  (and (= sensed-act :unknown) (> speed 0.0))
  ({:still 0.01 :on_foot 0.34 :in_vehicle 0.40 :on_bicycle 0.25} hidden-act)
  (and (= sensed-act :unknown) (<= speed 0.0))
  ({:still 0.70 :on_foot 0.10 :in_vehicle 0.19 :on_bicycle 0.01} hidden-act))

(defn ios-mobility [dat locations]
    (let [_ (aprint (first locations))

          locations (map #(assoc-in % [:location :accuracy] (or (get-in % [:location :horizontal_accuracy])
                                                                (get-in % [:location :accuracy]))) locations)
          locations (filter #(< (get-in % [:location :accuracy]) 75) locations)
          dat
          (loop [[{:keys [timestamp] :as spl} & rest-spl] dat
                 locations locations
                 ret []]
            (if spl
              (let [[start end] [(t/minus timestamp (t/minutes 2)) timestamp]
                    locations (drop-while #(t/before? (:timestamp %) start) locations)
                    [within over] (split-with #(t/within? start end (:timestamp %)) locations)
                    max-speed (if (seq within)
                                (apply max (map (comp :speed :location) within))
                                nil)]
                (recur rest-spl over (conj ret (assoc spl :max-speed max-speed)))
                )
              ret)

            )
          dat (apply vector (map (fn [d]
                                   (let [{:keys [activity confidence]} (first (:activities d))]
                                     (assoc d :data
                                              {:activity [(keyword activity) (keyword confidence)]
                                               :max-speed (:max-speed d)}))

                                   ) dat))
          ; initial transition probability
          transition-matrix
          {[:still :still] 0.9 [:on_foot :on_foot] 0.9 [:in_vehicle :in_vehicle] 0.9 [:on_bicycle :on_bicycle] 0.9
           [:still :on_foot]  0.09 [:still :in_vehicle] 0.005 [:still :on_bicycle] 0.005
           [:on_foot :still]  0.06 [:on_foot  :in_vehicle] 0.02 [:on_foot  :on_bicycle] 0.02
           [:in_vehicle :still]  0.005 [:in_vehicle :on_foot] 0.09 [:in_vehicle :on_bicycle] 0.005
           [:on_bicycle :still]  0.005 [:on_bicycle :on_foot] 0.09 [:on_bicycle :in_vehicle] 0.005}
          activity-mapping
                  {:transport :in_vehicle
                   :cycling :on_bicycle
                   :cycle :on_bicycle
                   :run :on_foot
                   :walk :on_foot
                   :still :still
                   :unknown :unknown}
          prob-mapping
          {:low 0.5
           :high  0.99
           :medium 0.75
           }

; emission probability (i.e. prob of observing y given x=activity) where x is the real (but unknown) state
          emission-prob
          (fn [hidden-act y]
             (let [[sensed-act conf] (:activity y)
                   sensed-act (activity-mapping sensed-act)
                   conf (prob-mapping conf)
                   speed (:max-speed y)]
               (if (= sensed-act hidden-act)
                 conf
                 (/ (- 1 conf) 3))


               )
            )
          ; initial pi
          pi {:still 0.91 :on_foot 0.03 :in_vehicle 0.03 :on_bicycle 0.03}
          segments (hmm/downsample-and-segment dat)
          inferred-segments (hmm/to-inferred-segments segments transition-matrix emission-prob pi)
          inferred-segments
          ; merge two consecutive still segment if they are no more than 1 hours away
            (loop [seg (first inferred-segments)
                   [n-seg & rest-segs] (rest inferred-segments)
                   ret [] ]
              (if seg
                (if
                  (and (= (:inferred-activity seg) (:inferred-activity n-seg) :still)
                       (< (t/in-minutes (t/interval (:end seg) (:start n-seg))) 90))
                  (recur {:start (:start seg)
                          :end (:end n-seg)
                          :data (concat (:data seg) (:data n-seg))
                          :inferred-activity :still}
                         rest-segs
                         ret)
                  (recur n-seg
                         rest-segs
                         (conj ret seg))
                  )
                ret)
              )
          act-loc (hmm/merge-segments-with-location inferred-segments locations)
          act-loc (map (fn [{:keys [locations] :as %}]
                         (cond-> %
                                 (and (seq locations) (not (= (:inferred-activity %) :still)))
                                 (assoc :locations (hmm/kalman-filter locations 4))
                                 )) act-loc)
          act-loc (filter #(> (:duration-in-seconds %) 0) act-loc)
          ]
      (map (fn [{:keys [displacement-speed inferred-activity duration-in-seconds] :as seg}]
             ; deal with subway cases
             (if (and (= inferred-activity :still)
                      displacement-speed
                      (>= displacement-speed 1)
                      (> duration-in-seconds 120)
                      )
               (assoc seg :inferred-activity :in_vehicle)
               seg)
             ) act-loc)
      )
)

(let [conn (mg/connect)
      db   (mg/get-db conn "omh")
      coll "dataPoint"
      ; get users with ios mobility data
      users (mc/distinct db "dataPoint"
                         "user_id"
                         {"header.schema_id.name" "mobility-stream-iOS"}
                         )]
  ; for each user
  (doseq [user users]
    ; prepare data
    (let [act-dat (mq/with-collection db coll
                                  (mq/find {"header.schema_id.name" "mobility-stream-iOS"
                                            :user_id user
                                            "body.activities" {mo/$exists true}})
                                  (mq/sort {"header.creation_date_time" 1})
                                  (mq/keywordize-fields true)
                            )
          act-dat (for [dp act-dat]
                {:timestamp (dt-parser
                              (get-in dp[:header :creation_date_time]))
                 :activities (get-in  dp [:body :activities])})
          loc-dat  (mq/with-collection db coll
                                       (mq/find {"header.schema_id.name" "mobility-stream-iOS"
                                                 :user_id user
                                                 "body.location" {mo/$exists true}})
                                       (mq/sort {"header.creation_date_time" 1})
                                       (mq/keywordize-fields true)
                                       )
          loc-dat (for [dp loc-dat]
                    {:timestamp (dt-parser
                                  (get-in dp [:header :creation_date_time])
                                  )
                     :location (get-in  dp [:body :location])})

          segments (ios-mobility (sort-by :timestamp act-dat)
                                 (sort-by :timestamp loc-dat))

          ]

          (spit (str user ".json") (cheshire.custom/generate-string segments))
      )


    )

  )

