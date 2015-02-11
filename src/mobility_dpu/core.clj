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
            [mobility-dpu.spatial :as spatial]
            [mobility-dpu.summary :as summary]
            monger.joda-time
            )
  (:use [aprint.core])
  (:import (org.joda.time DateTime ReadableDateTime LocalDate ReadablePartial ReadableInstant)
           (org.joda.time.format ISODateTimeFormat DateTimeFormatter DateTimeFormat)))



(timbre/refer-timbre)

; set encoder for JodaDateTime
(cheshire.custom/add-encoder DateTime
                             (fn [c jsonGenerator]
                               (.writeString jsonGenerator (str c))))
; date time parser that try all the possible formats
 (defn dt-parser [dt]
    (let [try-parse #(try (DateTime/parse %1 (.withOffsetParsed ^DateTimeFormatter %2)) (catch IllegalArgumentException _ nil))]
      (or (try-parse dt (ISODateTimeFormat/dateTime))
          (try-parse dt (ISODateTimeFormat/dateTimeNoMillis))
          (try-parse dt (DateTimeFormat/forPattern "yyyy-MM-dd'T'HH:mmZZ"))
          )
      )
)

(defn recursive-time->str
  "Covert all the Joda time objects contained in the given map to str"
  [m]
  (cond
    (map? m)
      (reduce (fn [m key]
                (assoc m key (recursive-time->str (get m key)))
                ) m (keys m))
    (or (list? m) (vector? m))
      (map recursive-time->str m)
    (or (instance? ReadableInstant m) (instance? ReadablePartial m))
    (str m)
    :else
    m
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

(defn ios-mobility [dat locations]
    (let [_ (aprint (first locations))
          ; some records use "horizontal_accuracy". change all of them to "accuracy"
          locations (map #(assoc-in % [:location :accuracy] (or (get-in % [:location :horizontal_accuracy])
                                                                (get-in % [:location :accuracy]))) locations)
          ; remove the location samples that have low accuracy
          locations (filter #(< (get-in % [:location :accuracy]) 75) locations)
          ; compute the max-gps-speed since two minute ago for each activity sample. (this feature is not used.)
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
          ; map iOS activity names to canonical activity names
          activity-mapping
                  {:transport :in_vehicle
                   :cycling :on_bicycle
                   :cycle :on_bicycle
                   :run :on_foot
                   :walk :on_foot
                   :still :still
                   :unknown :unknown}
          ; map iOS probability to numerical value
          prob-mapping
          {:low 0.5
           :high  0.99
           :medium 0.75
           }

          ; emission probability (i.e. prob of observing y given x=activity) where x is the real hidden state
          ; current implementation use
          ; 1) the sampled confidence if x == the sampled activity,
          ; 2) and (1-confidence) / 3 for other activities
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
          act-loc (filter #(> (:duration-in-seconds %) 0) act-loc)
          ]
      ; FIXME not sure if the following function is needed anymore.
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
  (for [user users]
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

          summaries (summary/summarize segments)
          summaries-datapoints (map #(summary/summary->datapoint % user "iOS") summaries)
          ]
         (when (seq summaries-datapoints)
           (let [removed (mc/remove db coll {:user_id user "header.schema_id.name" (-> summaries-datapoints
                                                                                       first
                                                                                       (get-in ["header" "schema_id" "name"]))})
                 inserted (mc/insert-batch db coll (recursive-time->str summaries-datapoints))]
             (aprint summaries-datapoints)
             (info "Remove" removed "Insert" inserted))
           )
      )
    )
  )

