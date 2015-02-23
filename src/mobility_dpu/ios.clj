(ns mobility-dpu.ios
  (:require [clj-time.core :as t]
            [monger.operators :as mo]
            [monger.query :as mq]
            [mobility-dpu.core :refer [Segmentor]]
            [mobility-dpu.temporal :refer [dt-parser]])
  )


(deftype iOSSegmentor [db coll]
  Segmentor
  (source-name [this] "iOS")
  (get-activity-dat
    [this user]
    (let [act-dat (mq/with-collection db coll
                                      (mq/find {"header.schema_id.name" "mobility-stream-iOS"
                                                :user_id user
                                                "body.activities" {mo/$exists true}})
                                      (mq/sort {"header.creation_date_time" 1})
                                      (mq/keywordize-fields true)
                                      )


          ]
      (for [dp act-dat]
        (let [timestamp (dt-parser
                          (get-in dp[:header :creation_date_time]))
              {:keys [activity confidence]} (first (get-in  dp [:body :activities])) ]
          {:timestamp timestamp
           :data {:activity [(keyword activity) (keyword confidence)]
                  :timestamp timestamp}
           }
          )
        )
      ))
  (get-location-dat [this user] "Return a list of all the raw location samples"
    (let [loc-dat  (mq/with-collection db coll
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


          ]
      ; some records use "horizontal_accuracy". change all of them to "accuracy"
      (map #(assoc-in % [:location :accuracy] (or (get-in % [:location :horizontal_accuracy])
                                                  (get-in % [:location :accuracy]))) loc-dat))
    )
  (transition-matrix [this] "The initial state transition matrix"
    {[:still :still] 0.9 [:on_foot :on_foot] 0.9 [:in_vehicle :in_vehicle] 0.9 [:on_bicycle :on_bicycle] 0.9
     [:still :on_foot]  0.09 [:still :in_vehicle] 0.005 [:still :on_bicycle] 0.005
     [:on_foot :still]  0.06 [:on_foot  :in_vehicle] 0.02 [:on_foot  :on_bicycle] 0.02
     [:in_vehicle :still]  0.005 [:in_vehicle :on_foot] 0.09 [:in_vehicle :on_bicycle] 0.005
     [:on_bicycle :still]  0.005 [:on_bicycle :on_foot] 0.09 [:on_bicycle :in_vehicle] 0.005})
  (emission-prob [this state observation]
    ; emission probability (i.e. prob of observing y given x=activity) where x is the real hidden state
    ; current implementation use
    ; 1) the sampled confidence if x == the sampled activity,
    ; 2) and (1-confidence) / 3 for the other cases
    (let [; map iOS activity names to canonical activity names
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
          [sensed-act conf] (:activity observation)
          sensed-act (activity-mapping sensed-act)
          conf (prob-mapping conf)]
      (if (= sensed-act state)
        conf
        (/ (- 1 conf) 3))
      )
    )
  (init-pi [this] "The initial prob of each state at time 0"
    {:still 0.91 :on_foot 0.03 :in_vehicle 0.03 :on_bicycle 0.03}
    )
  (post-process [this inferred-segments]
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
    )
  )
