(ns mobility-dpu.android
  (:require [monger.query :as mq]
            [mobility-dpu.core :refer [Segmentor]]
            [mobility-dpu.temporal :refer [dt-parser]]))

(deftype AndroidSegmentor [db coll]
  Segmentor
  (source-name [this] "Android")
  (get-activity-dat [this user]
    (let [act-dat (mq/with-collection db coll
                                      (mq/find {"header.schema_id.name" "mobility"
                                                :user_id user})
                                      (mq/keywordize-fields true))
          nomalize-act (fn [act] (let [act (keyword (clojure.string/lower-case act))]
                                   (if (= act :walking)
                                     :on_foot
                                     act)))

          ]
      (for [dp act-dat]
        (let [timestamp (dt-parser
                          (get-in dp[:header :creation_date_time]))
              data (into {} (map #(vector (nomalize-act (:activity %)) (:confidence %)) (second (first (get-in  dp [:body])))))]
          {:timestamp  timestamp
           :data (assoc data :timestamp timestamp)}))
      )
    )
  (get-location-dat [this user]
    (let [loc-dat  (mq/with-collection db coll
                                       (mq/find {"header.schema_id.name" "location"
                                                 :user_id user})
                                       (mq/keywordize-fields true)
                                       )]
      (for [dp loc-dat]
        {:timestamp (dt-parser (get-in dp [:header :creation_date_time]))
         :location (get-in  dp [:body])})
      ))
  (transition-matrix [this]
    {[:still :still] 0.9 [:on_foot :on_foot] 0.9 [:in_vehicle :in_vehicle] 0.9 [:on_bicycle :on_bicycle] 0.9
     [:still :on_foot]  0.09 [:still :in_vehicle] 0.005 [:still :on_bicycle] 0.005
     [:on_foot :still]  0.06 [:on_foot  :in_vehicle] 0.02 [:on_foot  :on_bicycle] 0.02
     [:in_vehicle :still]  0.005 [:in_vehicle :on_foot] 0.09 [:in_vehicle :on_bicycle] 0.005
     [:on_bicycle :still]  0.005 [:on_bicycle :on_foot] 0.09 [:on_bicycle :in_vehicle] 0.005})
  (emission-prob [this hidden obs]
    "Each hidden state E's prob = prob of E given in the observation + a portion of prob of Tilting & Unknown prob
      Still state will get larger portion from Unknown prob, but smaller portion from Tilting prob"
    (let [tilting (/ (or (:tilting obs) 0) 7.0)
          unknown (/ (or (:unknown obs) 0) 5.0)]
      (/ (+ (if (= hidden :still)
              (* 2 unknown) unknown)
            (if (= hidden :still)
              tilting (* 2 tilting))
            (* (or (hidden obs) 0) 0.99)
            1)

         100.0)
      ))
  (init-pi [this]
    {:still 0.91 :on_foot 0.03 :in_vehicle 0.03 :on_bicycle 0.03})
  (post-process [this segments]
    segments
    )
  )

