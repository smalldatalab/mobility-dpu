(ns mobility-dpu.summary
  (:require
    [clj-time.core :as t]
    [mobility-dpu.spatial :as spatial]
    [taoensso.timbre :as timbre]
    [mobility-dpu.algorithms :as algorithms]

    [mobility-dpu.temporal :as temporal]
    [mobility-dpu.datapoint :as datapoint]
    [schema.core :as s]
    [clj-time.coerce :as c])
  (:use [aprint.core]
        [mobility-dpu.config]
        [mobility-dpu.protocols]
        [mobility-dpu.process-episode]
        )
  )
;;;; The following functions are specific for EpisodeProtocol

(timbre/refer-timbre)



(s/defn duration :- s/Num [% :- EpisodeSchema]
  (or (:duration %) (t/in-seconds (t/interval (:start %) (:end %))))
  )


(s/defn active-time-in-seconds :- s/Num [episodes :- [EpisodeSchema]]
  (->> episodes
       (filter #(#{:on_foot :on_bicycle} (:inferred-state %)))
       (map duration)
       (apply + 0)
       )
  )

(defn episode-distance [episode]
  (or (:distance episode)
      (->
        (:location-trace (:trace-data episode))
        (spatial/kalman-filter (:filter-walking-speed @config) 3000)
        (spatial/trace-distance (:max-human-speed @config))
        ))
  )

(s/defn walking-distance-in-km :- s/Num
  "Return the total (kallman-filtered) walking distance in the given segment in KM"
  [episodes :- [EpisodeSchema]]
  (->> episodes
       (filter #(= (:inferred-state %) :on_foot))
       (filter #(or (:distance %) (> (count (:location-trace (:trace-data %))) 1)))
       (map episode-distance)
       (apply + 0)
       )
  )

(s/defn longest-trek-in-km
  "Return the total (kallman-filtered) walking distance in the given segment in KM"
  [episodes :- [EpisodeSchema]]
  (->> episodes
       (filter #(= (:inferred-state %) :on_foot))
       (filter #(or (:distance %) (> (count (:location-trace (:trace-data %))) 1)))
       (map episode-distance)
       (apply max 0)
       )
  )

(defn- total-step-count
  "Return the total number of step count in the given segments"
  [episodes]
  (apply + 0 (->> (mapcat #(:step-trace (:trace-data %)) episodes)
                  (map :step-count)))
  )
(defn- infer-home [still-episodes]
  (algorithms/infer-home still-episodes)

  )
(s/defn geodiameter :- s/Num [episodes :- [EpisodeSchema]]
  (algorithms/geodiameter-in-km (filter identity (map :cluster episodes)))
  )
(defn- gait-speed [episodes n-meter quantile]
  (algorithms/x-quantile-n-meter-gait-speed
    (->> episodes
         (filter #(= (:inferred-state %) :on_foot))
         (map (comp :location-trace :trace-data))
         (filter seq)
         )
    quantile
    n-meter)

  )

(s/defn mobility-datapoint :- MobilityDataPoint
  [user device type date zone creation-datetime body]
  (let [[major minor]
        (map #(Integer/parseInt %) (clojure.string/split (:mobility-datapoint-version @config) #"\."))]
    (datapoint/datapoint user
                         "cornell"                          ; namespace
                         (str "mobility-daily-" type)       ; schema name
                         major minor                        ; version
                         (clojure.string/lower-case device) ; souce
                         "SENSED"                           ; modality
                         date                               ; time for id
                         creation-datetime                  ; creation datetime
                         (assoc body
                           :date date
                           :zone (str zone)
                           :device (clojure.string/lower-case device)) ; body
                         ))
  )




(s/defn summarize :- SummaryDataPoint
  [user :- s/Str
   device :- s/Str
   step-supported? :- s/Bool
   hide-location? :- s/Bool
   {:keys [episodes date zone]} :- DayEpisodeGroup]
  (let [gait (gait-speed episodes (:n-meters-of-gait-speed @config) (:quantile-of-gait-speed @config))
        body (cond->
               {
                :home             (infer-home episodes)
                :geodiameter      {:unit "km", :value (geodiameter episodes)}
                :walking_distance {:unit "km", :value (walking-distance-in-km episodes)}
                :active_time      {:unit "sec" :value (active-time-in-seconds episodes)}
                :step_count       (if step-supported? (total-step-count episodes))
                :longest_trek     {:unit "km" :value (longest-trek-in-km episodes)}
                :coverage         (algorithms/coverage date zone episodes)
                :episodes         (map (fn [epi]
                                         (let [locations
                                               (map
                                                 #(vector (:latitude %) (:longitude %) (:accuracy %) (:timestamp %))
                                                 (-> epi
                                                     (:trace-data)
                                                     (:location-trace)))]
                                           (cond-> (dissoc epi :raw-data :trace-data)
                                                   (and (not= (:inferred-state epi) :still)
                                                        (not hide-location?))
                                                   (assoc :locations locations)
                                                   hide-location?
                                                   (dissoc :cluster)
                                                   )
                                           )
                                         ) episodes)

                }
               gait
               (assoc
                 :max_gait_speed {:unit "mps" :value gait}
                 :gait_speed {:n_meters   (:n-meters-of-gait-speed @config)
                              :quantile   (:quantile-of-gait-speed @config)
                              :gait_speed gait
                              }))
        body (assoc body
               ;; for backward-compaitability with the old format
               :geodiameter_in_km (:value (:geodiameter body))
               :walking_distance_in_km (:value (:walking_distance body))
               :active_time_in_seconds (:value (:active_time body))
               :steps (:step_count body)
               :max_gait_speed_in_meter_per_second (:value (:max_gait_speed body))
               :time_not_at_home_in_seconds (:value (:time_not_at_home (:home body)))
               :leave_home_time (:leave_home_time (:home body))
               :return_home_time (:return_home_time (:home body))
               )
        ]
    (mobility-datapoint
      user device "summary"
      date zone (or (:end (last episodes)) (temporal/to-last-millis-of-day date zone))
      body

      )))


(s/defn ^:always-validate get-datapoints :- [MobilityDataPoint]
  [source :- (s/protocol UserDataSourceProtocol)
   provided-home-location :- (s/maybe Location)
   & [hide-location? :- s/Bool]]
  (let [user (user source)
        device (source-name source)
        step-supported? (step-supported? source)
        ; extract episodes from raw data
        episodes (-> (extract-episodes source)
                     (assoc-cluster 50 20)
                     (merge-still-epidoses))
        home-clusters (infer-home-clusters episodes provided-home-location)
        episodes (map #(cond->
                        %
                        (home-clusters (:cluster %))
                        (assoc :home? true)) episodes)
        ]
    (mapcat
      (fn [day-group]
        [(p :summary (summarize user device step-supported? hide-location? day-group))
         ]
        )
      (group-by-day episodes)
      )

    )
  )


