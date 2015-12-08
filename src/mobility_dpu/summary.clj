(ns mobility-dpu.summary
  (:require
    [clj-time.core :as t]
    [mobility-dpu.spatial :as spatial]
    [taoensso.timbre :as timbre]
    [mobility-dpu.algorithms :as algorithms]

    [mobility-dpu.temporal :as temporal]
    [mobility-dpu.datapoint :as datapoint]
    [schema.core :as s])
  (:use [aprint.core]
        [mobility-dpu.config]
        [mobility-dpu.protocols]
        [mobility-dpu.process-episode]
        )
  )
;;;; The following functions are specific for EpisodeProtocol

(timbre/refer-timbre)


(defn- duration [%]
  (t/interval (:start %) (:end %))
  )


(s/defn active-time-in-seconds :- s/Num [episodes :- [EpisodeSchema]]
  (->> episodes
       (filter #(#{:on_foot :on_bicycle} (:inferred-state %)))
       (map duration)
       (map t/in-seconds)
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
       (filter #(or (> (count (:location-trace (:trace-data %))) 1) (:distance %)) )
       (map episode-distance)
       (apply + 0)
       )
  )

(s/defn longest-trek-in-km
  "Return the total (kallman-filtered) walking distance in the given segment in KM"
  [episodes :- [EpisodeSchema]]
  (->> episodes
       (filter #(= (:inferred-state %) :on_foot))
       (filter #(or (> (count (:location-trace (:trace-data %))) 1) (:distance %)) )
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
(defn- gait-speed [episodes]
  (algorithms/x-quantile-n-meter-gait-speed
    (->> episodes
         (filter #(= (:inferred-state %) :on_foot))
         (map  (comp :location-trace :trace-data))
         (filter seq)
         )
    (:quantile-of-gait-speed @config)
    (:n-meters-of-gait-speed @config))


  )


(s/defn segments [user :- s/Str device :- s/Str {:keys [episodes date zone]} :- DayEpisodeGroup]
  (datapoint/mobility-datapoint user device "segments"
                                date (or (:end (last episodes)) (temporal/to-last-millis-of-day date zone))
                                {:episodes episodes})

  )

(s/defn summarize [user :- s/Str device :- s/Str step-supported? :- s/Bool {:keys [episodes date zone]} :- DayEpisodeGroup]
  (datapoint/mobility-datapoint
    user device "summary"
    date (or (:end (last episodes)) (temporal/to-last-millis-of-day date zone))
    (merge (infer-home episodes)
           {:geodiameter_in_km                  (geodiameter episodes)
            :walking_distance_in_km             (walking-distance-in-km episodes)
            :active_time_in_seconds             (active-time-in-seconds episodes)
            :steps                              (if step-supported? (total-step-count episodes))
            :max_gait_speed_in_meter_per_second (gait-speed episodes)
            :gait_speed                         {:n_meters   (:n-meters-of-gait-speed @config)
                                                 :quantile   (:quantile-of-gait-speed @config)
                                                 :gait_speed (gait-speed episodes)
                                                 }
            :longest-trek-in-km                 (longest-trek-in-km episodes)
            :coverage                           (algorithms/coverage date zone episodes)
            :episodes                           (map #(dissoc % :raw-data :trace-data) episodes)
            })
  ))


(defn get-datapoints [source]
  (let [user (user source)
        device (source-name source)
        step-supported? (step-supported? source)
        episodes (-> (extract-episodes source)
                     (assoc-cluster 50 20)
                     (merge-still-epidoses))
        home-clusters (infer-home-clusters episodes)
        episodes (map #(cond-> %
                               (home-clusters (:cluster %))
                               (assoc :home? true)) episodes)
        ]
    (mapcat
      (fn [day-group]
        [(summarize user device step-supported? day-group)
         (segments user device day-group)
         ]
        )
      (group-by-day episodes)
      )

    )
  )