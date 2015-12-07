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
        [mobility-dpu.protocols])
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
   (+
     (->
       (:location-trace (:trace-data episode))
       (spatial/kalman-filter (:filter-walking-speed @config) 3000)
       (spatial/trace-distance (:max-human-speed @config))
      )
     )
  )

(s/defn walking-distance-in-km :- s/Num
  "Return the total (kallman-filtered) walking distance in the given segment in KM"
  [episodes :- [EpisodeSchema]]
  (->> episodes
       (filter #(= (:inferred-state %) :on_foot))
       (filter #(> (count (:location-trace (:trace-data %))) 1) )
       (map episode-distance)
       (apply + 0)
       )
  )

(s/defn longest-trek-in-km
  "Return the total (kallman-filtered) walking distance in the given segment in KM"
  [episodes :- [EpisodeSchema]]
  (->> episodes
       (filter #(= (:inferred-state %) :on_foot))
       (filter #(> (count (:location-trace (:trace-data %))) 1) )
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


(defn segments [user device date zone daily-episodes]
  (datapoint/segments-datapoint {:user              user
                                 :device            device
                                 :date              date
                                 :creation-datetime (or (:end (last daily-episodes)) (temporal/to-last-millis-of-day date zone))
                                 :body              {:episodes daily-episodes}})
  )
(defn summarize [user device date zone daily-episodes step-supported?]
  (let []
    (datapoint/summary-datapoint
      {:user                           user
       :device                         device
       :date                           date
       :creation-datetime              (or (:end (last daily-episodes)) (temporal/to-last-millis-of-day date zone))
       :geodiameter-in-km              (geodiameter daily-episodes)
       :walking-distance-in-km         (walking-distance-in-km daily-episodes)
       :longest-trek-in-km             (longest-trek-in-km daily-episodes)
       :active-time-in-seconds         (active-time-in-seconds daily-episodes)
       :steps                          (if step-supported? (total-step-count daily-episodes))
       :gait-speed-in-meter-per-second (gait-speed daily-episodes)
       :leave-return-home              (infer-home daily-episodes)
       :coverage                       (algorithms/coverage date zone daily-episodes)
       }
      )
    )
  )