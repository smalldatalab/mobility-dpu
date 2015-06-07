(ns mobility-dpu.summary
  (:require
    [clj-time.core :as t]
    [mobility-dpu.spatial :as spatial]
    [taoensso.timbre :as timbre]
    [mobility-dpu.algorithms :as algorithms]

    [mobility-dpu.temporal :as temporal]
    [mobility-dpu.datapoint :as datapoint])
  (:use [aprint.core]
        [mobility-dpu.config]
        [mobility-dpu.protocols])
  )
;;;; The following functions are specific for EpisodeProtocol

(timbre/refer-timbre)


(defn- duration [%]
  (t/interval (:start %) (:end %))
  )

(defn- place [episode]
  {:pre [(= (state episode) :still)]}
  (spatial/median-location (location-trace episode))
  )
(defn- active-time-in-seconds [on-foot-episodes]
  (apply + (->> on-foot-episodes
                (map (comp t/in-seconds duration))
                ))
  )

(defn- walking-distance-in-km
  "Return the total (kallman-filtered) walking distance in the given segments"
  [on-foot-episodes]
  (let [segs (filter #(> (count (location-trace %)) 1) on-foot-episodes)
        filtered-traces (map #(spatial/kalman-filter (location-trace %) (:filter-walking-speed @config) 30000) segs)]
    (apply + 0 (map spatial/trace-distance filtered-traces))
    )
  )

(defn- total-step-count
  "Return the total number of step count in the given segments"
  [episodes]
  (apply + 0 (->> (mapcat step-trace episodes)
                  (map step-count)))
  )
(defn- infer-home [still-episodes]
  (let [epis (filter :location
                     (for [epi still-episodes]
                       {:start    (start epi)
                        :end      (end epi)
                        :location (place epi)}
                       ))]
    (if-let [epis (seq epis)]
      (algorithms/infer-home epis nil)
      )
    )

  )
(defn- geodiameter [still-episodes]
  (algorithms/geodiameter-in-km (filter identity (map place still-episodes)))
  )
(defn- gait-speed [on-foot-episodes]
  (algorithms/x-quantile-n-meter-gait-speed
    (filter seq (map location-trace on-foot-episodes))
    (:quantile-of-gait-speed @config)
    (:n-meters-of-gait-speed @config))
  )


(defn segments [user device date zone daily-episodes]
  (datapoint/segments-datapoint {:user              user
                                 :device            device
                                 :date              date
                                 :creation-datetime (or (end (last daily-episodes)) (temporal/to-last-millis-of-day date zone))
                                 :body              {:episodes daily-episodes}})
  )
(defn summarize [user device date zone daily-episodes step-supported?]
  (let [state-groups (group-by state daily-episodes)
        on-foot-episdoes (:on_foot state-groups)
        still-episodes (:still state-groups)
        ]
    (datapoint/summary-datapoint
      {:user                           user
       :device                         device
       :date                           date
       :creation-datetime              (or (end (last daily-episodes)) (temporal/to-last-millis-of-day date zone))
       :geodiameter-in-km              (geodiameter still-episodes)
       :walking-distance-in-km         (walking-distance-in-km on-foot-episdoes)
       :active-time-in-seconds         (active-time-in-seconds on-foot-episdoes)
       :steps                          (if step-supported? (total-step-count daily-episodes))
       :gait-speed-in-meter-per-second (gait-speed on-foot-episdoes)
       :leave-return-home              (infer-home still-episodes)
       :coverage                       (algorithms/coverage date zone daily-episodes)
       }
      )
    )
  )