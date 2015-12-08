(ns mobility-dpu.algorithms
  (:require
    [clj-time.core :as t]
    [mobility-dpu.spatial :as spatial]
    [taoensso.timbre :as timbre]
    [mobility-dpu.temporal :as temporal]
    [schema.core :as s]
    [clj-time.coerce :as c])
  (:use [aprint.core]
        [mobility-dpu.config]
        [mobility-dpu.protocols ])
  (:import (org.joda.time LocalDate Interval DateTime)
           ))

(timbre/refer-timbre)

(s/defn infer-home
  "Divide the episodes sequence into 5 segemnts:

  first, scan from the begining.
  1) before getting home at midnight
  2) before leave home at morning

  and then scan reversely from the end
  3) after leaving home again at night (if ever)
  4) after return home after 2)

  5) the episode that between 2) and 4)

  In the most common case, the segment 5) should be not be empty and repsent the time the person stays in the work place.
  If the user never returns home after he left home at the morning, the segment 4) will be empty.
  If the user return home at the midnight, and never left again, the segments 3) 4) 5) will all be empty.
  "
  [episodes :- [EpisodeSchema]]
  (let [place-episodes (filter #(= (:inferred-state %) :still) episodes)
        [before-get-home-at-midnight after-get-home]
        (split-with (complement :home?) place-episodes)
        [befroe-leave-home after-leave-home]
        (split-with :home? after-get-home)
        [after-leave-home-at-night before-leave-home-at-night]
        (->> (split-with  (complement :home?) (reverse after-leave-home))
             (map reverse))
        [after-return-home time-between-leave-and-return]
        (->> (split-with  :home? (reverse before-leave-home-at-night))
             (map reverse))
        time-before-get-home-at-midnight (if (seq before-get-home-at-midnight)
                                           (t/in-seconds
                                             (t/interval  (.withTimeAtStartOfDay ^DateTime (:end (last before-get-home-at-midnight)))
                                                          (:end (last before-get-home-at-midnight))))
                                           0)
        time-after-leaving-home-at-night  (if (seq after-leave-home-at-night)
                                            (t/in-seconds
                                              (t/interval (:start (first after-leave-home-at-night))
                                                          (-> (first after-leave-home-at-night)
                                                              :start
                                                              (t/plus (t/days 1))
                                                              (.withTimeAtStartOfDay)
                                                              (t/minus (t/millis 1))
                                                              )
                                                          ))
                                            0)
        time-temporary-back-home (->>
                       time-between-leave-and-return
                       (filter :home?)
                       (map #(t/in-seconds (t/interval (:start %)(:end %))))
                       (apply + 0))

        time-not-at-home (+ time-before-get-home-at-midnight time-after-leaving-home-at-night (- time-temporary-back-home))
        ever-at-home? (seq (concat after-get-home after-return-home))
        ever-leave-home? (seq (concat after-leave-home ))
        ]

    (cond

      (and ever-at-home? (seq befroe-leave-home) (seq after-return-home))
      {:leave_home_time             (:end (last befroe-leave-home))
       :return_home_time            (:start (first after-return-home))
       :time_not_at_home_in_seconds (+ (t/in-seconds
                                         (t/interval (:end (last befroe-leave-home))
                                                     (:start (first after-return-home))))
                                       time-not-at-home
                                       )
       }
      ; return home at midnight and never leave home again
      (and ever-at-home? (seq before-get-home-at-midnight) (not ever-leave-home?))
      {:time_not_at_home_in_seconds time-not-at-home
       :return_home_time (:end (last before-get-home-at-midnight))}
      ; leave home at some point and never return home
      (and ever-at-home? (seq after-leave-home-at-night)
           (>= (t/hour (:end (last episodes))) 22))
      {:time_not_at_home_in_seconds time-not-at-home
       :leave_home_time (:start (first after-leave-home-at-night))}
      ; never leave home
      (and ever-at-home? (not ever-leave-home?))
      {:time_not_at_home_in_seconds time-not-at-home}

      )

    )



  )

(s/defn geodiameter-in-km :- s/Num
  [locations :- [Location]]
  (apply max 0 (for [l1 locations
                     l2 locations]
                 (if (= l1 l2)
                   0
                   (spatial/haversine l1 l2))
                 ))
  )



(defn- n-meter-gait-speed-from-head
  "Return the speed (m/s) of the trace starting from the head of the location sequence until the accumulative
  distance is over n meters"
  [location-trace meters]
  (loop [last-loc (first location-trace)
         [cur-loc & rest] (rest location-trace)
         accum-time 0
         accum-distance 0
         ]
    (if cur-loc
      (let [accum-time (+ accum-time (/ (t/in-millis (t/interval (timestamp last-loc) (timestamp cur-loc))) 1000.0))
            accum-distance (+ accum-distance (* 1000 (spatial/haversine last-loc cur-loc)))]
        (if (> accum-distance meters)
          (/ accum-distance accum-time)
          (recur cur-loc rest accum-time accum-distance)
          )
        )
      )
    )

  )
(defn- n-meter-gait-speed-over-trace
  "Return all the n-meter gait speeds (m/s) in a location trace"
  [location-trace meters]
  (if (seq location-trace)
    (if-let [speed-from-head (n-meter-gait-speed-from-head location-trace meters)]
      (cons speed-from-head (lazy-seq (n-meter-gait-speed-over-trace (rest location-trace) meters)))
      )
    )
  )

(defn x-quantile-n-meter-gait-speed
  "Return the x quantile n-meter walking speed after filtered by a kalman filter.
  Spurious samples of which the speed is over 6.7 m/s are removed"
  [location-traces x-quantile n-meters]
  (:pre [(>= x-quantile 0)
         (<= x-quantile 1)
         (> n-meters 0)
         (every? seq location-traces)
         ])
  (let [filtered-traces (map #(spatial/kalman-filter % (:filter-walking-speed @config) 10000) location-traces)
        speeds (mapcat #(n-meter-gait-speed-over-trace % n-meters) filtered-traces)
        speeds (filter #(< % (:max-human-speed @config)) speeds)
        ]
    (if (seq speeds)
      (nth (sort speeds) (int (* x-quantile (count speeds)))))
    )
  )


(defn coverage
  "Return the coverage of the mobility data of the day"
  [date zone episodes]
  {:pre [(every? #(and (:start %) (:end %)) episodes)]}
  (let [start-of-date (.toDateTimeAtStartOfDay ^LocalDate date zone)
        end-of-date (temporal/to-last-millis-of-day date zone)
        day-interval (t/interval start-of-date end-of-date)
        overlaps (for [epi episodes]
                   (.overlap ^Interval day-interval
                             (t/interval (:start epi) (:end epi))))
        overlaps (filter identity overlaps)
        ]
    (/ (apply + 0 (map t/in-seconds overlaps)) (t/in-seconds day-interval))
    )
  )


