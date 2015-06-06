(ns mobility-dpu.algorithms
  (:require
            [clj-time.core :as t]
            [mobility-dpu.spatial :as spatial]
            [taoensso.timbre :as timbre]
            [mobility-dpu.temporal :as temporal])
  (:use [aprint.core]
        [mobility-dpu.config]
        [mobility-dpu.protocols :only [LocationSampleProtocol timestamp]])
  (:import (org.joda.time LocalDate Interval)
           ))

(timbre/refer-timbre)

(defn duration [%]
  (t/interval (:start %) (:end %))
  )
(defn infer-home
  "Given a day of segments, return
  1) time leave/return home,
  2) time not at home in seconds,
  and 3) home location"
  [place-episodes possible-home-location]
  {:pre [(seq place-episodes)
         (every? #(and (:location %) (:start %) (:end %)) place-episodes)
         (or (nil? possible-home-location) (satisfies? LocationSampleProtocol possible-home-location))]}
  (let [first-loc (:location (first place-episodes))
        last-loc (:location (last place-episodes))]
    (let [home-location (if (< (spatial/haversine first-loc last-loc) 0.2)
                          first-loc
                          (if possible-home-location
                            (cond (< (spatial/haversine possible-home-location first-loc) 0.2)
                                  first-loc
                                  (< (spatial/haversine possible-home-location last-loc) 0.2)
                                  last-loc
                                  )
                            )
                          )]
      (if home-location
        (let [at-home? #(< (spatial/haversine home-location  (:location %)) 0.2)
              [before-leave-home after-leave-home] (split-with at-home? place-episodes)
              leave-home-time (if (and (= first-loc home-location) (seq before-leave-home))
                                (:end (last before-leave-home)))
              ever-leave-home? (seq after-leave-home)
              [after-return-home before-return-home] (map reverse (split-with at-home? (reverse after-leave-home)))
              return-home-time (if (seq after-return-home)
                                 (:start (first after-return-home))
                                 )
              back-home-seconds (apply + 0 (map (comp t/in-seconds duration) (filter at-home? before-return-home)))
              ]
          {:home home-location
           :leave_home_time (if ever-leave-home? leave-home-time)
           :return_home_time (if ever-leave-home? return-home-time)
           :time_not_at_home_in_seconds (if ever-leave-home?
                                          (- (t/in-seconds (t/interval leave-home-time return-home-time)) back-home-seconds)
                                          0)
           })
        {}
        )
      )
    )
  )

(defn geodiameter-in-km [locations]
  {:pre [(every? #(satisfies? LocationSampleProtocol %) locations)]}
  (apply max 0 (for [l1 locations
                     l2 locations]
                 (spatial/haversine l1 l2)
                 ))
  )


(comment (defn max-gait-speed
           "Return the 90% quatile walking speed without considering the spurious samples of which the speed is over 4 m/s"
           [episodes]
           (let [episodes (->> episodes
                               (filter #(= :on_foot (state %)))
                               (filter #(> (count (location-trace %)) 1))
                               )
                 episode-speeds
                 (for [episode episodes]
                   (let [filtered-trace (spatial/kalman-filter (location-trace episode) 2 30000)]
                     (map #(speed %) (partition 2 1 filtered-trace))
                     )
                   )
                 speeds (filter #(< % 4) (apply concat episode-speeds))
                 ]
             (if (seq speeds)
               (nth (sort speeds) (int (* 0.9 (count speeds))))
               0)
             )
           ))

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
      (let [accum-time (+ accum-time (t/in-seconds (t/interval (timestamp last-loc) (timestamp cur-loc))))
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
  "Return all the n-meter gait speeds in a location trace"
  [location-trace meters]
  {:pre [(every? #(satisfies? LocationSampleProtocol %) location-trace)]}

  (if (seq location-trace)
    (if-let [speed-from-head (n-meter-gait-speed-from-head location-trace meters)]
      (cons speed-from-head (lazy-seq (n-meter-gait-speed-over-trace (rest location-trace) meters)))
      )
    )
  )

(defn x-quantile-n-meter-gait-speed
  "Return the x quantile n-meter walking speed after filtered by a kalman filter.
  Spurious samples of which the speed is over 4 m/s are removed"
  [location-traces x-quantile n-meters]
  (:pre [(>= x-quantile 0)
         (<= x-quantile 1)
         (> n-meters 0)
         (every? seq location-traces)
         ])
  (let [filtered-traces (map #(spatial/kalman-filter % (:filter-walking-speed @config) 30000) location-traces)
        speeds (mapcat #(n-meter-gait-speed-over-trace % n-meters) filtered-traces)
        speeds (filter #(< % 4) speeds)
        ]
    (if (seq speeds)
      (nth (sort speeds) (int (* x-quantile (count speeds)))))
    )
  )


(defn coverage
  "Return the coverage of the mobility data"
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

