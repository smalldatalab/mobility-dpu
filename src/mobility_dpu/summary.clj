(ns mobility-dpu.summary
  (:require [clj-time.coerce :as c]
            [clj-time.core :as t]
            [mobility-dpu.spatial :as spatial]
            [taoensso.timbre :as timbre])
  (:use [aprint.core])
  (:import (org.joda.time DateTime LocalDate)))

(timbre/refer-timbre)





(defn infer-home
  "Given a day of segments, return
  1) time leave/return home,
  2) time not at home in seconds,
  and 3) home location"
  [segs]
  (let [first-loc (:median-location (first segs)) last-loc (:median-location (last segs))]
    (if (and  first-loc last-loc)
      (let [dist-between-first-last (spatial/haversine first-loc last-loc)
            segs (filter :median-location segs)]

        (if (< dist-between-first-last 0.2)
          (let [at-home? #(< (spatial/haversine first-loc  (:median-location %)) 0.2)
                after-leave-home (drop-while at-home? segs)
                leave-home (:start (first after-leave-home))

                [after-return-home before-return-home] (map reverse (split-with at-home? (reverse after-leave-home)))
                return-home (:start (first after-return-home))
                back-home-seconds (apply + 0 (map :duration-in-seconds (filter at-home? before-return-home)))
                ]
            {:home first-loc
             :leave_home_time leave-home
             :return_home_time return-home
             :time_not_at_home_in_seconds (if leave-home (- (t/in-seconds (t/interval leave-home return-home)) back-home-seconds)
                                                         0)
             })
          (do (info (str "Distance between the first-last is too high:" dist-between-first-last))
              {}
              )
          )
        )
      )
    )

  )

(defn geodiameter-in-km [segs]
  (let [segs (filter :median-location segs)]
    (apply max 0 (for [l1 (map :median-location segs)
                     l2 (map :median-location segs)]
                 (spatial/haversine l1 l2)
                  )))
  )


(defn active-time-in-seconds [segs]
  (apply + (->> segs
                (filter #(= :on_foot (:inferred-activity %)))
                (map :duration-in-seconds)
                ))
  )

(defn walking-distance-in-km
  "Return the total (kallman-filtered) walking distance in the given segments,
  but remove the potential spurious samples of which the speed is over 4 m/s"
  [segs]
  (let [segs (filter #(= :on_foot (:inferred-activity %)) segs)
        segs (filter #(> (count (:locations %)) 1) segs)
        locs (mapcat #(spatial/kalman-filter (:locations %) 2 30000) segs)
        locs (filter #(and (:filtered-speed (:location %)) (< (:filtered-speed (:location %)) 4) ) locs)
        ]
    (apply + 0 (map (comp :filtered-distance :location) locs))
    )
  )


(defn max-gait-speed
  "Return the 90% quatile walking speed without considering the spurious samples of which the speed is over 4 m/s"
  [segs]
  (let [segs (filter #(= :on_foot (:inferred-activity %)) segs)
        segs (filter #(> (count (:locations %)) 1) segs)
        locs (mapcat #(spatial/kalman-filter (:locations %) 2 30000) segs)
        speeds (map (comp :filtered-speed :location) locs)
        speeds (filter identity speeds)
        speeds (filter #(< % 4) speeds)
        ]
    (if (seq speeds)
      (nth (sort speeds) (int (* 0.9 (count speeds))))
      0)
    )
  )

(defn coverage
  "Return the coverage of the mobility data"
  [date segs]
   (let [zone (.getZone ^DateTime (:start (first segs)))
         start-of-date (.toDateTimeAtStartOfDay ^LocalDate date zone)
         end-of-date (.toDateTimeAtStartOfDay ^LocalDate  (t/plus date (t/days 1)) zone)
         seconds (apply + (map (fn [{:keys [start end]}]
                                 (t/in-seconds (t/interval start end))
                                 )
                               segs
                               ))
         ]
     (/ seconds (t/in-seconds (t/interval start-of-date end-of-date)))
     )
  )


(defn summarize [date daily-segments]
  (merge
    {
     :geodiameter_in_km (geodiameter-in-km daily-segments)
     :walking_distance_in_km (walking-distance-in-km daily-segments)
     :active_time_in_seconds (active-time-in-seconds daily-segments)
     :max_gait_speed_in_meter_per_second (max-gait-speed daily-segments)
     :coverage (coverage date daily-segments)
     }
    (infer-home daily-segments)
    )
  )



(defn debug-mesg [segs]
  :debug (map #(vector (:start %) (:end %) (:inferred-activity %) (spatial/haversine (:median-location (first segs))
                                                                                     (:median-location %))) segs)
  )