(ns mobility-dpu.summary
  (:require [clj-time.coerce :as c]
            [clj-time.core :as t]
            [mobility-dpu.spatial :as spatial]
            [taoensso.timbre :as timbre])
  (:use [aprint.core])
  (:import (org.joda.time DateTime LocalDate)))

(timbre/refer-timbre)

(defn group-by-day
  "Group segments by days. A segment can belong to mutiple groups if it covers a time range across more than one days"
  [segs]
  (let [seg->times
        (fn [seg]
          (let [seg-date (c/to-local-date (:start seg))
                seg-zone (.getZone ^DateTime (:start seg))
                end-of-seg-date (.toDateTimeAtStartOfDay ^LocalDate  (t/plus seg-date (t/days 1)) seg-zone)]
            [seg-date end-of-seg-date]
            ))]
    (if (seq segs)
      (loop [segs segs
             times (seg->times (first segs))
             group [] groups []]
        (let [[group-date end-of-group-date] times
              seg (first segs)
              seg-start-time (:start seg)
              seg-end-time (:end seg)]
          (if seg
            (cond
              ; the whole segment is ahead the group
              (t/after? seg-start-time end-of-group-date)
                   (recur segs (seg->times seg) [] (conj groups [group-date group]))
              ; some part of segment is beyond the time range of the group
              (t/after? seg-end-time end-of-group-date)
                   (recur segs (map #(t/plus % (t/days 1)) times) [] (conj groups [group-date (conj group seg)]))
              ; within the group
              :else
                   (recur (rest segs)  times (conj group seg) groups)
              )
            (conj groups [group-date group])
            )
          )
        )
      []
      )
    )

  )

(defn trim-date-time
  "Trim the time range of the segments if their start/end time are before/after the time of the given date"
  [date segs]
  (let [zone (.getZone ^DateTime (:start (first segs)))
        start-of-date (.toDateTimeAtStartOfDay ^LocalDate date zone)
        end-of-date (-> date
                        ^LocalDate (t/plus (t/days 1))
                        (.toDateTimeAtStartOfDay zone)
                        (t/minus (t/millis 1))
                        )
        ]
    (for [{:keys [start end] :as %} segs]
      (assoc % :start
               (if (t/before? start start-of-date)
                 start-of-date start)
               :end
               (if (t/after? end end-of-date)
                 end-of-date end))
      )
    )
  )


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


(defn summarize [segs]
    (for [[date day-segs] (group-by-day segs)]
      (let [day-segs (trim-date-time date day-segs)]
        {:date date
         :summary
         (merge
            {
             :geodiameter_in_km (geodiameter-in-km day-segs)
             :walking_distance_in_km (walking-distance-in-km day-segs)
             :active_time_in_seconds (active-time-in-seconds day-segs)
             :max_gait_speed_in_meter_per_second (max-gait-speed day-segs)
             :coverage (coverage date day-segs)

             }
            (infer-home day-segs)
         )
         :segments day-segs
         }

        )

      )
  )



(defn debug-mesg [segs]
  :debug (map #(vector (:start %) (:end %) (:inferred-activity %) (spatial/haversine (:median-location (first segs))
                                                                                     (:median-location %))) segs)
  )