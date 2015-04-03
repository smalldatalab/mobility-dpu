(ns mobility-dpu.mobility
  (:require [cheshire.core]
            [cheshire.custom]
            [taoensso.timbre :as timbre]
            [mobility-dpu.hmm :as hmm]
            [mobility-dpu.summary]
            monger.joda-time
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [mobility-dpu.temporal :as temporal]
            [mobility-dpu.summary :as summary]
            [mobility-dpu.datapoint :as datapoint])
  (:use [aprint.core]
        [mobility-dpu.protocols])
  (:import
    (mobility_dpu.protocols Episode)
    (org.joda.time DateTime)))

(timbre/refer-timbre)






(def init-state-prob {:still 0.91 :on_foot 0.03 :in_vehicle 0.03 :on_bicycle 0.03})
(def states (keys init-state-prob))
(def init-transition-matrix {[:still :still] 0.9 [:on_foot :on_foot] 0.9 [:in_vehicle :in_vehicle] 0.9 [:on_bicycle :on_bicycle] 0.9
                             [:still :on_foot]  0.09 [:still :in_vehicle] 0.005 [:still :on_bicycle] 0.005
                             [:on_foot :still]  0.06 [:on_foot  :in_vehicle] 0.02 [:on_foot  :on_bicycle] 0.02
                             [:in_vehicle :still]  0.005 [:in_vehicle :on_foot] 0.09 [:in_vehicle :on_bicycle] 0.005
                             [:on_bicycle :still]  0.005 [:on_bicycle :on_foot] 0.09 [:on_bicycle :in_vehicle] 0.005})




(defn- downsample
  ([[first & rest]]
    (if first
      (cons first (lazy-seq (downsample (timestamp first) rest)))
      nil
      ))
  ([last-sample-time [cur & rest]]
    (if cur
      (if (> (t/in-seconds (t/interval last-sample-time (timestamp cur))) 20)
        (cons cur (lazy-seq (downsample (timestamp cur) rest)))
        (downsample last-sample-time rest)
        )
      nil)
    )
  )

(defn- segment-by-gaps
  ([[head & rest]] (segment-by-gaps [head] rest))
  ([cur-group [head & rest]]
    (if head
      (if (< (t/in-minutes (t/interval (timestamp (last cur-group)) (timestamp head))) 7)
        (lazy-seq (segment-by-gaps (conj cur-group head) rest))
        (cons cur-group (lazy-seq (segment-by-gaps [head] rest)))
        )
      [cur-group])
    )
  )
(defn- hmm-partition
  "Partition a seq of activity samples where consecutive samples with the same inferred state are partitioned into one segments"
  [segment transition-matrix first-x-prob]
  {:pre [(every? identity (map first-x-prob states))
         (= (* (count states) (count states)) (count (keys transition-matrix)))
         (every? #(<= 0 % 1) (for [obs segment state states] (prob-sample-given-state obs state)))
         ]}
  (let [inferred-states (hmm/hmm segment transition-matrix first-x-prob)
        partitions (partition-by :state (map #(sorted-map :state %1 :sample %2) inferred-states segment))]
       (for [partition partitions]
         {:state (:state (first partition))
          :start (timestamp (:sample (first partition)))
          :end (timestamp (:sample (last partition)))
          :activity-samples (map :sample partition)}
         )
    )
  )


(defn- extend-and-merge-still-partitions
  "Partition a seq of activity samples where consecutive samples with the same inferred state are partitioned into one segments"
  ([[cur & remains :as partitions] gap-allowance-in-secs]
    {:pre [(every? #(and (:state %) (:start %) (:end %) (:activity-samples %))  partitions)]}
    {:post [(every? #(and (:state %) (:start %) (:end %) (:activity-samples %))  partitions)]}
    (if (seq remains)
      (let [next (first remains)
            cur-state (:state cur)
            next-state (:state next)
            gap (t/in-seconds (t/interval (:end cur) (:start next)))]
        (cond
              ; merge two "still" partitions if they are close in time
              (and (= cur-state next-state :still) (<= gap gap-allowance-in-secs) )
              (lazy-seq
                (extend-and-merge-still-partitions
                  (cons {:state :still
                         :start (:start cur)
                         :end (:end next)
                         :activity-samples (apply concat (map :activity-samples [cur next]))
                         }
                        (rest remains))
                  gap-allowance-in-secs))
              ; extend a "still" partitions if the gap between it and the next partition is small
              (and (= cur-state :still) (<= gap gap-allowance-in-secs) )
                (cons (assoc cur :end (t/minus (:start next) (t/millis 1)))
                      (lazy-seq
                        (extend-and-merge-still-partitions remains gap-allowance-in-secs)))
              :default
                (cons cur (lazy-seq (extend-and-merge-still-partitions remains gap-allowance-in-secs)))
              )
        )
      (list cur)
      )
    )
  )

(defn- merge-partitions-with-locations
  "Merge the activity segments with the location samples based on timestamps"
  [partitions loc-seq]
  (loop [[head & rest] partitions
         locations loc-seq
         ret []]
    (if head
      (let [{:keys [start end]} head
            start (t/minus start (t/seconds 1))
            locations (drop-while #(t/before? (timestamp %) start) locations)
            [within over] (split-with #(t/within? start end (timestamp %))  locations)
            head (assoc head :location-samples within)]
        (recur rest over (conj ret head))
        )
      ret)
    )
)




(defn extract-episodes [datasource]
  {:pre (satisfies? DatabaseProtocol datasource)
   :post (map (fn [e] satisfies? EpisodeProtocol e) %)}

  (let [act-seq (sort-by timestamp (activity-samples datasource))
        loc-seq (sort-by timestamp (location-samples datasource))
        ; remove the location samples that have low accuracy
        loc-seq (filter #(< (accuracy %) 75) loc-seq)
        ; downsampling
        act-seq (downsample act-seq)
        ; break data points into small segments by gaps
        segments (segment-by-gaps act-seq)
        segments (filter #(> (count %) 1) segments)

        ; further break a segment into partitions, each of which represent a continuos activity episode
        partitions (mapcat (fn [segment] (hmm-partition segment
                                                        init-transition-matrix
                                                        init-state-prob))
                                    segments)
        partitions (filter #(not= (:start %) (:end %)) partitions)
        partitions (cond-> partitions
                           (seq partitions) (extend-and-merge-still-partitions  (* 1.5 60 60)))
        ; merge activity segments with location datapoints by time
        partitions (merge-partitions-with-locations partitions loc-seq)
       ]
    ; generate episodes
    (map (fn [{:keys [state location-samples activity-samples]}]
           (Episode. state
                     (timestamp (first activity-samples))
                     (timestamp (last activity-samples))
                     activity-samples
                     location-samples )) partitions)

    )
  )


(defn- create-daily-group [date zone episodes]
  {:date date
   :zone zone
   :episodes (map (partial temporal/trim-episode-to-day-range date zone) episodes)}
  )


(defn group-by-day
  "Group epidsode by days. A segment can belong to mutiple groups if it covers a time range across more than one days"
  ([episodes] (let [episodes (sort-by start episodes)]
                (if (seq episodes)
                  (group-by-day [] (start (first episodes)) episodes))))
  ([group group-start-time [epi & rest :as episodes]]
    (let [date (c/to-local-date group-start-time)
          zone (.getZone ^DateTime group-start-time)
          group-end-time (temporal/to-last-millis-of-day date zone)]
      (cond
        ; no more episodes
        (nil? epi)
        (list (create-daily-group date zone group))
        ; the whole timespan of the current episode is ahead of the group
        (t/after? (start epi) group-end-time)
        (cons (create-daily-group date zone group)
              (lazy-seq (group-by-day [] (start epi) episodes)))
        ; some part of the episode is beyond the time range of the group.
        (t/after? (end epi) group-end-time)
        ; add the epi to the current group and move to the next group.
        ; the start time of the next group is the next millisec of the cur group's end time.
        ; but the time zone will be the timezone of the end time of the episode
        ; so that when the timezone changed during the episode. (e.g. when daylight saving time occurs)
        ; the timezone of the group will change accordingly.
        (let [next-group-start-time (t/to-time-zone (t/plus group-end-time (t/millis 1)) (.getZone (end epi)))]
          (cons  (create-daily-group date zone (conj group epi))
                 (lazy-seq (group-by-day [] next-group-start-time episodes)))
          )

        ; within the group
        :else
        (lazy-seq (group-by-day (conj group epi) group-start-time rest))
        )
      )
    )
  )

(defn get-datapoints [user data-source]
  (let [source (source-name data-source)
        daily-episode-groups (group-by-day (extract-episodes data-source))]
    (apply concat
     (for [{:keys [date zone episodes] } daily-episode-groups]
       [(summary/summarize user source date zone episodes)
        (summary/segments user source date zone episodes)
        ]
       ))
    )
  )





(comment       ; FIXME not sure if the following function is needed anymore.
  (map (fn [{:keys [displacement-speed inferred-activity duration-in-seconds] :as seg}]
         ; deal with subway cases
         (if (and (= inferred-activity :still)
                  displacement-speed
                  (>= displacement-speed 1)
                  (> duration-in-seconds 120)
                  )
           (assoc seg :inferred-activity :in_vehicle)
           seg)
         ) act-loc))







