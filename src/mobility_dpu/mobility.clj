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

            [mobility-dpu.spatial :as spatial]
            [schema.core :as s])
  (:use [aprint.core]
        [mobility-dpu.protocols]
        [mobility-dpu.process-episode])
  (:import
    (mobility_dpu.protocols Episode LocationSample StepSample)
    (org.joda.time DateTime)))

(timbre/refer-timbre)






(def init-state-prob {:still 0.91 :on_foot 0.03 :in_vehicle 0.03 :on_bicycle 0.03})
(def states (keys init-state-prob))
(def init-transition-matrix {[:still :still]      0.9 [:on_foot :on_foot] 0.9 [:in_vehicle :in_vehicle] 0.9 [:on_bicycle :on_bicycle] 0.9
                             [:still :on_foot]    0.09 [:still :in_vehicle] 0.005 [:still :on_bicycle] 0.005
                             [:on_foot :still]    0.06 [:on_foot :in_vehicle] 0.02 [:on_foot :on_bicycle] 0.02
                             [:in_vehicle :still] 0.005 [:in_vehicle :on_foot] 0.09 [:in_vehicle :on_bicycle] 0.005
                             [:on_bicycle :still] 0.005 [:on_bicycle :on_foot] 0.09 [:on_bicycle :in_vehicle] 0.005})




(s/defn downsample :- [(s/protocol TimestampedProtocol)]
  "Drop some samples in the given sequence of timestamped samples so that the sampling frequency is no higher than 20 seconds "
  ([[first & rest] :- [(s/protocol TimestampedProtocol)]]
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

(s/defn segment-by-gaps :- [[(s/protocol TimestampedProtocol)]]
  "Segments a sequence of timestamped samples into subsequences if there are gaps that is longer than 7-minute long"
  ([[head & rest] :- [(s/protocol TimestampedProtocol)]] (segment-by-gaps [head] rest))
  ([cur-group [head & rest]]
   (if head
     (if (< (t/in-minutes (t/interval (timestamp (last cur-group)) (timestamp head))) 7)
       (lazy-seq (segment-by-gaps (conj cur-group head) rest))
       (cons cur-group (lazy-seq (segment-by-gaps [head] rest)))
       )
     [cur-group])
    )
  )

(def HMM-Partition
  {:state State
   :start (s/protocol t/DateTimeProtocol)
   :end (s/protocol t/DateTimeProtocol)
   })

(s/defn hmm-partition :- [HMM-Partition]
  "Partition a seq of activity samples where consecutive samples with the same inferred state are partitioned into one segments"
  [segment :- [(s/protocol ActivitySampleProtocol)]]
  (let [inferred-states (hmm/hmm segment init-transition-matrix init-state-prob)
        partitions (partition-by :state (map #(sorted-map :state %1 :sample %2) inferred-states segment))]
    (for [partition partitions]
      {:state            (:state (first partition))
       :start            (timestamp (:sample (first partition)))
       :end              (timestamp (:sample (last partition)))}
      )
    )
  )


(defn- extend-and-merge-still-partitions
  "Partition a seq of activity samples where consecutive samples with the same inferred state are partitioned into one segments"
  ([[cur & remains :as partitions] gap-allowance-in-secs]
   {:pre [(every? #(and (:state %) (:start %) (:end %) (:activity-samples %)) partitions)]}
   {:post [(every? #(and (:state %) (:start %) (:end %) (:activity-samples %)) partitions)]}
   (if (seq remains)
     (let [next (first remains)
           cur-state (:state cur)
           next-state (:state next)
           gap (t/in-seconds (t/interval (:end cur) (:start next)))]
       (cond
         ; merge two "still" partitions if they are close in time
         (and (= cur-state next-state :still) (<= gap gap-allowance-in-secs))
         (lazy-seq
           (extend-and-merge-still-partitions
             (cons {:state            :still
                    :start            (:start cur)
                    :end              (:end next)
                    :activity-samples (apply concat (map :activity-samples [cur next]))
                    }
                   (rest remains))
             gap-allowance-in-secs))
         ; extend a "still" partitions if the gap between it and the next partition is small
         (and (= cur-state :still) (<= gap gap-allowance-in-secs))
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







(s/defn extract-episodes :- [EpisodeSchema]
  [datasource :- (s/protocol UserDataSourceProtocol)]
  "Extract episodes from the given data source."

  (let [act-seq (sort-by timestamp (s/validate [(s/protocol ActivitySampleProtocol)] (activity-samples datasource)))
        loc-seq (sort-by timestamp (s/validate [LocationSample] (location-samples datasource)))
        step-seq (sort-by timestamp (s/validate [StepSample] (steps-samples datasource)) )
        ; remove the location samples that have low accuracy
        loc-seq (filter #(< (:accuracy %) 75) loc-seq)
        ; downsampling
        act-seq (downsample act-seq)
        ; break the data point sequence into smaller segments at missing data gaps
        segments (segment-by-gaps act-seq)
        segments (filter #(> (count %) 1) segments)

        ; further break each segment into partitions, each of which represent a continuos activity episode

        ; each partition is a map as follows
        ; {:start START TIME OF PARTITION
        ;  :end END TIME
        ;  :state  THE INFFERED STATE
        ;  :activity-samples THE RAW SAMPLES}
        partitions (mapcat (fn [segment] (hmm-partition segment)) segments)
        ; remove 0-length partitions
        partitions (filter #(not= (:start %) (:end %)) partitions)


        ;partitions
        _ (comment (cond-> partitions
                           (seq partitions)
                           (extend-and-merge-still-partitions (* 1.5 60 60))))
        ]
    ; generate episodes
    (loop [[p & ps] partitions act-seq act-seq loc-seq loc-seq step-seq step-seq episodes []]
      (if p
        (let [split (fn [seq]
                      (->> seq
                           (drop-while #(t/before? (timestamp %) (:start p)))
                           (split-with #(or (t/before? (timestamp %) (:end p)) (= (timestamp %) (:end p))))
                           )
                      )
              [act-before act-after] (split act-seq)
              [loc-before loc-after] (split loc-seq)
              [step-before step-after] (split step-seq)
              episdoe (->Episode (:state p)
                                 (:start p)
                                 (:end p)
                                 (->TraceData
                                   act-before
                                   loc-before
                                   step-before
                                   ))
              ]
          (recur ps act-after loc-after step-after (conj episodes episdoe))
          )
        episodes
        )


      )


    )
  )


(defn get-datapoints [user data-source]
  (let [source (source-name data-source)
        episodes (-> (extract-episodes data-source)
                     (assoc-cluster 50 20)
                     (merge-still-epidoses))
        home-clusters (infer-home-clusters episodes)
        episodes (map #(cond-> %
                               (home-clusters (:cluster %))
                               (assoc :home? true)) episodes)
        ]
    (apply concat
           (for [{:keys [date zone episodes]} (group-by-day episodes)]
             [(summary/summarize user source date zone episodes (step-supported? data-source))
              (summary/segments user source date zone episodes)
              ]
             ))
    )
  )


(use 'mobility-dpu.database)
(use 'mobility-dpu.android)
(def epis (-> (extract-episodes
                (->AndroidUserDatasource
                  "google:108274213374340954232"
                  (mongodb "omh" "dataPoint")))
              (assoc-cluster 50 20)
              (merge-still-epidoses)))








