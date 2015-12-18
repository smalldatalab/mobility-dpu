(ns mobility-dpu.mobility
  (:require [cheshire.core]
            [cheshire.custom]
            [taoensso.timbre :as timbre]
            [mobility-dpu.hmm :as hmm]
            [mobility-dpu.summary]
            monger.joda-time
            [clj-time.core :as t]
            [schema.core :as s]
            [clj-time.coerce :as c])
  (:use [aprint.core]
        [mobility-dpu.protocols]
        [mobility-dpu.process-episode])
  (:import
    (mobility_dpu.protocols LocationSample StepSample)
    ))

(timbre/refer-timbre)






(def init-state-prob {:still 0.91 :on_foot 0.03 :in_vehicle 0.03 :on_bicycle 0.03})
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





(s/defn mobility-extract-episodes :- [EpisodeSchema]
  "Extract episodes from the given data source."
  [activity-samples :- [(s/protocol ActivitySampleProtocol)]
   location-samples :- [LocationSample]
   steps-samples :- [StepSample]]



  (let [act-seq (sort-by timestamp activity-samples)
        loc-seq (sort-by timestamp location-samples)
        step-seq (sort-by timestamp steps-samples )
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

        ]

    ; generate episodes
    (loop [[p & ps] partitions act-seq act-seq loc-seq loc-seq step-seq step-seq episodes []]
      (if p
        (let [split (fn [trace]
                      (->> trace
                           (drop-while #(t/before? (timestamp %) (:start p)))
                           (split-with #(or (t/before? (timestamp %) (:end p)) (= (timestamp %) (:end p))))
                           (map doall)
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










