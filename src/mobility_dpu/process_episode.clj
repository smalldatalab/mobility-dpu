(ns mobility-dpu.process-episode
  (:require [schema.core :as s]
            [mobility-dpu.spatial :as spatial]
            [clj-time.core :as t]
            [mobility-dpu.temporal :as temporal]
            [clj-time.coerce :as c]

            [taoensso.timbre :as timbre]
            [clj-time.format :as f])
  (:use [mobility-dpu.protocols])
  (:import (org.joda.time DateTime LocalDate)
           (org.joda.time.format ISODateTimeFormat)
           (java.util HashSet)))


(timbre/refer-timbre)

(defn minutes-sum [locs] (apply + (map :duration locs)))

(s/defn get-cluster-info [locs]
  (let [locs (sort-by :start locs)]
    (assoc (spatial/median-location locs)
      :first-time (:start (first locs))
      :last-time (:start (last locs))
      :total-time-in-minutes (minutes-sum locs)
      :number-days (->> locs
                        (map (comp c/to-local-date :start))
                        (distinct)
                        (count))
      :hourly-distribution
      (->>
        locs
        (mapcat
          (fn [loc]
            (loop [ret [] start (:start loc)]
              (let [end (if (t/after? (temporal/to-last-millis-of-hour start) (:end loc))
                          (:end loc)
                          (temporal/to-last-millis-of-hour start)
                          )
                    hours (/ (t/in-millis
                               (t/interval
                                 start
                                 end))
                             (* 1000 60 60.0)
                             )
                    ret (conj ret {(t/hour start) hours })]
                (if (= end (:end loc))
                  ret
                  (recur ret (t/plus end (t/millis 1)))
                  )

                )
              )
            )
          )
        (concat (map #(array-map % 0) (range 24)))
        (apply merge-with + )
        (sort-by first)
        )
      ))

  )
(s/defn assoc-cluster :- [EpisodeSchema]
        "Associate (still) episodes with their corresponding cluster assignments based on a time-based DBSCAN with the given
        epsMeters and minMinutes.
        EpsMeters is the distance threshold for neighbor points.
        MinMinutes is the  duration threshold for a set of neighbor points to be considered as a cluster"
        [episodes :- [EpisodeSchema] epsMeters minMinutes]
        {:pre [(= (count (map :start (filter #(= (:inferred-state %) :still) episodes))) (count (distinct (map :start (filter #(= (:inferred-state %) :still) episodes)))))]}

        (p :assoc-cluster
           (let [nodes   (->> episodes
                              (filter #(= (:inferred-state %) :still) )
                              (filter #(seq (:locations %)) )
                              (map #(assoc
                                     (spatial/median-location
                                       (:locations %))
                                     :duration (t/in-minutes (t/interval (:start %) (:end %)))
                                     :start (:start %)
                                     :end (:end %)
                                     ))
                              )
                 epsKM (* 0.001 epsMeters)
                 ; use mutable hashset to improve efficiency
                 visited-set (HashSet.)
                 visit! (fn [node]
                         (.add visited-set (:start node)))
                 visit? (fn [node]
                          (.contains visited-set (:start node)))

                 clustered-set (HashSet.)
                 cluster! (fn [node]
                          (.add clustered-set (:start node)))
                 clustered? (fn [node]
                          (.contains clustered-set (:start node)))


                 is-center? (memoize
                              (fn [node]
                                "If a node qualifies as a center i.e. the total time it and and the nodes close to it
                                have is over the threshold"
                                (let [neighbors (filter
                                                  #(and (<= (spatial/haversine node %) epsKM))
                                                  nodes)
                                      ]
                                  (= :IS-CENTER
                                    (reduce (fn [sum-mins next-node]
                                              (let [sum-mins (+ (:duration next-node) sum-mins)]
                                                (if (>= sum-mins minMinutes)
                                                  (reduced :IS-CENTER)
                                                  sum-mins
                                                  )
                                                )
                                              )
                                            0
                                            neighbors
                                            ))
                                  ))
                                            )

                 get-neighbors (fn [node]
                                 "get neighbors of a node (if it is a center)"
                                 (if (is-center? node)
                                   (remove #{node}
                                           (filter
                                             #(and (<= (spatial/haversine node %) epsKM))
                                             nodes)))
                                 )

                 expand-nodes (fn expand-nodes [node, neighbors]
                                "Expand a node to its neighbors and its neighbors' neighbor to form a cluster"
                                (cluster! node)
                                (loop [cluster [node] [neighbor & to-expands] neighbors]
                                  (if neighbor
                                    (let [cluster (if-not (clustered? neighbor)
                                                    (do
                                                      (cluster! neighbor)
                                                      (conj cluster neighbor))
                                                    cluster
                                                    )
                                          to-expands (if-not (visit? neighbor)
                                                       (do
                                                         (visit! neighbor)
                                                         (if-let [more-neighbors
                                                                  (remove
                                                                    #(and (visit? %) (clustered? %))
                                                                     (get-neighbors neighbor))

                                                                  ]
                                                           (into to-expands more-neighbors)
                                                           to-expands
                                                           )
                                                         )
                                                       to-expands
                                                       )
                                          ]
                                      (recur cluster to-expands)
                                      )
                                    cluster
                                    )
                                  )
                                )
                 dbscan (fn dbscan
                          [[node & rest-nodes]]

                          (if node
                            (if (visit? node)
                              (lazy-seq  (dbscan rest-nodes))
                              (do
                                (visit! node)
                                (if-let [neighbors (get-neighbors node)]
                                  (lazy-seq (cons (expand-nodes node neighbors) (dbscan rest-nodes)) )
                                  (lazy-seq  (dbscan rest-nodes))
                                  ))
                              )
                            )
                          )
                 clusters (dbscan nodes)
                 ; create an individual cluster for those "noise" node
                 ; (i.e. the place the user does not stay long enough to form a cluster)
                 ind-clusters (->> nodes
                                   (filter (complement clustered?))
                                   (map vector)
                                   )
                 clusters (into clusters ind-clusters)
                 ; create a <start-time to cluster> map
                 start-time->cluster
                 (->>
                   (for [cluster clusters]
                     (let [cluster-info (get-cluster-info cluster)]
                       (for [{:keys [start]} cluster]
                         [start cluster-info]
                         )
                       )
                     )
                   (apply concat)
                   (into {}))
                 ]
             (map #(cond->
                    %
                    (and (start-time->cluster (:start %)) (= (:inferred-state %) :still))
                    (assoc :cluster (start-time->cluster (:start %)))
                    ) episodes)
             ))
        )


(s/defn merge-still-epidoses :- [EpisodeSchema]
  "Merge the consecutive episodes that are
  1) both of still state
  2) the gap between them is less than 60 miniutes,
  and 3) are of the same cluster
  into one still episode"
  [episodes :- [EpisodeSchema]]
  (p :merge-still
     (let [merge-still
           (fn merge-still [[cur next & rest-episodes]]
             (if cur
               (if (and (:cluster cur) next
                        (= (:cluster cur) (:cluster next))
                        (<= (t/in-minutes (t/interval (:end cur) (:start next))) 60))
                 (lazy-seq (merge-still (cons (merge-two cur next) rest-episodes)))
                 (cons cur (lazy-seq (merge-still (cons next rest-episodes))))
                 )
               ))]

       (->> (filter (comp #{:still} :inferred-state) episodes) 
            merge-still
            (concat (remove (comp #{:still} :inferred-state) episodes))
            (sort-by :start)
            )
       ))
  )


(s/defn group-by-day :- [DayEpisodeGroup]
  "Group the epidsodes by days. In the case where a segment belongs to mutiple groups (i.e. spanning multiple days),
   it will be simultaneously included in those groups but with start/end time trimed to the corresponding timeframe

   The algorithm works as follows:
   1) compute the dates that each episode covers
   2) group episodes into the date groups they cover
   3) use the timezone in which the majority of the episode occured as the timezone of that date
   4) compute the interval of the date within the given timezone, and find all episodes that cover the interval
   5) trim episodes to fit the interval
   "
  [episodes :- [EpisodeSchema]]

  (p :group-by-day
     (->>

       (let [; iterate over all the local dates this episodes cover
             epi->dates (fn [{:keys [start end]}]
                          (->>
                            (c/to-local-date start)
                            (iterate #(t/plus % (t/days 1) ))
                            (take-while (complement #(t/after? % (c/to-local-date end))))))
             ]
         (->> (for [epi episodes]
                (for [date (epi->dates epi)]
                  [date epi]
                  )
                )
              (apply concat)
              (group-by first)
              (map (fn [[date date-epi]]
                     (let [epis (map second date-epi)
                           ; use the timzone in which the majority segments occured
                           zone (->>
                                  epis
                                  (map #(.getZone ^DateTime (:start %)) )
                                  (frequencies)
                                  (sort-by second)
                                  (reverse)
                                  (first)
                                  (first)
                                  )
                           start (.toDateTimeAtStartOfDay ^LocalDate date zone)
                           interval (t/interval start
                                                (t/minus (t/plus start (t/days 1)) (t/millis 1)))

                           ]
                       {:date     date
                        :zone     zone
                        :episodes (->>
                                    episodes
                                    (filter #(t/overlaps? interval (t/interval (:start %) (:end %))))
                                    (sort-by :start)
                                    (map (partial temporal/trim-episode-to-day-range date zone)))
                        }
                       )
                     ))
              (sort-by :date)
              )

         )

       ))
  )

(s/defn provided-home-location->cluster :- (s/maybe Cluster)
  "Identify the cluster in which the user\n   spent the most time and is within 100 meters away from the provided home location"
  [episodes :- [EpisodeSchema]
   location :- Location]
  (let [cluster (->> episodes
                     (filter #(and (:cluster %)
                                   (< (spatial/haversine location (:cluster %)) 0.1)))
                     (group-by :cluster)
                     (sort-by (fn [[cluster epis]]
                                (apply + (map #(t/in-seconds (t/interval (:start %) (:end %))) epis))
                                ))
                     (last)
                     (first)

                     )]
    (if cluster (info "User has been to the provided home location:" cluster))
    cluster
    )


  )

(s/defn infer-home-clusters :- #{Cluster}
  "Determine the clusters that are home locations through the algorithm:
  1) For each day if the user has been to the provided home location
      or the first cluster and last cluster are the same,
      assume the cluster is the home for that day, and the rest cluster are non-home
  2) Take a cluster as home, if number of day it is assumed to be home is more than the number of day it is assumed to be non-home"
  [episodes :- [EpisodeSchema] provided-home-location :- (s/maybe Location)]
  (p :infer-home
     (let [provided-home-cluster
           (if provided-home-location
             (provided-home-location->cluster
               episodes provided-home-location))]
       (->>
         (filter :cluster episodes)
         (group-by-day)
         (mapcat
           (fn [{:keys [episodes]}]
             (let [clusters (map :cluster episodes)]
               (cond
                 ; if user has ever been to the provided home location.
                 ; assume the rest places are not home
                 (and provided-home-cluster (some #{provided-home-cluster} clusters))
                 (cons [provided-home-cluster :home]
                       (->> clusters
                            (remove #{provided-home-cluster})
                            (distinct)
                            (map #(vector % :non-home))
                            )
                       )
                 ; if user was at the same place at the begining and the end of the day
                 ; assume it is the home, and the rest are not home
                 (and (first clusters)
                      (= (first clusters) (last clusters))
                      (< (t/hour (:start (first episodes))) 11)
                      (> (t/hour (:end (last episodes))) 17)
                      )
                 (cons [(first clusters) :home]
                       (->> clusters
                            (remove #{(first clusters)})
                            (distinct)
                            (map #(vector % :non-home))
                            )
                       )
                 ; this is the weakest assumption. If none of the above is true,
                 ; assume the first and last clusters are home if they cover certain time range and are over 3 hrs long
                 ; but, unlike the other cases, do NOT assume the rest clusters are not home
                 (and (< (t/hour (:start (first episodes))) 8)
                      (>= (t/in-hours (t/interval (:start (first episodes)) (:end (first episodes)))) 2)
                      (> (t/hour (:end (last episodes))) 20)
                      (>= (t/in-hours (t/interval (:start (last episodes)) (:end (last episodes)))) 2)
                      )
                 [[(first clusters) :home]
                  [(last clusters) :home]
                  ]

                 )
               )))
         (group-by first)
         (filter (fn [[cluster types]]
                   (let [{:keys [home non-home]} (frequencies (map second types))]
                     (> (or home 0) (or non-home 0))
                     )
                   )
                 )
         (map first)
         (into #{})
         )))
  )

