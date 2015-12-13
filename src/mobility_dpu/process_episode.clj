(ns mobility-dpu.process-episode
  (:require [schema.core :as s]
            [mobility-dpu.spatial :as spatial]
            [clj-time.core :as t]
            [mobility-dpu.temporal :as temporal]
            [clj-time.coerce :as c]

            [taoensso.timbre :as timbre])
  (:use [mobility-dpu.protocols])
  (:import (org.joda.time DateTime)))


(timbre/refer-timbre)

(s/defn assoc-cluster :- [EpisodeSchema]
        "Associate (still) episodes with their corresponding cluster assignments based on a time-based DBSCAN with the given
        epsMeters and minMinutes.
        EpsMeters is the distance threshold for neighbor points.
        MinMinutes is the  duration threshold for a set of neighbor points to be considered as a cluster"
        [episodes :- [EpisodeSchema] epsMeters minMinutes]
        {:pre [(= (count (map :start (filter #(= (:inferred-state %) :still) episodes))) (count (distinct (map :start (filter #(= (:inferred-state %) :still) episodes)))))]}

        (let [locations   (->> episodes
                               (filter #(= (:inferred-state %) :still) )

                               (filter #(seq (:location-trace (:trace-data %))) )
                               (map #(assoc
                                      (spatial/median-location
                                        (:location-trace (:trace-data %)))
                                      :duration (t/in-minutes (t/interval (:start %) (:end %)))
                                      :start (:start %)
                                      :end (:end %)
                                      ))
                               )

              epsKM (* 0.001 epsMeters)
              minutes-sum #(apply + (map :duration %))
              remaining-epis (atom (into #{} locations))
              get-neighbors (fn [epi]
                              (filter
                                #(and (not= epi %)
                                      (<= (spatial/haversine epi %) epsKM))
                                @remaining-epis))
              expand-cluster (fn [core seed-neighbors]
                               (loop [cluster #{core} [cur-seed & rest-seeds] seed-neighbors]
                                 (if cur-seed
                                   (do
                                     (assert (@remaining-epis cur-seed))
                                     (swap! remaining-epis disj cur-seed)
                                     (let [seed-neighbors (get-neighbors cur-seed)
                                           is-seed-center? (>= (minutes-sum seed-neighbors) minMinutes)]
                                       (recur
                                         (conj cluster cur-seed)
                                         (cond-> rest-seeds
                                                 is-seed-center?
                                                 (into
                                                   (->> seed-neighbors
                                                        (filter (complement (into #{} rest-seeds)) )
                                                        )
                                                   )
                                                 )
                                         )
                                       )
                                     )
                                   cluster
                                   )
                                 ))
              [clusters noises]
              (loop [clusters [] noises [] [cur & rest-episodes] locations ]
                (if cur
                  (if (@remaining-epis cur)
                    (do
                      (swap! remaining-epis disj cur)
                      (let [neighbors (get-neighbors cur)
                            is-center? (>= (minutes-sum neighbors) minMinutes)]
                        (if is-center?
                          (recur (conj clusters (expand-cluster cur neighbors)) noises rest-episodes)
                          (recur clusters (conj noises cur) rest-episodes)
                          )
                        ))
                    (recur clusters noises rest-episodes)
                    )
                  [clusters noises]
                  )
                )
              start-time->cluster
              (->>
                (for [locs (concat clusters (map vector noises))]
                  (let [locs (sort-by :start locs)
                        cluster-center
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

                          )]
                    (for [{:keys [start]} locs]
                      [start cluster-center]
                      )
                    )
                  )
                (apply concat)
                (concat (for [{:keys [start]} noises]
                          [start :noise]
                          ))
                (into {}))
              ]
          (map #(cond->
                 %
                 (and (start-time->cluster (:start %)) (= (:inferred-state %) :still))
                 (assoc :cluster (start-time->cluster (:start %)))
                        ) episodes)
          )
        )


(s/defn merge-still-epidoses :- [EpisodeSchema]
  "Merge the consecutive episodes that are
  1) both of still state
  2) the gap between them is less than 60 miniutes,
  and 3) are of the same cluster
  into one still episode"
  [[cur next & rest-episodes]]
  (if cur
    (if (and (= :still (:inferred-state cur) (:inferred-state next))
             (not (nil? (:cluster cur)))
             (= (:cluster cur) (:cluster next))
             (<= (t/in-minutes (t/interval (:end cur) (:start next))) 60))
      (lazy-seq (merge-still-epidoses (cons (merge-two cur next) rest-episodes)))
      (cons cur (lazy-seq (merge-still-epidoses (cons next rest-episodes))))
      )
    )
  )


(s/defn group-by-day :- [DayEpisodeGroup]
  [episodes :- [EpisodeSchema]]
  "Group epidsodes by days. In the case where a segment belongs to mutiple groups (i.e. spanning multiple days),
   it will be simultaneously included in those groups but with start/end time trimed to the corresponding timeframe"
  (->> (group-by (comp c/to-local-date :start) episodes)
       (map (fn [[date epis]]
              (let [first-epi (first (sort-by :start epis))
                    zone (.getZone ^DateTime (:start first-epi))
                    start (.withTimeAtStartOfDay ^DateTime (:start first-epi))
                    interval (t/interval start
                                         (t/minus (t/plus start (t/days 1)) (t/millis 1)))]
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


  (let [
        provided-home-cluster
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
      ))



  )

