(ns mobility-dpu.hmm
  (:require [taoensso.timbre :as timbre]
            [clj-time.core :as t]
            [mobility-dpu.spatial :as spatial]))

(timbre/refer-timbre)

(defn hmm
  "Apply Hidden Makov Model to smooth the activity samples
  See: http://en.wikipedia.org/wiki/Baum%E2%80%93Welch_algorithm"
  [dat transition-matrix prob-y-given-x first-x-prob]
  (let [; observations
        Y (map :data dat)
        ; times
        T (map :timestamp dat)
        a transition-matrix
        ; initial transition matrix
        b (memoize (fn [[x nth-of-y]]
                     (prob-y-given-x x (nth Y nth-of-y))))
        pi first-x-prob
        ]
    (loop [a a pi pi X []]
      (let [; scale factor c_t's
            *c (atom {})
            ; alpha
            *alpha (atom {})
            alpha (fn [i t]
                    (get @*alpha [i t]))
            ; init alpha's
            _ (doseq [t (range (count Y))]
                (let [unscaled (for [i (keys pi)]
                                 [i
                                  (if (= t 0)
                                    (* (pi i) (b [i t]))
                                    (* (b [i t]) (apply + (map (fn [%] (* (alpha % (- t 1)) (a [% i]))) (keys pi)) )))
                                  ])

                      c_t (/ 1 (apply + (map second unscaled)))
                      ]

                  (swap! *c #(assoc % t c_t))
                  (doseq [[i v] unscaled]
                    (swap! *alpha #(assoc % [i t] (* c_t v)))
                    )

                  )
                )
            ; beta
            *beta (atom {})
            beta (fn beta [i t]
                   (p :beta (if (= (- (count Y) 1) t)
                              1
                              (if-let [v (get @*beta [i t])]
                                v
                                (let [scale (get @*c (+ t 1))
                                      v (apply + (map (fn [j]
                                                        (* scale (beta j (+ t 1)) (a [i j]) (b [j (+ t 1)])) ) (keys pi)))]
                                  (swap! *beta #(assoc % [i t] v))
                                  v
                                  )
                                )))
                   )
            ; init beta
            _ (doseq [i (keys pi) t (range (count Y))] (let [a (beta i (- (count Y) t 1))]
                                                         (if (or (Double/isInfinite a) (= a 0.0))
                                                           (throw (Exception. "Over or underflow" (str a))))
                                                         )
                                                       )




            theta-denominator (memoize (fn [t]
                                         (apply + (map (fn [j] (* (alpha j t) (beta j t))) (keys pi)))))
            gamma        (memoize (fn [i t] (p :gamma (/ (* (alpha i t) (beta i t))
                                                         (theta-denominator t) ))))
            theta (memoize (fn [i j t]
                             (p :theta
                                (/ (* (alpha i t) (a [i j]) (get @*c (+ t 1)) (beta j (+ t 1)) (b [j (+ t 1)]))
                                   (theta-denominator t)))

                             ))

            new-pi (into {} (map (fn [i] [i (gamma i 0)]) (keys pi)))
            new-a (into {} (map (fn [[i j]]
                                  [[i j]
                                   (/ (apply + (map (fn [t] (theta i j t)) (range (- (count Y) 1))))
                                      (apply + (map (fn [t] (gamma i t)) (range (- (count Y) 1)))))
                                   ]
                                  ) (keys a)))
            new-X (for [t (range (count Y))]
                    (first (apply max-key second (for [i (keys pi)]
                                                   [i (gamma i t)])))
                    )
            ]
        ;(println "Iteration")
        (if (= new-X X) (map #(sorted-map :timestamp %1 :inferred-activity %2 :data %3) T X Y)
                        (recur new-a new-pi new-X))
        ))
    ))


(defn downsample-and-segment-by-gaps [dat]
  (let [; filter out too frequent samples
        dat (map (fn [cur last] (assoc cur :gap (t/in-seconds (t/interval (:timestamp last) (:timestamp cur) ))))
                 dat (concat [(first dat)] (butlast dat)))
        dat (filter #(> (:gap %) 20) dat)
        first-sample (first dat)
        rest-samples (rest dat)

        ; divide traces into segments if the gap is too large (> 7 minutes)
        segments
          (loop [[cur & obs] rest-samples ret [] cur-segment [first-sample]]
            (if cur
              (if (< (:gap cur) (* 7 60))
                (recur obs ret (conj cur-segment cur))
                (recur obs (conj ret cur-segment) [cur])
                )
              (conj ret cur-segment)
              )
            )
        ]
    (filter #(> (count %) 1) segments)))


(defn to-inferred-segments
  "Apply HMM to each activity sample segments and create activity segments,
  where consecutive samples with the same inferred activity are merged into one segments"
  [segments transition-matrix prob-y-given-x first-x-prob]
  (mapcat (fn [segment]
            (let [act-segments (partition-by :inferred-activity (hmm segment transition-matrix prob-y-given-x first-x-prob))]
              (for [seg act-segments]
                (let [{start :timestamp inferred-activity :inferred-activity} (first seg)
                      {end :timestamp} (last seg)
                      data (map :data seg)]
                  {:inferred-activity inferred-activity
                   :start start
                   :end end
                   :data data}
                  )
                )
              )
            ) segments))


(defn merge-segments-with-location
  "Merge the activity segments with the location samples based on timestamps"
  [segments locations]
  (loop [[{:keys [start end] :as seg} & rest-segments] segments
         locations locations
         ret []]
    (if seg
      (let [locations (drop-while #(t/before? (:timestamp %) (t/minus start (t/minutes 1))) locations)
            [within over] (split-with #(t/within? (t/minus start (t/minutes 1)) end
                                                  (:timestamp %))
                                      locations)
            f-loc (:location (first within))
            l-loc (:location (last within))
            distance (if (seq within)
                       (spatial/haversine f-loc l-loc ) )
            avg-gps-speed (if (seq within)
                            (/ (apply + (filter identity (map (comp :speed :location) locations))) (count locations)))
            duration (t/in-seconds (t/interval start end))
            displacement-speed (if (and distance (> duration 0))
                                (/ (* 1000 distance) duration))
            seg (assoc seg :locations within
                           :distance distance
                           :avg-gps-speed avg-gps-speed
                           :displacement-speed displacement-speed
                           :first-location f-loc
                           :last-location l-loc
                           :median-location (spatial/median-location within)
                           :duration-in-seconds duration
                           )]
        (recur rest-segments over (conj ret seg))
        )
      ret)
    )
  )


