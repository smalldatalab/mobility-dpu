(ns mobility-dpu.hmm
  (:require [taoensso.timbre :as timbre])
  (:use [mobility-dpu.protocols]
        [aprint.core]))

(timbre/refer-timbre)

(defn compute-alpha-and-c [pi a b Y]
  (let [states (keys pi)
        c-atom (atom (transient {}))
        alpha-atom (atom (transient {}))]
    (doseq [t (range (count Y))]
      (let [alpha_i_unscaled
            (map (fn [i]
                   (if (= t 0)
                     (* (pi i) (b [i t]))
                     (* (b [i t]) (apply + (map (fn [last-state] (* (get @alpha-atom [last-state (- t 1)]) (a [last-state i]))) states)))))
                 states
                 )
            c_t (/ 1 (apply + alpha_i_unscaled))
            ]
        (assert (not= c_t 0.0))
        (swap! c-atom #(assoc! % t c_t))
        (doseq [[i v] (map vector states alpha_i_unscaled)]
          (assert (not= v 0.0))
          (swap! alpha-atom #(assoc! % [i t] (* c_t v)))
          )
        )
      )

    (let [c-map (persistent! @c-atom)
          alpha-map (persistent! @alpha-atom)]
      {:alpha (fn [i t] (get alpha-map [i t]))
       :c     (fn [t] (get c-map t))}
      )

    )

  )

(defn compute-beta [pi a b c Y]
  (let [beta-atom (atom (transient {}))]
    ; The order of t and i are critical.
    ; We should do all the i's first for t_0, then go on to t_1, .. etc.
    (doseq [t (reverse (range (count Y))) i (keys pi)]
      (let [beta_i_t
            (if (= (- (count Y) 1) t)
              (c t)
              (apply + (map (fn [j] (* (c t) (get @beta-atom [j (+ t 1)]) (a [i j]) (b [j (+ t 1)]))) (keys pi)))
              )]
        (assert (not= beta_i_t 0))
        (swap! beta-atom #(assoc! % [i t] beta_i_t))
        )
      )
    (let [beta-map (persistent! @beta-atom)]
      (fn [i t]
        (get beta-map [i t])))

    )
  )
(defn hmm
  "Apply Hidden Makov Model to smooth the activity samples
  See: http://en.wikipedia.org/wiki/Baum%E2%80%93Welch_algorithm"
  [sample-seq transition-matrix first-x-prob]
  (if (< (count sample-seq) 2)
    (throw (Exception. "HMM can't work when the number of samples < 2"))
    (let [; observations
          Y (into [] sample-seq)
          a transition-matrix
          ; initial transition matrix
          b (memoize (fn [[x nth-of-y]]
                       (prob-sample-given-state (nth Y nth-of-y) x)))
          pi first-x-prob
          ]
      (loop [a a pi pi X []]
        (let [{:keys [alpha c]} (compute-alpha-and-c pi a b Y)
              beta (compute-beta pi a b c Y)
              gamma (memoize (fn [i t] (/ (* (alpha i t) (beta i t)) (c t))))
              theta (memoize (fn [i j t] (* (alpha i t) (a [i j]) (beta j (+ t 1)) (b [j (+ t 1)]))))
              new-pi (into {} (map (fn [i] [i (gamma i 0)]) (keys pi)))
              new-a (into {} (map (fn [[i j]]
                                    [[i j]
                                     (/ (apply + (map (fn [t] (theta i j t)) (range (- (count Y) 1))))
                                        (apply + (map (fn [t] (gamma i t)) (range (- (count Y) 1)))))
                                     ]
                                    ) (keys a)))
              new-X (for [t (range (count Y))]
                      (first (apply max-key second (for [i (keys pi)] [i (gamma i t)]))))
              ]
          (if (= new-X X) X (recur new-a new-pi new-X))
          ))
      ))
  )











