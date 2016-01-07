(ns mobility-dpu.hmm
  (:require [taoensso.timbre :as timbre]
            [schema.core :as s])
  (:use [mobility-dpu.protocols]
        [aprint.core]))

(timbre/refer-timbre)



(s/defn hmm :- [State]
  "Apply Hidden Makov Model to smooth the activity samples
   https://en.wikipedia.org/wiki/Viterbi_algorithm"
  [sample-seq :- [(s/protocol ActivitySampleProtocol)]
   transition-matrix
   init-prob]
  (let [states (keys init-prob)
        init-trace (zipmap
                     states
                     (for [state states]
                       {:prob (init-prob state)
                        :path []})
                     )
        state-trace
        (loop [[sample & rest-samples] sample-seq state-trace init-trace]
          (if sample
            (let [scale (/ 1 (apply + (map (comp :prob val) state-trace)))]
              (recur
                rest-samples
                (zipmap
                  states
                  (for [state states]
                    (let [[max-prev-state max-prob]
                          (apply
                            max-key
                            second
                            (for [prev-state states]
                              [prev-state
                               (* (prob-sample-given-state sample state)
                                  (transition-matrix [prev-state state])
                                  (:prob (state-trace prev-state)))]))]
                      {:path (conj (:path (state-trace max-prev-state)) max-prev-state)
                       :prob (* max-prob scale)}
                      )
                    ))))
            state-trace
            )
          )
        max-last-state (key (apply max-key (comp :prob val) state-trace))
        ]
    (conj (:path (state-trace max-last-state)) max-last-state)
    )
  )











