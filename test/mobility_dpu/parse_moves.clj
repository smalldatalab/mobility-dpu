(ns mobility-dpu.parse-moves
  (:require [cheshire.core :as json]
            [mobility-dpu.moves-sync :as moves]
            [schema.core :as s]
            [clojure.test :refer :all])
  (:use [mobility-dpu.protocols]
        [mobility-dpu.android]
        [mobility-dpu.config]))
(deftest test-parse-moves
         (testing "If e can parse moves data correctly"
                  (let [storylines (json/parse-string (slurp (clojure.java.io/resource "moves_storyline.json")) true)]

                    (is (->>
                          storylines
                          (mapcat :segments)
                          (mapcat moves/segment->episodes)
                          (s/validate [EpisodeSchema])
                          ))

                    )
                  ))