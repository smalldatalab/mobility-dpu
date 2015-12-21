(ns mobility-dpu.temporal
  (:require [clj-time.core :as t]
            [clj-time.coerce :as c]
            [schema.core :as s]
            [clj-time.format :as f])
  (:use mobility-dpu.protocols)
  (:import (org.joda.time DateTime DateTimeZone LocalDate)
           (org.joda.time.format ISODateTimeFormat DateTimeFormat DateTimeFormatter)
           (mobility_dpu.protocols Episode))
  )

; date time parser that try all the possible formats
(defn dt-parser [dt]
  (first
    (for [f (map #(.withOffsetParsed ^DateTimeFormatter %)
                 [(ISODateTimeFormat/dateTime)
                  (ISODateTimeFormat/dateTimeNoMillis)
                  (DateTimeFormat/forPattern "yyyy-MM-dd'T'HH:mmZZ")])
          :let [d (try (f/parse f dt) (catch Exception _ nil))]
          :when d] d)))



(defn to-last-millis-of-day [date zone]
  (t/minus (.toDateTimeAtStartOfDay ^LocalDate (t/plus date (t/days 1)) zone) (t/millis 1))
  )
(s/defn to-last-millis-of-hour :- DateTime [dt :- DateTime]
  (-> dt
      (t/plus (t/hours 1))
      (#(.withTime ^DateTime % (t/hour %) 0 0 0 ))
      (t/minus  (t/millis 1))
      )
  )


(defn trim-episode-to-day-range
  "Trim the time range of the segments if their start/end time are before/after the time of the given date"
  [date zone episode]
  (let [start-of-date (.toDateTimeAtStartOfDay ^LocalDate date zone)
        end-of-date (to-last-millis-of-day date zone)
        ]
    (let [new-start
          (if (t/before? (:start episode) start-of-date)
            start-of-date (:start episode))
          new-end
          (if (t/after? (:end episode) end-of-date)
            end-of-date (:end episode))]
      (trim episode new-start new-end)

      )
    )
  )



