(ns mobility-dpu.temporal
  (:require [clj-time.core :as t]
            [clj-time.coerce :as c])
  (:import (org.joda.time DateTime DateTimeZone LocalDate)
           (org.joda.time.format ISODateTimeFormat DateTimeFormat DateTimeFormatter)))

; date time parser that try all the possible formats
(defn dt-parser [dt]
  (let [try-parse #(try (DateTime/parse %1 (.withOffsetParsed ^DateTimeFormatter %2)) (catch IllegalArgumentException _ nil))
        ^DateTime dt (or (try-parse dt (ISODateTimeFormat/dateTime))
                         (try-parse dt (ISODateTimeFormat/dateTimeNoMillis))
                         (try-parse dt (DateTimeFormat/forPattern "yyyy-MM-dd'T'HH:mmZZ"))
                         )

        ]
    (if (= "UTC" (.getID ^DateTimeZone (.getZone dt)))
      (t/to-time-zone dt (t/time-zone-for-id "America/New_York"))
      dt
      )))


(defn- trim-date-time
  "Trim the time range of the segments if their start/end time are before/after the time of the given date"
  [date segs]
  (let [zone (.getZone ^DateTime (:start (first segs)))
        start-of-date (.toDateTimeAtStartOfDay ^LocalDate date zone)
        end-of-date (-> date
                        ^LocalDate (t/plus (t/days 1))
                        (.toDateTimeAtStartOfDay zone)
                        (t/minus (t/millis 1))
                        )
        ]
    [date
     (for [{:keys [start end] :as %} segs]
       (assoc % :start
                (if (t/before? start start-of-date)
                  start-of-date start)
                :end
                (if (t/after? end end-of-date)
                  end-of-date end))
       )]
    )
  )
(defn group-by-day
  "Group segments by days. A segment can belong to mutiple groups if it covers a time range across more than one days"
  [segs]
  (let [seg->times
        (fn [seg]
          (let [seg-date (c/to-local-date (:start seg))
                seg-zone (.getZone ^DateTime (:start seg))
                end-of-seg-date (t/minus (.toDateTimeAtStartOfDay ^LocalDate (t/plus seg-date (t/days 1)) seg-zone) (t/millis 1))]
            [seg-date end-of-seg-date]
            ))]
    (if (seq segs)
      (loop [segs segs
             times (seg->times (first segs))
             group [] groups []]
        (let [[group-date end-of-group-date] times
              seg (first segs)
              seg-start-time (:start seg)
              seg-end-time (:end seg)]
          (if seg
            (cond
              ; the whole segment is ahead the group
              (t/after? seg-start-time end-of-group-date)
              (recur segs (seg->times seg) [] (conj groups (trim-date-time group-date group)))
              ; some part of segment is beyond the time range of the group
              (t/after? seg-end-time end-of-group-date)
              (recur segs (map #(t/plus % (t/days 1)) times) [] (conj groups (trim-date-time group-date (conj group seg))))
              ; within the group
              :else
              (recur (rest segs)  times (conj group seg) groups)
              )
            (conj groups (trim-date-time group-date group))
            )
          )
        )
      []
      )
    )

  )


