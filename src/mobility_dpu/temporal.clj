(ns mobility-dpu.temporal
  (:require [clj-time.core :as t])
  (:import (org.joda.time DateTime DateTimeZone)
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
