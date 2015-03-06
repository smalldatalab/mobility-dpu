(ns mobility-dpu.datapoint
  (:require [clj-time.coerce :as c])
  (:import (org.joda.time ReadableInstant ReadablePartial)))

(defn recursive-time->str
  "Convert all the Joda time objects in the given map to str"
  [m]
  (cond
    (map? m)
    (reduce (fn [m key]
              (assoc m key (recursive-time->str (get m key)))
              ) m (keys m))
    (or (list? m) (vector? m)
        (and (not (string? m)) (seq? m)))
    (map recursive-time->str m)
    (or (instance? ReadableInstant m) (instance? ReadablePartial m))
    (str m)
    :else
    m
    )
  )
(defn datapoint-template [user device type date creation-datetime body]
  (let [id (str "mobility-daily-" type "-" user "-" date "-" (clojure.string/lower-case device))]
    (recursive-time->str
      {:_id id
       "_class"  "org.openmhealth.dsu.domain.DataPoint"
       "user_id" user
       "header" { "_id" id,
                 "schema_id" { "namespace"  "cornell",
                              "name"  (str "mobility-daily-" type),
                              "version" { "major"  1, "minor"  0 } },
                 "creation_date_time" creation-datetime,
                 "creation_date_time_epoch_milli" (c/to-long creation-datetime)
                 "acquisition_provenance" { "source_name" (str "Mobility-DPU-v1.0-" device),
                                           "modality" "SENSED" } },
       "body" body
       }
      )
    )




  )
(defn daily-datapoint-template [user device type date creation-date-time body]
  (datapoint-template user device type
                      date
                      creation-date-time
                      (assoc body :device device  :date date)
                      )
  )
(defn daily-summary-datapoint [user device date daily-summary creation-date-time]
  (daily-datapoint-template user device "summary"
                            date
                            creation-date-time
                            daily-summary
                            )
  )

(defn daily-segments-datapoint [user device date daily-segments creation-date-time]
  (daily-datapoint-template user device "segments"
                            date
                            creation-date-time
                            {:segments daily-segments}
                            )
  )
