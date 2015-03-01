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
(defn daily-datapoint-template [user device type daily-summary body]
  (datapoint-template user device type
                      (:date daily-summary)
                      (-> daily-summary
                          (:segments)
                          last
                          :end)
                      (assoc body :device device)
                      )
  )
(defn daily-summary-datapoint [user device daily-summary]
  (daily-datapoint-template user device "summary"
                            daily-summary
                            (assoc (:summary daily-summary) :date (:date daily-summary))
                            )
  )

(defn daily-segments-datapoint [user device daily-summary]
  (daily-datapoint-template user device "segments"
                            daily-summary
                            {:segments (:segments daily-summary) :date (:date daily-summary)}
                            )
  )
