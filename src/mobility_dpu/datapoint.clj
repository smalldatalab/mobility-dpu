(ns mobility-dpu.datapoint
  (:require [clj-time.coerce :as c])
  (:import (org.joda.time ReadableInstant ReadablePartial DateTimeZone)
           (clojure.lang IPersistentMap IPersistentCollection Keyword))
  (:use [mobility-dpu.config]))

(defmulti time->str class)
(defmethod time->str IPersistentMap [m]
  (reduce (fn [m [k v]]
            (assoc m (time->str k) (time->str v))
            ) {} m))
(defmethod time->str String [m] m)
(defmethod time->str IPersistentCollection [m]
  (map time->str m))
(defmethod time->str ReadableInstant [m] (str m))
(defmethod time->str ReadablePartial [m] (str m))
(defmethod time->str DateTimeZone [m] (str m))
(defmethod time->str :default [m] m)

(defn datapoint [user namespace schema source modality time-for-id creation-datetime body]
  (let [id (str schema "-" user "-" time-for-id "-" (clojure.string/lower-case source))]
    (time->str
      {:_id     id
       :_class  "org.openmhealth.dsu.domain.DataPoint"
       :user_id user
       :header  {"id"                             id,
                 "schema_id"                      {"namespace" namespace,
                                                   "name"      schema,
                                                   "version"   {"major" 1, "minor" 0}},
                 "creation_date_time"             creation-datetime,
                 "creation_date_time_epoch_milli" (c/to-long creation-datetime)
                 "acquisition_provenance"         {"source_name" source,
                                                   "modality"    modality}},
       :body    body
       })
    )
  )

(defn mobility-datapoint [user device type date creation-datetime body]
  (datapoint user
             "cornell"                                      ; namespace
             (str "mobility-daily-" type)                   ; schema name
             (clojure.string/lower-case device)             ; souce
             "SENSED"                                       ; modality
             date                                           ; time for id
             creation-datetime                              ; creation datetime
             (assoc body
               :date date
               :device (clojure.string/lower-case device))  ; body
             )
  )

(defn summary-datapoint [{:keys [user
                                 device
                                 date
                                 creation-datetime
                                 geodiameter-in-km
                                 walking-distance-in-km
                                 active-time-in-seconds
                                 steps
                                 gait-speed-in-meter-per-second
                                 leave-return-home
                                 coverage
                                 ]}]
  {:pre [; these params are not allowed to be nil
         (and user device date
              creation-datetime
              geodiameter-in-km
              walking-distance-in-km
              active-time-in-seconds
              coverage)

         ]}

  (let [body (merge leave-return-home
                    {:geodiameter_in_km                  geodiameter-in-km
                     :walking_distance_in_km             walking-distance-in-km
                     :active_time_in_seconds             active-time-in-seconds
                     :steps                              steps
                     :max_gait_speed_in_meter_per_second gait-speed-in-meter-per-second
                     :gait_speed                         {:n_meters   (:n-meter-of-gait-speed @config)
                                                          :quantile   (:quantile-of-gait-speed @config)
                                                          :gait_speed gait-speed-in-meter-per-second
                                                          }
                     :coverage                           coverage
                     })]
    (mobility-datapoint user device "summary" date creation-datetime body)
    )
  )


(defn segments-datapoint [{:keys [user device date creation-datetime body]}]
  {:pre [
         ; these params are not allowed to be nil
         (and user device date creation-datetime body)
         ]}

  (mobility-datapoint user device "segments" date creation-datetime body)
  )

