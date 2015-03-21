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



(defn datapoint [user device type date creation-datetime body]
  (let [id (str "mobility-daily-" type "-" user "-" date "-" (clojure.string/lower-case device))]
    (time->str
      {:_id id
       :_class  "org.openmhealth.dsu.domain.DataPoint"
       :user_id user
       :header { "_id" id,
                "schema_id" { "namespace"  "cornell",
                             "name"  (str "mobility-daily-" type),
                             "version" { "major"  1, "minor"  0 } },
                "creation_date_time" creation-datetime,
                "creation_date_time_epoch_milli" (c/to-long creation-datetime)
                "acquisition_provenance" { "source_name" (str "Mobility-DPU-v1.0-" device),
                                          "modality" "SENSED" } },
       :body (assoc body :device device  :date date)
       })
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
  {:pre [
         ; these params are not allowed to be nil
         (and user device date
              creation-datetime
              geodiameter-in-km
              walking-distance-in-km
              active-time-in-seconds
              coverage)

         ]}

  (let [body (merge leave-return-home
                    {:geodiameter_in_km geodiameter-in-km
                     :walking_distance_in_km walking-distance-in-km
                     :active_time_in_seconds active-time-in-seconds
                     :steps steps
                     :max_gait_speed_in_meter_per_second gait-speed-in-meter-per-second
                     :gait_speed {:n_meters (:n-meter-of-gait-speed config)
                                  :quantile (:quantile-of-gait-speed config)
                                  :gait_speed gait-speed-in-meter-per-second
                                  }
                     :coverage coverage
                     })]
    (datapoint user device "summary" date creation-datetime body)
    )
  )


(defn segments-datapoint [{:keys [user device date creation-datetime body]}]
  {:pre [
         ; these params are not allowed to be nil
         (and user device date creation-datetime body)
         ]}

  (datapoint user device "segments" date creation-datetime body)
  )

