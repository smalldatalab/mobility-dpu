(ns mobility-dpu.datapoint
  (:require [clj-time.coerce :as c]
            [schema.core :as s])
  (:import (org.joda.time ReadableInstant ReadablePartial DateTimeZone)
           (clojure.lang IPersistentMap IPersistentCollection Keyword))
  (:use [mobility-dpu.config]
        [mobility-dpu.protocols]))





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

(defn datapoint [user namespace schema version source modality time-for-id creation-datetime body]
  (let [version version
        id (str schema "-v" version "-" user "-" time-for-id "-" (clojure.string/lower-case source))]
    (time->str
      {:_id     id
       :_class  "org.openmhealth.dsu.domain.DataPoint"
       :user_id user
       :header  {:id                             id,
                 :schema_id                      {:namespace namespace,
                                                   :name      schema,
                                                   :version   {:major version, :minor 0}},
                 :creation_date_time             creation-datetime,
                 :creation_date_time_epoch_milli (c/to-long creation-datetime)
                 :acquisition_provenance         {:source_name source,
                                                  :modality    modality}},
       :body    body
       })
    )
  )


