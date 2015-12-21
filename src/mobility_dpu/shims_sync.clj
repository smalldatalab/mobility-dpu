(ns mobility-dpu.shims-sync
  (:use [mobility-dpu.protocols]
        [mobility-dpu.config]
        [aprint.core]
        [mobility-dpu.database]
        [mobility-dpu.datapoint])

  (:require [clj-http.client :as client]
            [clj-time.format :as f]
            [mobility-dpu.temporal :as temporal])
  (:import (org.joda.time.format ISODateTimeFormat DateTimeFormatter)))

(def authorizations-endpoint (str (:shim-endpoint @config) "/authorizations"))
(def data-endpoint (str (:shim-endpoint @config) "/data"))


(defn get-available-endpoints
  "return service/endpoints that the user has authorize"
  [user sync-tasks]
  (let [body (get-in (client/get authorizations-endpoint
                                 {:query-params     {"username" user}
                                  :as               :json
                                  :throw-exceptions false})
                     [:body])
        auths (map keyword (mapcat :auths body))

        ]
    (select-keys sync-tasks auths)

    )
  )

(defn extract-time [body]
  (let [time (or (get-in body [:effective_time_frame :time_interval :start_date_time])
                 (get-in body [:effective_time_frame :date_time]))
        ]
    (temporal/dt-parser time)
    )
  )

(defn to-datapoint
  "Convert data return by omh-shims to local data point"
  [user service schema body]
  (let [time (extract-time body)]
    (datapoint user "omh" (name schema) 1 0 (name service) "SENSED" time time body)
    )
  )

(defn to-datapoint-with-header
  "Convert data return by Shimmer to local data point"
  [user header body]
  (let [time (extract-time body)
        {:keys [name namespace version]} (get-in header [:schema_id])
        {:keys [source_name source_origin_id]} (get-in header [:acquisition_provenance])
        [major minor] (clojure.string/split version #"\.")
        ]
    (datapoint user namespace name
               (Integer/parseInt major) (Integer/parseInt minor)
               source_name "SENSED"
               time time body
               :source_origin_id source_origin_id)
    )
  )


(defn get-data
  "Send request to shims server to get the NORMALIZED data points.
  Return nil if the endpoint does not support normalization"
  [user service endpoint]
  (let [ret (client/get (str data-endpoint "/" (name service) "/" (name endpoint))
                        {:query-params     {"username"  user
                                            "normalize" "true"}
                         :as               :json
                         :throw-exceptions false})]
    (cond (= 200 (:status ret))
          (:body ret)
          ; Shims servcer will throw java.lang.UnsupportedOperationException if nomalization is not supported
          (.contains (:body ret) "java.lang.UnsupportedOperationException")
          nil
          :default
          (throw (Exception. (str ret)))
          )
    )
  )
(defn extract-datapoints [user responses service]
  (if (map? (first responses))
    ; for shimmer
    (for [{:keys [body header]} responses]
      ; convert the returned data point to the datapoint object that can be stored to the database
      (to-datapoint-with-header user header body)
      )
    ; for omh-shims
    (for [[schema datapoints] responses]
      (for [dp datapoints]
        ; convert the returned data point to the datapoint object that can be stored to the database
        (if dp
          (to-datapoint user service schema dp))
        )
      )))

(defn get-datapoints
  "Return normalized datapoints for the given users from the sync tasks"
  [user sync-tasks]
  (->>
    ; get service/endpoints (among the sync-tasks) this user has connected to
    (for [[service endpoints] (get-available-endpoints user sync-tasks)]
      ;for each endpoint
      (for [endpoint endpoints]
        ; get data points from the shims server
        (extract-datapoints user (:body (get-data user service endpoint)) service))

      )
    (flatten)
    (filter identity))
  )

