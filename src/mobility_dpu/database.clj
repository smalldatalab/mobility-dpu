(ns mobility-dpu.database
  (:require [monger.core :as mg]
            [monger.query :as mq]
            [monger.collection :as mc]

            [mobility-dpu.temporal]
            [schema.core :as s])
  (:use [mobility-dpu.config]
        [mobility-dpu.protocols])
  (:import (mobility_dpu.protocols DatabaseProtocol DataPointRecord)))





(def validator (s/validator DataPoint))
(defn mongodb
  "Create a MongoDB-backed DatabaseProtocol"
  [db coll]
  (let [conn (mg/connect (:mongodb @config))
        db (mg/get-db conn db)]
    (reify DatabaseProtocol
      (query [_ ns name user]
        (for [row (mq/with-collection db coll
                                      (mq/find {"header.schema_id.name"      name
                                                "header.schema_id.namespace" ns
                                                :user_id                     user})
                                      (mq/keywordize-fields true)
                                      (mq/sort {"header.creation_date_time_epoch_milli" 1})
                                      )]
          (DataPointRecord.
            (:body row)
            (mobility-dpu.temporal/dt-parser
              (get-in row [:header :creation_date_time])))
          ))
      (last-time [_ ns name user]
        (let [row (->> (mq/with-collection db coll
                                           (mq/find {"header.schema_id.name"      name
                                                     "header.schema_id.namespace" ns
                                                     :user_id                     user})
                                           (mq/keywordize-fields true)
                                           (mq/sort {"header.creation_date_time_epoch_milli" -1})
                                           (mq/limit 1)
                                           )
                       (first)
                       )]
          (if row (mobility-dpu.temporal/dt-parser
                    (get-in row [:header :creation_date_time])))
          )
        )
      (save [_ data]
        (mc/save db coll (validator data))
        )
      (users [_] (mc/distinct db "endUser" "_id" {}))
      ))
  )
