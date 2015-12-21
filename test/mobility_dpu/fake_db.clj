(ns mobility-dpu.fake-db
  (:require [cheshire.core :as json]
            [schema.core :as s])
  (:use [mobility-dpu.protocols])
  (:import (java.util.zip GZIPInputStream)
           ))

(defn fake-db [file]
  (let [data (->> (with-open [rdr (clojure.java.io/reader
                                    (GZIPInputStream.
                                      (clojure.java.io/input-stream file)))]
                    (doall (json/parsed-seq rdr true))))]
    (reify DatabaseProtocol
      (query [_ ns name user]
        (let [rows (filter #(= {:namespace ns :name name} (dissoc (get-in % [:header :schema_id]) :version)) data)]
          (sort-by
            timestamp
            (for [row rows]
              (->DataPointRecord
                (:body row)
                (mobility-dpu.temporal/dt-parser
                  (get-in row [:header :creation_date_time])))))
          )

        )
      (last-time [this ns name user]
        (if-let [rows (seq (query this ns name user))]
          (timestamp (last rows))
          )
        )
      (save [_ data]
        (s/validate DataPoint data)
        )
      (users [_] ["test"])
      )
    )
  )