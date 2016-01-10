(ns mobility-dpu.fake-db
  (:require [cheshire.core :as json]
            [schema.core :as s]
            [mobility-dpu.database :as database]
            [clj-time.core :as t]
            [monger.core :as mg]
            [monger.collection :as mc])
  (:use [mobility-dpu.protocols])
  (:import (java.util.zip GZIPInputStream)
           ))

(defn fake-db [file]
  (let [data (atom (->> (with-open [rdr (clojure.java.io/reader
                                          (GZIPInputStream.
                                            (clojure.java.io/input-stream file)))]
                          (doall (json/parsed-seq rdr true)))))]
    (reify DatabaseProtocol
      (query [_ ns name user]
        (let [rows (filter #(= {:namespace ns :name name} (dissoc (get-in % [:header :schema_id]) :version)) @data)]
          (sort-by
            timestamp
            (for [row rows]
              (->DataPointRecord
                (:body row)
                (mobility-dpu.temporal/dt-parser
                  (get-in row [:header :creation_date_time])))))
          )

        )
      (remove-until [_ ns name user date]
        (reset! data (remove #(and
                               (= {:namespace ns :name name}
                                  (dissoc (get-in % [:header :schema_id]) :version))
                               (< (compare (get-in % [:header :creation_date_time]) (str (t/plus date (t/days 1)))) 0)
                               ) @data))
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

(defn test-db [file]
  (let [data (->> (with-open [rdr (clojure.java.io/reader
                                    (GZIPInputStream.
                                      (clojure.java.io/input-stream file)))]
                    (doall (json/parsed-seq rdr true))))
        db (mg/get-db (mg/connect) "test")
        ]
    (mc/remove db "testDataPoint")
    (doseq [dp data]
      (mc/save db "testDataPoint" (assoc dp :user_id "test"))
      )
    (database/mongodb db "testDataPoint")
    )
  )