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



(defn test-db [file]
  (let [data (->> (with-open [rdr (clojure.java.io/reader
                                    (GZIPInputStream.
                                      (clojure.java.io/input-stream file)))]
                    (doall (json/parsed-seq rdr true))))
         _ (mg/drop-db (mg/connect) "test")
        db (mg/get-db (mg/connect) "test")
        ]
    (mc/remove db "testDataPoint")
    (doseq [dp data]
      (mc/save db "testDataPoint" (assoc dp :user_id "test"))
      )
    (database/mongodb db "testDataPoint")
    )
  )