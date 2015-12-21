(ns mobility-dpu.core-test
  (:require [clojure.test :refer :all]
            [mobility-dpu.shims-sync :refer :all]
            [cheshire.core :as json]
            [clj-time.coerce :as c]
            [schema.core :as s]
            [mobility-dpu.home-location :as home]
            [mobility-dpu.summary :as summary]
            [mobility-dpu.fake-db :refer [fake-db]]
            [mobility-dpu.temporal :as temporal])

  (:use     [mobility-dpu.protocols]
            [mobility-dpu.android])
  )


(deftest shimmer-datapoint
  (testing "If convert shimmer response properly"
    (let [user "test"
          sample (json/parse-string
                   (slurp (clojure.java.io/resource "googlefit.json"))
                   true)
          id (clojure.string/join
               "_"
               [(str "omh.physical-activity")
                (str "v" 1 "." 0)
                user
                (clojure.string/lower-case "Google Fit API")
                "2015-10-09T23:53:08.610Z"
                ]
               )
          output (extract-datapoints
                   user
                   sample
                   "googlefit")
          ]
      (is
        (= (s/validate [DataPoint] output)
           (list
             {:_id     id
              :_class  "org.openmhealth.dsu.domain.DataPoint"
              :user_id user
              :header  {:id                             id,
                        :schema_id                      {:namespace "omh",
                                                         :name      "physical-activity",
                                                         :version   {:major 1, :minor 0}},
                        :creation_date_time             (temporal/dt-parser "2015-10-09T23:53:08.610Z"),
                        :creation_date_time_epoch_milli (c/to-long "2015-10-09T23:53:08.610Z")
                        :acquisition_provenance         {:source_name      "Google Fit API",
                                                         :modality         "SENSED"
                                                         :source_origin_id "derived:com.google.activity.segment:com.google.android.gms:LGE:Nexus 4:4b2a149e:from_sample<-derived:com.google.activity.sample:com.google.android.gms:LGE:Nexus 4:4b2a149e:detailed"}},
              :body    (:body (first sample))
              })

           ))

      )

    )
  )



(deftest android-mobility
  (testing "If generate valid mobility data points from android mobility"
    (let [db (fake-db (clojure.java.io/resource "android_mobility_samples.json.gz"))
          dps (seq
                (summary/get-datapoints
                  (->AndroidUserDatasource "test" db)
                  (home/provided-home-location "test" db)))]
      (doseq [dp dps]
        (save db dp)
        )
      )
    ))






