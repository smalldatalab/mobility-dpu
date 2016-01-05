(ns mobility-dpu.core-test
  (:require [clojure.test :refer :all]
            [mobility-dpu.shims-sync :refer :all]
            [cheshire.core :as json]
            [clj-time.coerce :as c]
            [schema.core :as s]
            [mobility-dpu.home-location :as home]
            [mobility-dpu.summary :as summary]
            [mobility-dpu.fake-db :refer [fake-db test-db]]
            [mobility-dpu.temporal :as temporal]
            [mobility-dpu.main :as main]
            [environ.core :refer [env]]
            )

  (:use     [mobility-dpu.protocols]
            [mobility-dpu.android]
            [mobility-dpu.config])
  )

(deftest test-env-config
  (testing "If environment config is used and correctly parsed"
    (is
      (= @config
         (merge
           default
           {
            ; mongodb stuff
            :mongodb-uri "mongodb://127.0.0.1/test"
            :log-file "test-file.log"
            :gmap-geo-coding-server-key "test-key"
            :shim-endpoint "test-endpoint"
            :sync-tasks {:test2 (list "TYPE1" "TYPE3") :test1 ["TYPE2"]}           ;"test2:type1,test1:type2,test2:type3"
            }))
      )
    )
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

(defn location-exist? [form]
  (let [found (atom false)]
    (clojure.walk/postwalk
      (fn [ele]
        (if (and (map? ele) (or (:latitude ele) (:longitude ele)))
          (reset! found ele)
          )
        )
      form
      )
    @found
    )
  )

(deftest test-hide-location
  (testing "If all locations are purged when hide-location? == true, or the vice versa"
    (let [db (fake-db (clojure.java.io/resource "android_mobility_samples.json.gz"))
          dps (seq
                (summary/get-datapoints
                  (->AndroidUserDatasource "test" db)
                  (home/provided-home-location "test" db)
                  true
                  ))

          ]

      (is (not (location-exist? dps)))
      )
    (let [db (fake-db (clojure.java.io/resource "android_mobility_samples.json.gz"))
          dps (seq
                (summary/get-datapoints
                  (->AndroidUserDatasource "test" db)
                  (home/provided-home-location "test" db)
                  false
                  ))
          ]
      (is (location-exist? dps))


      )
    ))



(deftest test-android-mobility
  (testing "If generate valid mobility data points from android mobility"
    (let [db (test-db (clojure.java.io/resource "android_mobility_samples.json.gz"))
          dps (seq
                (summary/get-datapoints
                  (->AndroidUserDatasource "test" db)
                  (home/provided-home-location "test" db)))]
      (doseq [dp dps]
        (save db dp)
        )
      )
    ))

(deftest test-purge-raw-trace
  (testing "If all locations are purged when hide-location? == true, or the vice versa"
    (let [db (test-db (clojure.java.io/resource "android_mobility_samples.json.gz"))
          source (->AndroidUserDatasource "test" db)

          ]
      (is (= 20 (count
                 (summary/get-datapoints
                   source
                   (home/provided-home-location "test" db)
                   false
                   ))))
      (main/sync-one-user "test" source true db)

      (is (= 2 (count
                 (summary/get-datapoints
                   source
                   (home/provided-home-location "test" db)
                   false
                   ))))
      )

    ))

