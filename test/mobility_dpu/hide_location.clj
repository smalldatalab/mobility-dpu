(ns mobility-dpu.hide-location
  (:require [mobility-dpu.summary :as summary]
            [mobility-dpu.home-location :as home]
            [mobility-dpu.main :as main]
            [clojure.test :refer :all])
  (:use     [mobility-dpu.protocols]
            [mobility-dpu.android]
            [mobility-dpu.config]
            [mobility-dpu.fake-db]))


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
                  (let [db (test-db (clojure.java.io/resource "android_mobility_samples.json.gz"))
                        dps (seq
                              (summary/get-datapoints
                                (->AndroidUserDatasource "test" db)
                                (home/provided-home-location "test" db)
                                true
                                ))

                        ]

                    (is (not (location-exist? dps)) "Locations should NOT exists")
                    )
                  (comment (let [db (test-db (clojure.java.io/resource "android_mobility_samples.json.gz"))
                                 dps (seq
                                       (summary/get-datapoints
                                         (->AndroidUserDatasource "test" db)
                                         (home/provided-home-location "test" db)
                                         false
                                         ))
                                 ]
                             (is (location-exist? dps) "Locations should exists")


                             ))
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
