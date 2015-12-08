(ns mobility-dpu.main
  (:gen-class)
  (:require [taoensso.timbre :as timbre]
            [taoensso.timbre.appenders.core :as appenders]
            [mobility-dpu.temporal :refer [dt-parser]]
            [mobility-dpu.moves-sync :as moves]
            [mobility-dpu.shims-sync :as shims]
            )
  (:use [mobility-dpu.protocols]
        [mobility-dpu.android]
        [mobility-dpu.ios]
        [mobility-dpu.database]
        [mobility-dpu.config]
        [aprint.core]))

; config logger
(timbre/refer-timbre)
(timbre/merge-config!
  {:appenders {:spit (appenders/spit-appender {:fname (:log-file @config)})}})

(def db (mongodb "omh" "dataPoint"))
(defn -main
  "The application's main function"
  [& args]
  ; Create a new thread to sync moves data
  (future
    (loop []
      (doseq [user (or (seq args) (users db))]
        (try
          (doseq [dp (moves/get-datapoints user)]
            (info "Save data for " user " "
                  (get-in dp [:body :date]) " "
                  (get-in dp [:body :device]))
            (save db dp)
            )
          (catch Exception e (error e)))
        )
      (recur))
    )
  ; Create a new thread to sync other shims sync tasks
  (future
    (loop []
      (doseq [user (or (seq args) (users db))]
        (try
          (doseq [dp (shims/get-datapoints user (:sync-tasks @config))]

            (info "Save data for " user " "
                  (get-in dp [:header "acquisition_provenance" "source_name"]) " "
                  (get-in dp [:header "creation_date_time"]))
            (save db dp)

            )

          (catch Exception e (error e)))
        ; sleep to avoid polling service provider too fast
        ; FIXME use a more efficient throttle function?
        (Thread/sleep 5000)
        )
      ; sleep to avoid deplete the API quota
      (Thread/sleep (* 1000 60 15))

      (recur))
    )


  ; sync Android and iOS mobility
  (let [user-raw-data-counts (atom {})]
    (loop []
      (doseq [; run dpu for specifc users (if args are set) or all the users in the db
              user (or (seq args) (users db))
              ; functions to generate datapoints from different sources: Android, iOS, and Moves App
              source-fn [->AndroidUserDatasource ->iOSUserDatasource]]
        (let [source (source-fn user db)
              raw-data-count (count (raw-data source))]
          ; only compute new data points if there are new raw data that have been uploaded
          (if-not (= raw-data-count (get @user-raw-data-counts [user source-fn]))
            (try (doseq [datapoint (mobility/get-datapoints user (source-fn user db))]
                   (info "Save data for " user " "
                         (get-in datapoint [:body :date]) " "
                         (get-in datapoint [:body :device]))
                   (save db datapoint))
                 ; store number of raw data counts to check data update in the future
                 (swap! user-raw-data-counts assoc [user source-fn] raw-data-count)
                 (catch Exception e (error e)))
            (info "No new data for " user)
            )
          )
        )
      (recur)
      )
    )

  )









  (use 'mobility-dpu.database :reload)
  (use 'mobility-dpu.android :reload)



  (def dps
    (let [source (->AndroidUserDatasource
                   "google:108274213374340954232"
                   (mongodb "omh" "dataPoint"))]

      (mobility-dpu.summary/get-datapoints
        "google:108274213374340954232"
        (source-name source)
        (step-supported? source)
        (extract-episodes source))
      )
    )


