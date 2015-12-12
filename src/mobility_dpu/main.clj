(ns mobility-dpu.main
  (:gen-class)
  (:require [taoensso.timbre :as timbre]
            [taoensso.timbre.appenders.core :as appenders]
            [mobility-dpu.temporal :refer [dt-parser]]
            [mobility-dpu.shims-sync :as shims]
            [mobility-dpu.summary :as summary]
            [mobility-dpu.home-location :as home]
            [clj-time.core :as t]
            [schema.core :as s]
            )
  (:use [mobility-dpu.protocols]
        [mobility-dpu.android :only [->AndroidUserDatasource]]
        [mobility-dpu.ios :only [->iOSUserDatasource]]
        [mobility-dpu.moves-sync :only [->MovesUserDatasource]]
        [mobility-dpu.database :only [mongodb]]
        [mobility-dpu.config :only [config]]
        [aprint.core]))

; config logger
(timbre/refer-timbre)
(timbre/merge-config!
  {:appenders {:spit (appenders/spit-appender {:fname (:log-file @config)})}})

(def db (mongodb "omh" "dataPoint"))
(defn -main
  "The application's main function"
  [& args]

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


  ; sync Android and iOS mobility and Moves
  (let [user-source->last-update (atom {})]
    (loop []
      (doseq [; run dpu for specifc users (if args are set) or all the users in the db
              user (or (seq args) (users db))
              ; functions to generate datapoints from different sources: Android, iOS, and Moves App
              source-fn [#(->AndroidUserDatasource % db)
                         #(->iOSUserDatasource % db)
                         #(->MovesUserDatasource %)]]
        (let [source (source-fn user)
              last-raw-data-update-time (last-update source)
              last-process-time (@user-source->last-update  [user source-fn])]
          ; only compute new data points if there are new raw data that have been uploaded
          (if
            (or (nil? last-raw-data-update-time)
                (nil? last-process-time)
                (t/after? last-raw-data-update-time last-process-time)
                )
            (try
              (let [provided-home-loc (home/provided-home-location user db)]
                (if provided-home-loc
                  (info (str "User " user " provided home location:" provided-home-loc)))
                  (doseq [datapoint (summary/get-datapoints
                                      (source-fn user)
                                      provided-home-loc)]
                    (info "Save data for " user " "
                          (get-in datapoint [:body :date]) " "
                          (get-in datapoint [:body :device]))
                    (save db (s/validate MobilityDataPoint datapoint)))

                  ; store number of raw data counts to check data update in the future
                  (swap! user-source->last-update assoc [user source-fn] last-raw-data-update-time)
                )
                 (catch Exception e (error e)))
            (info "No new data for " user)
            )
          )
        )
      (recur)
      )
    )

  )



