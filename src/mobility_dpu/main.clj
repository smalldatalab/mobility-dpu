(ns mobility-dpu.main
  (:gen-class
    :name mobility_dpu.api
    :methods [#^{:static true} [movesDatapoints [String] String]
              #^{:static true} [latestMovesDatapoint [String] org.joda.time.DateTime]])
  (:require [taoensso.timbre :as timbre]
            [taoensso.timbre.appenders.core :as appenders]
            [mobility-dpu.temporal :refer [dt-parser]]
            [mobility-dpu.shims-sync :as shims]
            [mobility-dpu.summary :as summary]
            [mobility-dpu.home-location :as home]
            [clj-time.core :as t]
            [schema.core :as s]
            [cheshire.core :as json])
  (:use [mobility-dpu.protocols]
        [mobility-dpu.android :only [->AndroidUserDatasource]]
        [mobility-dpu.ios :only [->iOSUserDatasource]]
        [mobility-dpu.moves-sync :only [->MovesUserDatasource]]
        [mobility-dpu.database :only [mongodb time->str]]
        [mobility-dpu.config :only [config]]
        [aprint.core])
  )

;; config logger
(timbre/refer-timbre)


;; tracking the last update time
(def user-source->last-update (atom {}))

(defn sync-shims [db users]
  (doseq [user users]
    (try
      (doseq [dp (shims/get-datapoints user (:sync-tasks @config))]

        (info "Save data for " user " "
              (get-in dp [:header :acquisition_provenance :source_name]) " "
              (get-in dp [:header :creation_date_time]))
        (save db dp)

        )

      (catch Throwable e (error e)))
    ; sleep to avoid polling service provider too fast
    ; FIXME use a more efficient throttle function?
    (Thread/sleep 5000)
    )
  )

(defn sync-one-user [user data-source purge-gps? db]
  (let [start-time (t/now)
        provided-home-loc (home/provided-home-location user db)]
    (let [datapoints (summary/get-datapoints
                       data-source
                       provided-home-loc
                       )
          ; purge GPS data
          datapoints (cond->>
                       datapoints
                       purge-gps?
                       (map summary/dissoc-locations))]
      (if (seq datapoints)
        (let [dates (->>
                      datapoints
                      (map (comp :date :body))
                      (distinct)
                      (sort)
                      )]
          (doseq [datapoint datapoints]
            ; now purge gps
            (save db (s/validate MobilityDataPoint datapoint))
            )
          (info (format "Save data for %s %s %s-%s Elapsed time %ss (Remove Gps? %s)"
                        user
                        (source-name data-source)
                        (first dates)

                        (last dates)
                        (t/in-seconds (t/interval start-time (t/now)))
                        purge-gps?
                        )
                )
          :success
          )
        )
      )
    )
  )


(defn sync-data-sources [db data-sources users]
  (doseq [; run dpu for specifc users (if args are set) or all the users in the db
          user users
          ; functions to generate datapoints from different sources: Android, iOS, and Moves App
          source-fn data-sources]
    (let [source (source-fn user)
          last-raw-data-update-time (try (last-update source) (catch Exception _))
          last-process-time (get @user-source->last-update  [user (source-name source)])
          purge-gps? (remove-gps? db user)
          ]

      ; only compute new data points if there are new raw data that have been uploaded
      (if
        (or (nil? last-raw-data-update-time)
            (nil? last-process-time)
            (t/after? last-raw-data-update-time last-process-time)
            )
        ;; process data
        (try
          (when (sync-one-user user source purge-gps? db)
            ; store the last update time
            (swap! user-source->last-update assoc [user (source-name source)] last-raw-data-update-time))
          (catch Throwable e
            (error  e (str "Sync failed: user " user " " (source-name source) " " last-raw-data-update-time))
            ))
        ;; do not process data, print out reason
        (info (format "Skip user %s %s Last Raw Data: %s Last processed: %s" user (source-name source) last-raw-data-update-time last-process-time))
        )
      )
    ))


(defn -main
  "The application's main function"
  [& args]

  (timbre/merge-config!
    {:appenders {:spit (appenders/spit-appender {:fname (:log-file @config)})}})

  (info "Run with config:" @config)
  (info "This version do not check remove GPS option on admindashboard")
  (info "Connecting to database")

  (let [db (loop []
             (if-let [db (try (mongodb)
                          (catch Throwable _
                            (info "Waiting for mongodb" (@config :mongodb-uri))
                            (Thread/sleep 1000)
                            nil
                            ))]
               db
               (recur)
               ))
        get-users #(or (seq args) (users db))]
    (info "Create indexes if they did not exist ...")
    (maintain db)
    (loop []
      (try
        (sync-data-sources
          db
          [#(->MovesUserDatasource %)]
          (get-users))
        (catch Throwable e
          (Thread/sleep 1000)
          (warn e)
          ))
            (recur)
      )
  )
  )


;; TODO: Figure out how to let the caller specify the mongo uri and shim server url
(defn -movesDatapoints [user-id]
  (let [db (mongodb)
        provided-home-loc (home/provided-home-location user-id db)
        data-source (->MovesUserDatasource user-id)
        datapoints (summary/get-datapoints data-source  provided-home-loc)]

    (json/generate-string (time->str datapoints))
    )
  )


(defn -latestMovesDatapoint [user-id]
  (let [data-source (->MovesUserDatasource user-id)]
    (last-update data-source)
    )
  )
