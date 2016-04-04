(ns mobility-dpu.moves-sync
  (:require [clj-http.client :as client]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [mobility-dpu.throttle :as throttle]

            [schema.core :as s]
            [clojure.java.jdbc :as sql])
  (:use [mobility-dpu.protocols]
        [mobility-dpu.moves-convert]
        [mobility-dpu.config]
        [mobility-dpu.process-episode]
        [aprint.core])
  (:import (org.joda.time LocalDate DateTimeZone DateTime)
           (org.joda.time.format ISODateTimeFormat DateTimeFormatter)))



;;; Endpoints

(def authorizations-endpoint (delay (str (:shim-endpoint @config) "/authorizations")))
(def moves-shim (delay (str (:shim-endpoint @config) "/data/moves/")))
(def profile-endpoint (delay (str @moves-shim "profile")))
(def storyline-endpoint (delay (str @moves-shim "storyline")))
(def summary-endpoint (delay (str @moves-shim "summary")))


(def date-format (ISODateTimeFormat/basicDate))

(def request-throttle (let [t1 (throttle/create-throttle! (t/hours 1) 1400)
                            t2 (throttle/create-throttle! (t/minutes 1) 50)]
                        (fn [f]
                          (t1 #(t2 f))
                          )
                        ))

(defn client [endpoint params]
  (loop []
    (let [{:keys [body status] :as res}
          (request-throttle #(client/get
                              endpoint
                              (merge
                                params
                                {:as               :json
                                 :throw-exceptions false})))]
      (if
        (and (= 500 status)
               (.contains ^String body "429 Client Error (429)"))
       (do (Thread/sleep 60000)
           (recur))

       res)


      ))

  )



;;; The following two functions query Moves data from the shim server

(defn get-profile
  "get the user's Moves profile"
  [user]
  (let [profile (get-in (client @profile-endpoint {:query-params     {"username" user
                                                                     "normalize" "false"}
                                                  })
                        [:body :body :profile])]
    (if profile
      {:first-date   (LocalDate/parse (:firstDate profile) date-format)
       :current-zone (DateTimeZone/forID (get-in profile [:currentTimeZone :id]))})
    )
  )


(s/defn   daily-storyline-sequence :- [MovesData]
  "Query the storylines from the moves until the current date in the user's timezone"
  ([user] (let [{:keys [first-date current-zone] :as response} (get-profile user)]
            (if response
              (daily-storyline-sequence
                user first-date
                (c/to-local-date (t/to-time-zone (t/now) current-zone))
                current-zone)
              )
            ))
  ([user start til zone]

   (let [end (t/plus start (t/days 6))
         end (if (t/after? end til) til end)
         response (client @storyline-endpoint {:query-params     {"username"  user
                                                                 "dateStart" start
                                                                 "dateEnd"   end
                                                                 "normalize" "false"}

                                                  })
         storylines (->> (get-in response [:body :body])
                         (map #(assoc % :zone zone))
                         (s/validate [MovesData])
                         )

         ]
     (if (= end til)
       storylines
       (concat storylines (lazy-seq
                            (daily-storyline-sequence user (t/plus start (t/days 7)) til zone))))
     )
    )
  )

(s/defn   reverse-daily-summary-sequence
  "Query the Moves summaries in reverse chronolgical order (with batch size = 31 days)
   until the first date of the user indicated in the profile"
  ([user] (let [{:keys [first-date current-zone] :as response} (get-profile user)]
            (if response
              (reverse-daily-summary-sequence
                user first-date
                (c/to-local-date (t/to-time-zone (t/now) current-zone))
                )
              )
            ))
  ([user first-date to]

    (let [from (t/minus to (t/days 30))
          start (if (t/before? from first-date) first-date from)
          response (client @summary-endpoint {:query-params     {"username"  user
                                                                "dateStart" start
                                                                "dateEnd"   to
                                                                "normalize" "false"}

                                                 })
          summaries (->> (reverse (get-in response [:body :body])))
          ]
      (if (= start first-date)
        summaries
        (concat summaries (lazy-seq
                             (reverse-daily-summary-sequence user first-date (t/minus to (t/days 31))))))
      )
    )
  )





(defn- get-auths
  "return the authorizations the user has"
  [user]
  (let [{:keys [body]} (client @authorizations-endpoint {:query-params     {"username" user}})]
    (mapcat :auths body)
    )
  )

(defn auth? [user]
  ((into #{} (get-auths user)) "moves"))

(defn last-update-time [user]
  (if-let [update-time (->>
                         (reverse-daily-summary-sequence user)
                         (map :lastUpdate)
                         (remove nil?)
                         (first)) ]
    (DateTime/parse update-time date-time-format)
    ))

(defn status [user]
  (if (and (auth? user))
    {:token? true
     :token-valid? (not (nil? (get-profile user)))
     :last-time (last-update-time user)
     }
    {:token? false}
    )
  )

(s/defn ^:always-validate moves-extract-episodes :- (s/maybe [EpisodeSchema])
  [user :- s/Str]
  (if (auth? user)
    (->> (daily-storyline-sequence user)
         (mapcat :segments)
         (mapcat segment->episodes)

         (group-by #(select-keys % [:start :inferred-state]))
         (map (comp last second))
         (sort-by :start)
         ))
  )

(defrecord MovesUserDatasource [user]
  UserDataSourceProtocol
  (source-name [_] "Moves-App")
  (user [_] user)
  (extract-episodes [_]
    (moves-extract-episodes user))
  (step-supported? [_]
    true)
  (last-update [_]
    (if (auth? user)
      (last-update-time user)
      )

    )
  )

; code for query Moves token status
(comment
  (def db-spec (delay {:connection-uri (@config :admin-dashboard-jdbc-uri)}))
  (def report
    (->>
      (sql/query
        @db-spec
        ["
              SELECT username, users.id as id
              FROM users
              INNER JOIN study_participants ON study_participants.user_id = users.id
              INNER JOIN studies ON study_participants.study_id = studies.id
              WHERE studies.name = ?;" "HSSJan2015testpilot"])
      (map (fn [%] (merge % (status (:username %)))))

      ))
  (->>
    (for [{:keys [username id token? token-valid? last-time]} report]
      (format "%s,%s,%s,%s,%s" username id token? token-valid? last-time)
      )
    (cons "username,id,token?,token-valid?,last-time")
    (clojure.string/join "\n")
    )
  )