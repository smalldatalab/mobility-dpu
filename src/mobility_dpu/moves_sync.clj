(ns mobility-dpu.moves-sync
  (:require [clj-http.client :as client]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [mobility-dpu.throttle :as throttle]

            [schema.core :as s])
  (:use [mobility-dpu.protocols]
        [mobility-dpu.config]
        [mobility-dpu.process-episode]
        [aprint.core])
  (:import (org.joda.time LocalDate DateTimeZone DateTime)
           (org.joda.time.format ISODateTimeFormat DateTimeFormatter)))


(def authorizations-endpoint (delay (str (:shim-endpoint @config) "/authorizations")))
(def moves-shim (delay (str (:shim-endpoint @config) "/data/moves/")))
(def profile-endpoint (delay (str @moves-shim "profile")))
(def storyline-endpoint (delay (str @moves-shim "storyline")))
(def summary-endpoint (delay (str @moves-shim "summary")))
(def location-sample-accuracy 50)


(def MovesTimestemp #"\d{8}T\d{6}([-+]\d{4})|Z")
(def MovesDate #"\d{8}")
(def MovesActitivyGroup (s/enum "walking" "running" "cycling" "transport"))
(def MovesLocation
  {:lat s/Num, :lon s/Num})
(def MovesTrackPoint
  (assoc MovesLocation  :time MovesTimestemp))
(def MovesActivity
  {(s/optional-key :group)       MovesActitivyGroup,
   (s/optional-key :startTime)   MovesTimestemp,
   (s/optional-key :endTime)     MovesTimestemp,
   (s/optional-key :steps)        s/Num,
   ; the distance in meters
   (s/optional-key :distance)    s/Num
   (s/optional-key :calories) s/Num
   :trackPoints [MovesTrackPoint],
   ; the duration in seconds
   :duration    s/Num,
   :manual      s/Bool,
   :activity    s/Str
   s/Any s/Any
   })

(def PlaceSegment
  {:type       #"place",
   :startTime  MovesTimestemp,
   :endTime    MovesTimestemp,
   :place      {:type s/Str,

                :location {:lat s/Num, :lon s/Num}
                (s/optional-key :id) s/Num,
                (s/optional-key :foursquareId) s/Str
                (s/optional-key :facebookPlaceId) s/Str
                (s/optional-key :foursquareCategoryIds) [s/Str]
                (s/optional-key :name) s/Str
                },
   (s/optional-key :activities) [MovesActivity],
   :lastUpdate MovesTimestemp})

(def MovingSegment
  {:type (s/enum "move" "off"),
   :startTime  MovesTimestemp,
   :endTime    MovesTimestemp,
   (s/optional-key :activities) [MovesActivity],
   :lastUpdate MovesTimestemp})

(def Segment
  (s/conditional
    #(= (:type %) "move")
    MovingSegment
    #(= (:type %) "off")
    MovingSegment
    #(= (:type %) "place")
    PlaceSegment
    )
  )

(def MovesData
  {:zone DateTimeZone
   :date MovesDate
   :summary [s/Any]
   :segments [Segment]
   (s/optional-key :caloriesIdle) s/Num
   (s/optional-key :lastUpdate) MovesTimestemp
   }
  )




(def date-time-format (.withOffsetParsed ^DateTimeFormatter (ISODateTimeFormat/basicDateTimeNoMillis)))
(def date-format (ISODateTimeFormat/basicDate))

(defn client [endpoint params]
  (loop []
    (let [{:keys [body status] :as res}
          (client/get
            endpoint
            (merge
              params
              {:as               :json
               :throw-exceptions false}) )]
      (if (and (= 500 status)
               (.contains ^String body "429 Client Error (429)"))
        ; making requests too frequently
       (do (Thread/sleep 60000)
           (recur))
       res)


      ))

  )


(defn- get-auths
  "return the authorizations the user has"
  [user]
  (let [body (get-in (client @authorizations-endpoint {:query-params     {"username" user}
                                                      })
                     [:body])]
    (mapcat :auths body)
    )
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
       :current-zone (DateTimeZone/forID (clojure.string/replace
                                           (get-in profile [:currentTimeZone :id])
                                           ; Moves return the wrong timezone id.... replace Asia/Yangon with Asia/Rangoon
                                           "Yangon" "Rangoon"))})
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
    (loop [start start results []]
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
            results  (into results storylines)
            ]
        (if (= end til)
          results
          (recur (t/plus start (t/days 7)) results))


        ))

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


(def activity-mapping {"transport" :in_vehicle
                       "cycling"   :on_bicycle
                       "running"   :on_foot
                       "walking"   :on_foot})

(s/defn ^:always-validate activity->episode :- EpisodeSchema [{:keys [group startTime endTime steps distance calories trackPoints duration] :as raw-data} :- MovesActivity]
  (let [startTime (DateTime/parse startTime date-time-format)
        endTime (DateTime/parse endTime date-time-format)
        location-trace (map (fn [{:keys [lat lon time]}] (->LocationSample (DateTime/parse time date-time-format) lat lon location-sample-accuracy)) trackPoints)
        episode (assoc (->Episode (activity-mapping group) startTime endTime
                                  (if steps [(->StepSample startTime endTime steps)])
                                  location-trace) :raw-data raw-data)

        ]
    (cond->
      episode
      distance (assoc :distance (/ distance 1000.0))
      calories (assoc :calories calories)
      duration (assoc :duration duration)
      ))
  )

(defmulti segment->episodes :type)

(s/defn activities->episodes :- [EpisodeSchema]
  [activities :- [MovesActivity]]
  (->>
    activities
    (filter #(and (:group %) (:startTime %) (:endTime %)))
    (map activity->episode))
  )
(s/defmethod ^:always-validate segment->episodes "place" :- [EpisodeSchema]

   [{:keys [startTime endTime place activities] :as raw-data} :- PlaceSegment]
     (let [startTime (DateTime/parse startTime date-time-format)
       endTime (DateTime/parse endTime date-time-format)
       {:keys [lon lat]} (:location place)
         ]
   (cons (assoc (->Episode :still startTime endTime
                           nil
                           [(->LocationSample startTime lat lon location-sample-accuracy)
                            (->LocationSample endTime lat lon location-sample-accuracy)
                            ]
                           )
           :raw-data raw-data)
         (activities->episodes activities)))
             )

(s/defmethod ^:always-validate segment->episodes "move" :- [EpisodeSchema]
   [{:keys [activities]} :- MovingSegment]
             (activities->episodes activities))

(s/defmethod ^:always-validate segment->episodes "off" :- [EpisodeSchema]
             [{:keys [activities]} :- MovingSegment]
             (activities->episodes activities))

(defn auth? [user]
  ((into #{} (get-auths user)) "moves"))

(defn remove-duplicate [episodes]
  (loop [[epi & rest-epis] (sort-by :start episodes)
         key->index (transient {})
         results [] ]
    (if epi
      (let [key (select-keys epi [:start :inferred-state])]
        (if-let [index (key->index key)]
          ; replace the previous instance
          (recur rest-epis key->index (assoc results index epi))
          ; add to the end of the vector
          (recur rest-epis (assoc! key->index key (count results)) (conj results epi))
          )
        )
      results)
    )
  )
(s/defn ^:always-validate moves-extract-episodes :- (s/maybe [EpisodeSchema])
  [user :- s/Str]
  (if (auth? user)
    (->> (daily-storyline-sequence user)
         (mapcat :segments)
         (mapcat segment->episodes)
         remove-duplicate
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
      (if-let [update-time (:lastUpdate (first (filter :lastUpdate (reverse-daily-summary-sequence user))))]
        (DateTime/parse update-time date-time-format)
        ))

    )
  )







