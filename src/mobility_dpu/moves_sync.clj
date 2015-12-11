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


(def request-throttle (let [t1 (throttle/create-throttle! (t/hours 1) 1400)
                            t2 (throttle/create-throttle! (t/minutes 1) 50)]
                        (fn [f]
                          (t1 #(t2 f))
                          )
                        ))
(def authorizations-endpoint (str (:shim-endpoint @config) "/authorizations"))
(def moves-shim (str (:shim-endpoint @config) "/data/moves/"))
(def profile-endpoint (str moves-shim "profile"))
(def storyline-endpoint (str moves-shim "storyline"))
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



(defn- base-datapoint [user date zone storyline]
  {:user              user
   :device            "Moves-App"
   :date              date
   :creation-datetime (if (:endTime (last storyline))
                        (DateTime/parse (:endTime (last (:segments storyline))) date-time-format)
                        (mobility-dpu.temporal/to-last-millis-of-day date zone))}
  )

(defn- get-auths
  "return the authorizations the user has"
  [user]
  (let [body (get-in (request-throttle #(client/get authorizations-endpoint {:query-params     {"username" user}
                                                                             :as               :json
                                                                             :throw-exceptions false}))
                     [:body])]
    (mapcat :auths body)
    )
  )
;;; The following two functions query Moves data from the shim server

(defn get-profile
  "get the user's Moves profile"
  [user]
  (let [profile (get-in (request-throttle #(client/get profile-endpoint {:query-params     {"username" user}
                                                                         :as               :json
                                                                         :throw-exceptions false}))
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
         response (request-throttle #(client/get storyline-endpoint {:query-params     {"username"  user
                                                                                        "dateStart" start
                                                                                        "dateEnd"   end}
                                                                     :as               :json
                                                                     :throw-exceptions false
                                                                     }))
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


(def activity-mapping {"transport" :in_vehicle
                       "cycling"   :on_bicycle
                       "running"   :on_foot
                       "walking"   :on_foot})

(s/defn ^:always-validate activity->episode :- EpisodeSchema [{:keys [group startTime endTime steps distance calories trackPoints duration] :as raw-data} :- MovesActivity]
  (let [startTime (DateTime/parse startTime date-time-format)
        endTime (DateTime/parse endTime date-time-format)
        location-trace (map (fn [{:keys [lat lon time]}] (->LocationSample (DateTime/parse time date-time-format) lat lon location-sample-accuracy)) trackPoints)
        trace (->TraceData [] location-trace (if steps
                                               [(->StepSample startTime endTime steps)]
                                               []
                                               ) )
        episode (assoc (->Episode (activity-mapping group) startTime endTime trace) :raw-data raw-data)

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
       trace (->TraceData [] [(->LocationSample startTime lat lon location-sample-accuracy)
                              (->LocationSample endTime lat lon location-sample-accuracy)
                              ] [])]
   (cons (assoc (->Episode :still startTime endTime trace) :raw-data raw-data)
         (activities->episodes activities)))
             )

(s/defmethod ^:always-validate segment->episodes "move" :- [EpisodeSchema]
   [{:keys [activities]} :- MovingSegment]
             (activities->episodes activities))

(s/defmethod ^:always-validate segment->episodes "off" :- [EpisodeSchema]
             [{:keys [activities]} :- MovingSegment]
             (activities->episodes activities))


(s/defn ^:always-validate moves-extract-episodes :- (s/maybe [EpisodeSchema])
  [user :- s/Str]
  (if ((into #{} (get-auths user)) "moves")
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
  (source-name [_] "Movesp-App")
  (user [_] user)
  (extract-episodes [_]
    (moves-extract-episodes user))
  (step-supported? [_]
    true)
  (last-update [_]
    nil
    )
  )







