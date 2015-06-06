(ns mobility-dpu.moves-sync
  (:require [clj-http.client :as client]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [mobility-dpu.datapoint :as datapoint]
            [mobility-dpu.algorithms :as algorithms]
            [mobility-dpu.throttle :as throttle])
  (:use [mobility-dpu.protocols]
        [mobility-dpu.config]
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


(def date-time-format (.withOffsetParsed ^DateTimeFormatter (ISODateTimeFormat/basicDateTimeNoMillis)))
(def date-format (ISODateTimeFormat/basicDate))
(defn- base-datapoint [user date zone storyline]
  {:user user
   :device  "Moves-App"
   :date  date
   :creation-datetime (if (:endTime (last storyline))
                        (DateTime/parse (:endTime (last (:segments storyline))) date-time-format)
                        (mobility-dpu.temporal/to-last-millis-of-day date zone))}
  )

(defn- get-auths
  "return the authorizations the user has"
  [user]
  (let [body (get-in (request-throttle #(client/get authorizations-endpoint {:query-params {"username" user}
                                                                         :as :json
                                                                         :throw-exceptions false}))
                        [:body])]
    (mapcat :auths body)
    )
  )
;;; The following two functions query Moves data from the shim server

(defn get-profile
  "get the user's Moves profile"
  [user]
  (let [profile (get-in (request-throttle #(client/get profile-endpoint {:query-params {"username" user}
                                                                        :as :json
                                                                        :throw-exceptions false}))
                        [:body :body :profile])]
    (if profile
      {:first-date (LocalDate/parse (:firstDate profile) date-format)
       :current-zone (DateTimeZone/forID (get-in profile [:currentTimeZone :id]))})
    )
  )

(defn- daily-storyline-sequence
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
          response (request-throttle #(client/get storyline-endpoint {:query-params {"username" user
                                                                                    "dateStart" start
                                                                                    "dateEnd" end}
                                                                     :as :json
                                                                     :throw-exceptions false
                                                                     }))
          storylines (get-in response [:body :body])
          storylines (map #(assoc % :zone zone) storylines)
          ]
      (if (= end til)
        storylines
        (concat storylines (lazy-seq
                             (daily-storyline-sequence user (t/plus start (t/days 7)) til zone))))
      )
    )
  )

;;; The following are summarizing functions


(def activity-mapping {"transport" :in_vehicle
                       "cycling" :on_bicycle
                       "running" :on_foot
                       "walking" :on_foot})

(def on-foot? #(= (get activity-mapping (:activity %)) :on_foot))

(defn- to-location-sample [{:keys [lat lon time]}]
  (->LocationSample (if time (DateTime/parse time date-time-format))
                    lat
                    lon
                    80
                    )
  )

(defn- filter-and-sum [sequence]  (apply + 0 (filter identity sequence)))

(defn- steps [activities] (filter-and-sum
                            (map :steps activities)))
(defn- active-distance [activities] (/ (filter-and-sum
                                         (map :distance
                                              (filter on-foot?  activities)))
                                       1000))
(defn- active-duration [activities] (filter-and-sum
                                      (map :duration
                                           (filter on-foot?  activities))))
(defn- geodiameter [segments]
  (let [locs (map to-location-sample (filter identity (map (comp :location :place) segments)))]
    (algorithms/geodiameter-in-km locs)
    ))

(defn- infer-home [segments possible-home-location]
  (let [place-segments (filter (comp :location :place) segments)
        episodes (map (fn [{:keys [startTime endTime place]}]
                        {:start (DateTime/parse startTime date-time-format)
                         :end (DateTime/parse endTime date-time-format)
                         :location (to-location-sample (:location place))}
                        ) place-segments)
        ]
    (if-let [episodes (seq episodes)]
      (algorithms/infer-home episodes possible-home-location)
      )))

(defn- gait-speed [activities]
  (let [traces (map :trackPoints activities)
        location-traces (for [trace traces]
                          (map to-location-sample trace)
                          )
        location-traces (filter seq location-traces)
        ]
    (if-let [location-traces (seq location-traces)]
      (algorithms/x-quantile-n-meter-gait-speed
        location-traces
        (:quantile-of-gait-speed @config)
        (:n-meters-of-gait-speed @config))
      )
    )
  )

(defn- coverage [date zone segments]
  (let [episodes (map (fn [{:keys [startTime endTime] :as segment}]
                        {:start (DateTime/parse startTime date-time-format)
                         :end (DateTime/parse endTime date-time-format)}
                        ) segments)
        ]
    (algorithms/coverage date zone episodes)
    )
  )


(defn- summarize [user date zone storyline]
  (let [daily-segments (:segments storyline)
        activities (filter on-foot? (mapcat :activities daily-segments))]
    (datapoint/summary-datapoint
      (merge (base-datapoint user date zone storyline)
             {:geodiameter-in-km      (geodiameter daily-segments)
              :walking-distance-in-km (active-distance activities)
              :active-time-in-seconds (active-duration activities)
              :steps (steps activities)
              :gait-speed-in-meter-per-second (gait-speed activities)
              :leave-return-home (infer-home daily-segments nil)
              :coverage (coverage date zone daily-segments)
              })
      )
    ))

(defn segments-datapoint [user date zone storyline]
  (mobility-dpu.datapoint/segments-datapoint
    (assoc (base-datapoint user date zone storyline)
      :body storyline))
  )



(defn get-datapoints
  ([user] (if (some #(= % "moves") (get-auths user))
            (get-datapoints user (daily-storyline-sequence user))))
  ([user [{:keys [date zone segments] :as storyline} & rest]]
      (if storyline
        (if (seq segments)
          (let [date (LocalDate/parse date date-format)              ]
            (concat
              [(segments-datapoint user date zone storyline)
               (summarize user date zone storyline)]
              (lazy-seq (get-datapoints user rest))
              )
            )
          (lazy-seq (get-datapoints user rest))
          )
        )))





