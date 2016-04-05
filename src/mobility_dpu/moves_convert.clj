(ns mobility-dpu.moves-convert
  (:require [schema.core :as s])
  (:use [mobility-dpu.protocols])
  (:import (org.joda.time DateTime DateTimeZone)
           (org.joda.time.format DateTimeFormatter ISODateTimeFormat)))

;;; Moves data schema

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
  {:zone                          DateTimeZone
   :date                          MovesDate
   :summary                       [s/Any]
   :segments                      [Segment]
   (s/optional-key :caloriesIdle) s/Num
   (s/optional-key :lastUpdate)   MovesTimestemp
   }
  )


;;; Moves data to Mobility Data Conversion Functions

(def date-time-format (.withOffsetParsed ^DateTimeFormatter (ISODateTimeFormat/basicDateTimeNoMillis)))


(def location-sample-accuracy 50)
(def activity-mapping {"transport" :in_vehicle
                       "cycling"   :on_bicycle
                       "running"   :on_foot
                       "walking"   :on_foot})

(s/defn activity->episode :- EpisodeSchema
        "Convert single activity instance in Moves record to Mobility episode"
        [{:keys [group startTime endTime steps distance calories trackPoints duration] :as raw-data} :- MovesActivity]

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


(s/defn activities->episodes :- [EpisodeSchema]
        "Convert mutiple activity instances in Moves record to mutiple Mobility episodes"
        [activities :- [MovesActivity]]
        (->>
          activities
          ; only convert activity instances that have time and are associated with a general activity group
          (filter #(and (:group %) (:startTime %) (:endTime %)))
          (map activity->episode))
        )


(defmulti segment->episodes :type)

(s/defmethod segment->episodes "place" :- [EpisodeSchema]
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

(s/defmethod segment->episodes "move" :- [EpisodeSchema]
             [{:keys [activities]} :- MovingSegment]
             (activities->episodes activities))

(s/defmethod segment->episodes "off" :- [EpisodeSchema]
             [{:keys [activities]} :- MovingSegment]
             (activities->episodes activities))


