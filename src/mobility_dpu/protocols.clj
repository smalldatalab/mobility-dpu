(ns
  mobility-dpu.protocols
  (:require [clj-time.core :as t]
            [schema.core :as s]
            [clj-time.coerce :as c]
            [clj-time.format :as f])
  (:import (org.joda.time LocalDate DateTimeZone)
           (org.joda.time.format ISODateTimeFormat)))

(def PossibleStates [:in_vehicle
                     :on_bicycle
                     :on_foot
                     :still
                     :unknown])
(def State (apply s/enum PossibleStates))

(def Latitude s/Num)
(def Longitude s/Num)
(def Accuracy s/Num)

(def Location
  {:latitude Latitude
   :longitude Longitude
   s/Any s/Any})

(defprotocol ActivitySampleProtocol
  (prob-sample-given-state [this state]
    "return P(this sample | Hidden State=state ),  namely, what is the probability of observing the sample given the real hidden state is s")
  )

(defprotocol TimestampedProtocol
  (timestamp [this]))

(s/defrecord LocationSample [timestamp :- (s/protocol t/DateTimeProtocol)
                             latitude :- Latitude
                             longitude :- Longitude
                             accuracy :- (s/maybe Accuracy)]
  TimestampedProtocol
  (timestamp [_] "The time when the location smaple is colleted" timestamp)
  )

(s/defrecord StepSample[start :- (s/protocol t/DateTimeProtocol)
                        end :- (s/protocol t/DateTimeProtocol)
                        step-count :- (s/pred #(>= % 0))]
  TimestampedProtocol
  (timestamp [_] "The time when the step smaple is colleted" start)
  )


(s/defrecord TraceData [activity-trace :- [(s/protocol ActivitySampleProtocol)]
                        location-trace :- [LocationSample]
                        step-trace :- [StepSample]])




(def Cluster
  (assoc Location
    :first-time (s/protocol t/DateTimeProtocol)
    :last-time (s/protocol t/DateTimeProtocol)
    :total-time-in-minutes s/Num
    :number-days s/Int
    :hourly-distribution [[(s/one s/Int "hour") (s/one s/Num "number of hours")]]
    ))



(def EpisodeSchema
  {:inferred-state State
   :start (s/protocol t/DateTimeProtocol)
   :end (s/protocol t/DateTimeProtocol)
   :trace-data TraceData
   (s/optional-key :cluster) Cluster
   (s/optional-key :home?) s/Bool
   ; distance in KM
   (s/optional-key :distance) s/Num
   (s/optional-key :calories) s/Num
   ; distance in seconds
   (s/optional-key :duration) s/Num

   (s/optional-key :raw-data) s/Any
   })



(def DateSchema (s/pred #(f/parse (ISODateTimeFormat/date) (str %)) "valid date"))
(def DateTimeSchema (s/pred #(f/parse (ISODateTimeFormat/dateTime) (str %)) "valid datetime"))

(def DayEpisodeGroup
  {:date     LocalDate
   :zone     DateTimeZone
   :episodes [EpisodeSchema]})

(def DataPoint
  {:header
            {:acquisition_provenance         {(s/optional-key :modality) #"SENSED",
                                              :source_name s/Str,
                                              (s/optional-key :source_origin_id) s/Str}
             :creation_date_time_epoch_milli s/Int,
             :creation_date_time             DateTimeSchema
             :schema_id                      {:version   {:minor s/Int, :major s/Int},
                                              :name      s/Str
                                              :namespace s/Str}
             :id                             s/Str}
   :user_id s/Str,
   :_class  #"org.openmhealth.dsu.domain.DataPoint",
   :_id     s/Str
   :body    s/Any
   })

(def SummaryDataPoint
  (assoc DataPoint
    :body
    {:longest_trek                       {:unit #"km" :value s/Num},
     :active_time                        {:unit #"sec" :value s/Num},
     :walking_distance                   {:unit #"km" :value s/Num},
     :home                               (s/maybe
                                           {
                                            (s/optional-key :leave_home_time)   DateTimeSchema,
                                            (s/optional-key :return_home_time)  DateTimeSchema,
                                            (s/optional-key :time_not_at_home)  {:unit #"sec" :value s/Num}
                                            })
     :date                               DateSchema,
     :device                             s/Str
     (s/optional-key :max_gait_speed)    {:unit #"mps" :value s/Num},
     (s/optional-key :gait_speed)        {:gait_speed s/Num, :quantile s/Num, :n_meters s/Num}
     :geodiameter                        {:unit #"km" :value s/Num},
     :step_count                         (s/maybe s/Num),
     :coverage                           s/Num,
     :episodes                           [s/Any]

     ;; for backward compatibility
     :geodiameter_in_km              s/Num
     :walking_distance_in_km         s/Num
     :active_time_in_seconds         s/Num
     :steps                          (s/maybe s/Num)
     :max_gait_speed_in_meter_per_second (s/maybe s/Num)
     :time_not_at_home_in_seconds (s/maybe s/Num)
     :leave_home_time (s/maybe DateTimeSchema)
     :return_home_time (s/maybe DateTimeSchema)
     })
  )
(def SegmentDataPoint
  (assoc DataPoint
    :body
    {:date     DateSchema
     :device   s/Str
     :episodes [s/Any]
     })
  )

(def MobilityDataPoint
  (s/conditional
    #(let [schema-name (get-in % [:header :schema_id :name])]
      (= schema-name "mobility-daily-summary")
      )
    SummaryDataPoint
    #(let [schema-name (get-in % [:header :schema_id :name])]
      (= schema-name "mobility-daily-segments")
      )
    SegmentDataPoint
    ))

(defrecord DataPointRecord [body timestamp]
  TimestampedProtocol
  (timestamp [_] timestamp)
  )

(s/defrecord Episode [inferred-state :- State
                      start :- (s/protocol t/DateTimeProtocol)
                      end :- (s/protocol t/DateTimeProtocol)
                      trace-data :- TraceData])

(defmulti trim (fn [main _ _]
                 (class main)
                 ))
(defmethod trim TraceData
  [data
   new-start
   new-end]
  (let [interval (t/interval new-start new-end)
        within #(or (t/within? interval (timestamp %)) (= new-end (timestamp %)))]
    (assoc data
      :activity-trace (filter within (:activity-trace data))
      :step-trace (filter within (:step-trace data))
      :location-trace (filter within (:location-trace data)))
    )
  )
(defmethod trim Episode
  [episode   new-start   new-end]
  (assoc episode
    :trace-data (trim (:trace-data episode) new-start   new-end)
    :start new-start
    :end new-end
    )
  )

(defmulti merge-two (fn [a b]
                 (assert (= (class a) (class b)) )
                  (class a)
                 ))
(defmethod merge-two TraceData
  [a b]
  (->TraceData
    (sort-by timestamp (apply concat (map :activity-trace [a b])))
    (sort-by timestamp (apply concat (map :location-trace [a b])))
    (sort-by timestamp (apply concat (map :step-trace [a b])))
    )
  )
(defmethod merge-two Episode
  [a b]
  (assoc (merge a b)
    :trace-data (merge-two (:trace-data a) (:trace-data b))
    :start (if (t/before? (:start a) (:start b))
             (:start a)
             (:start b)
             )
    :end (if (t/after? (:end a) (:end b))
           (:end a)
           (:end b)
           )
    )
  )

(defprotocol DatabaseProtocol
  (query [this schema-namespace schema-name user]
    "query data point of specific schema and user")
  (remove-until [this ns name user date]
    "remove data up to (including) certain date"
    )
  (last-time [this schema-namespace schema-name user]
    "query data point of specific schema and user")
  (save [this data]
    "Save the data point. Replace the existing data point with the same id.")
  (users [this]
    "Return distinct users")
  (purge-raw-data? [this user]
    "Return whether the user's raw data should be purged."
    )
  )

(defprotocol UserDataSourceProtocol
  (user [this] "Return the user name")
  (source-name [this] "Return the name of the data source")
  (step-supported? [this] "If the data source support the steps count")
  (extract-episodes [this])
  (last-update [this] "Return the last update time")
  (purge-raw-trace [this until-date] "Remove the raw data until (and including) the given date")
  )



