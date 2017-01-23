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



(def DateSchema (s/pred #(f/parse (ISODateTimeFormat/date) (str %)) "valid date"))
(def DateTimeSchema (s/pred #(f/parse (ISODateTimeFormat/dateTime) (str %)) "valid datetime"))


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

(defprotocol MalleableProtocol
  (trim [this start end])
  (merge-two [this that])
  )

(s/defrecord Episode [inferred-state :- State
                      start :- (s/protocol t/DateTimeProtocol)
                      end :- (s/protocol t/DateTimeProtocol)
                      steps :- (s/maybe [StepSample])
                      locations :- [LocationSample]]
  MalleableProtocol
  (trim [this
         new-start
         new-end]
    (let [interval (t/interval new-start new-end)
          within #(or (t/within? interval (timestamp %)) (= new-end (timestamp %)))]
      (assoc this
        :start new-start
        :end new-end
        :locations (filter within locations)
        :steps (filter within steps)
        )))
  (merge-two [a b]
    (assoc a
      :locations (sort-by timestamp (concat locations (:locations b)))
      :steps (sort-by timestamp (concat steps (:steps b)))
      :start (if (t/before? (:start a) (:start b))
               (:start a)
               (:start b)
               )
      :end (if (t/after? (:end a) (:end b))
             (:end a)
             (:end b)
             )
      ))

  )

(def EpisodeSchema
  {:inferred-state State
   :start (s/protocol t/DateTimeProtocol)
   :end (s/protocol t/DateTimeProtocol)
   :steps (s/maybe [StepSample])
   :locations [LocationSample]
   (s/optional-key :cluster) Cluster
   (s/optional-key :home?) s/Bool
   ; distance in KM
   (s/optional-key :distance) s/Num
   (s/optional-key :calories) s/Num
   ; distance in seconds
   (s/optional-key :duration) s/Num

   (s/optional-key :raw-data) s/Any
   })


(def CompactEpisodeSchema
  (assoc EpisodeSchema
    :steps [(s/one (s/eq ["step-count" "start" "end"]) :title)
            [(s/one s/Num :lat) (s/one DateTimeSchema :start) (s/one DateTimeSchema :end)]]
    :locations [(s/one (s/eq ["latitude" "longitude" "accuracy" "timestamp"]) :title)
                [(s/one s/Num :lat) (s/one s/Num :lng) (s/one s/Num :accuracy) (s/one DateTimeSchema :timestamp)]]
    ))



(def DayEpisodeGroup
  {:date     LocalDate
   :zone     DateTimeZone
   :episodes [s/Any]})

(def DataPoint
  {:header
            {:user_id s/Str,
             :acquisition_provenance         {(s/optional-key :modality) #"SENSED",
                                              :source_name s/Str,
                                              (s/optional-key :source_origin_id) s/Str}
             :creation_date_time_epoch_milli s/Int,
             :creation_date_time             DateTimeSchema
             :schema_id                      {:version   {:minor s/Int, :major s/Int},
                                              :name      s/Str
                                              :namespace s/Str}
             :id                             s/Str}

   :_class  #"org.openmhealth.dsu.domain.DataPoint",
   :_id     s/Str
   :body    s/Any
   })

(def SummaryDataPoint
  (assoc DataPoint
    :body
    {(s/optional-key :longest_trek)      {:unit #"km" :value s/Num},
     :active_time                        {:unit #"sec" :value s/Num},
     :walking_distance                   {:unit #"km" :value s/Num},
     :home                               (s/maybe
                                           {
                                            (s/optional-key :leave_home_time)   DateTimeSchema,
                                            (s/optional-key :return_home_time)  DateTimeSchema,
                                            (s/optional-key :time_not_at_home)  {:unit #"sec" :value s/Num}
                                            })
     :date                               DateSchema,
     :zone                               s/Str              ; timezone
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

(def SummaryDatePointV1
  (assoc DataPoint
    :body
    {(s/optional-key :longest-trek-in-km)  s/Num
     :active_time_in_seconds s/Num
     :walking_distance_in_km s/Num
     :steps (s/maybe s/Num)
     :coverage s/Num
     :date  DateSchema,
     (s/optional-key :home)  s/Any
     (s/optional-key :leave_home_time) (s/maybe DateTimeSchema)
     (s/optional-key :return_home_time) (s/maybe DateTimeSchema)
     (s/optional-key :time_not_at_home_in_seconds) s/Num
     :max_gait_speed_in_meter_per_second (s/maybe s/Num)
     :geodiameter_in_km s/Num
     :device s/Str,
     :gait_speed {:gait_speed (s/maybe s/Num), :quantile s/Num, :n_meters (s/maybe s/Num)}
     (s/optional-key :episodes) [s/Any]
     }
    :header s/Any

    ))

(def EpisodeDataPoint
  (assoc DataPoint
    :body
    {:date                               DateSchema,
     :zone                               s/Str              ; timezone
     :device                             s/Str
     :episodes [CompactEpisodeSchema]
     })
  )

(def MobilityDataPoint
  (s/conditional
    #(let [schema-name (get-in % [:header :schema_id :name])]
      (= schema-name "mobility-daily-summary")
      )
    SummaryDataPoint
    #(let [schema-name (get-in % [:header :schema_id :name])]
      (= schema-name "mobility-daily-episodes")
      )
    EpisodeDataPoint
    ))

(defrecord DataPointRecord [body timestamp]
  TimestampedProtocol
  (timestamp [_] timestamp)
  )





(defprotocol DatabaseProtocol
  (query [this schema-namespace schema-name user]
    "query data point of specific schema and user")
  (maintain [this]
    "create/maintain index")
  (remove-until [this ns name user date]
    "remove data up to (including) certain date"
    )
  (last-time [this schema-namespace schema-name user]
    "query data point of specific schema and user")
  (save [this data]
    "Save the data point. Replace the existing data point with the same id.")
  (users [this]
    "Return distinct users")
  (remove-gps? [this user]
    "Return whether the user's gps data should be purged."
    )
  (cache-episode [this user group data]
    "Cache intermidiate episode sequence"
    )
  (get-episode-cache [this user group]
    "Get cached intermidiate episode sequence")
  (offload-data [this schema-namespace schema-name user until]
    "Offload data from the main collection"
    )
  (study-users [this study]
    "Return users who participate in the given study")
  )

(defprotocol UserDataSourceProtocol
  (database [this] "The database of this source")
  (user [this] "Return the user name")
  (source-name [this] "Return the name of the data source")
  (step-supported? [this] "If the data source support the steps count")
  (extract-episodes [this])
  (last-update [this] "Return the last update time")
  (purge-raw-trace [this until-date] "Remove the raw data until (and including) the given date")
  )



