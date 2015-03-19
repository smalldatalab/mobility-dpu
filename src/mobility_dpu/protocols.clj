(ns
  mobility-dpu.protocols
  (:require [clj-time.core :as t]))

(defprotocol TimestampedProtocol
  (timestamp [this]))

(defprotocol DatapointProtocol
  (body [this])
  )
(defprotocol ActivitySampleProtocol
  (prob-sample-given-state [this state])
  )

(defprotocol LocationSampleProtocol
  (latitude [this])
  (longitude [this])
  (accuracy [this])
  )

(defrecord LocationSample [timestamp latitude longitude accuracy]
  LocationSampleProtocol
  (latitude [_] latitude)
  (longitude [_] longitude)
  (accuracy [_] accuracy)
  TimestampedProtocol
  (timestamp [_] timestamp)
  )

(defprotocol EpisodeProtocol
  (state [this])
  (start [this])
  (end [this])
  (activity-trace [this])
  (location-trace [this])
  (trim [this new-start new-end])
  )
(defrecord Episode [inferred-state start end activity-samples location-samples]
  EpisodeProtocol
  (state [_] inferred-state)
  (start [_] start)
  (end [_] end)
  (trim [this new-start new-end]
    (let [interval (t/interval new-start new-end)
          within #(or (t/within? interval (timestamp %)) (= new-end (timestamp %)))]
      (Episode. (state this) new-start new-end
                (filter within (activity-trace this))
                (filter within (location-trace this))
                )
      )
    )
  (location-trace [_] location-samples)
  (activity-trace [_] location-samples)
  )

(defprotocol DatabaseProtocol
  (query [this schema-namespace schema-name user]
         "Return the name of the source this segmentor is designed for")
  (save [this data]
         "Save the data point. Replace the existing data point with the same id.")
  (users [this]
         "Return distinct")
  )


(defprotocol UserDataSourceProtocol
  (source-name [this] "Return the name of the data source")
  (activity-samples [this] "Return a list of all the raw activity samples")
  (location-samples [this] "Return a list of all the raw location samples")
  )



