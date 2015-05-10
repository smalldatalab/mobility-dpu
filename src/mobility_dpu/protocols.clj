(ns
  mobility-dpu.protocols
  (:require [clj-time.core :as t]))

(defprotocol TimestampedProtocol
  (timestamp [this]))
(defprotocol DatapointProtocol
  (body [this])
  )
(defprotocol ActivitySampleProtocol
  (prob-sample-given-state [this s]
  "return P(Hidden State=s | this sample),  namely, given observing this sample, what is the probability of hidden state being s")
  )

(defprotocol LocationSampleProtocol
  (latitude [this])
  (longitude [this])
  (accuracy [this] "accuracy of the location sample in meters")
  )

(defrecord LocationSample [timestamp latitude longitude accuracy]
  LocationSampleProtocol
  (latitude [_] latitude)
  (longitude [_] longitude)
  (accuracy [_] accuracy)
  TimestampedProtocol
  (timestamp [_] "The time when the location smaple is colleted" timestamp)
  )

(defprotocol EpisodeProtocol
  "An episode represents a period of time in which the user is in a certain mobility state"
  (state [this] "The mobility state of this episode")
  (start [this] "start time")
  (end [this] "end time")
  (activity-trace [this] "activity datapoints")
  (location-trace [this] "location datapoints")
  (trim [this new-start new-end] "trim the period of the episode to the new timeframe")
  )
(defrecord Episode [inferred-state start end activity-samples location-samples]
  EpisodeProtocol
  (state [_] inferred-state)
  (start [_] start)
  (end [_] end)
  (trim [this new-start new-end]
    "Trim the episode and keep the traces that are within the new timeframe"
    (let [interval (t/interval new-start new-end)
          within #(or (t/within? interval (timestamp %)) (= new-end (timestamp %)))]
      (Episode. (state this) new-start new-end
                (filter within (activity-trace this))
                (filter within (location-trace this))
                )
      )
    )
  (location-trace [_] location-samples)
  (activity-trace [_] activity-samples)
  )

(defprotocol DatabaseProtocol
  (query [this schema-namespace schema-name user]
         "query data point of specific schema and user")
  (save [this data]
         "Save the data point. Replace the existing data point with the same id.")
  (users [this]
         "Return distinct")
  )


(defprotocol UserDataSourceProtocol
  (source-name [this] "Return the name of the data source")
  (activity-samples [this] "Return a list of all the raw activity samples")
  (location-samples [this] "Return a list of all the raw location samples")
  ; (steps-samples [this] "Return a list of all the step count samples")
  )



