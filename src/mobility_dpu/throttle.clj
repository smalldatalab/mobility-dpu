(ns mobility-dpu.throttle
  (:require [clj-time.core :as t]
            [taoensso.timbre :as timbre])
  (:import (org.joda.time ReadablePeriod)
           (clojure.lang PersistentQueue)))
(timbre/refer-timbre)
(defn create-throttle!
  "Create a rate limiting throttler which maintains that the number of requests in any time interval of the given
  length (period) won't execeed the desired limit. The request times are recorded in a vector atom, and each time when
  a new request is issue, the throttler will check if the number of the most recent requsts has exceeded the limit.
  If so, the throttler will sleep until the time a new request is allowed."
  [^ReadablePeriod period limit]
  {:pre ([(isa? period ReadablePeriod) (> limit 0)])}
  (let [instances (atom (PersistentQueue/EMPTY))]
    (fn [f]
      (let [time-threshold (t/minus (t/now) period)
            latest-instances (loop [queue @instances]       ; pop the queue til time threshold
                               (if (and (seq queue) (t/before? (first queue) time-threshold))
                                 (recur (pop queue))
                                 queue
                                 ))
            count (count latest-instances)]
        (when (>= count limit)
          (let [sleep (t/in-millis (t/interval time-threshold (first latest-instances)))]
            (debug (str "Sleep " sleep " for throttle " period " " limit " " " now count:" count))
            (Thread/sleep sleep))
          )
        (reset! instances (conj latest-instances (t/now)))
        (f)
        )
      )
    )
  )
