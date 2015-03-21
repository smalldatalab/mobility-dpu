(ns mobility-dpu.spatial
  (:require [clj-time.core :as t])
  (:use [mobility-dpu.protocols]
        [aprint.core])
  (:import (mobility_dpu.protocols LocationSample)))

(defn haversine
  [a b]
  (let [R 6372.8 ; kilometers
        dlat (Math/toRadians (- (latitude b) (latitude a)))
        dlon (Math/toRadians (- (longitude b) (longitude a)))
        lat1 (Math/toRadians (latitude a))
        lat2 (Math/toRadians (latitude b))
        a (+ (* (Math/sin (/ dlat 2)) (Math/sin (/ dlat 2))) (* (Math/sin (/ dlon 2)) (Math/sin (/ dlon 2)) (Math/cos lat1) (Math/cos lat2)))]
    (* R 2 (Math/asin (Math/sqrt a)))))



;
;var initLoc = locations[0];
;var time = moment.parseZone(initLoc.timestamp).valueOf();
;var lat = initLoc.location.latitude;
;var lng = initLoc.location.longitude;
;initLoc.location.filterLatitude = lat;
;initLoc.location.filterLongitude = lng;
;var accuracy = initLoc.location.horizontal_accuracy ? initLoc.location.horizontal_accuracy : initLoc.location.accuracy;
;var variance = accuracy * accuracy;
;
;for(var i=1; i<locations.length; i++){
;        var cur = locations[i];
;        var speed =  cur.location.speed > 4 ? cur.location.speed : 4;
;var newLat = cur.location.latitude;
;var newLng = cur.location.longitude;
;var newTime = moment.parseZone(cur.timestamp).valueOf();
;var newAccuracy = cur.location.horizontal_accuracy ? cur.location.horizontal_accuracy : cur.location.accuracy;
;var timediff =  newTime - time;
;variance += timediff * speed * speed / 1000.0;
;// Kalman gain matrix K = Covarariance * Inverse(Covariance + MeasurementVariance)
;var k = variance / (variance + (newAccuracy * newAccuracy))
;lat += k * (newLat - lat);
;lng += k * (newLng - lng);
;time = newTime;
;cur.location.filterLatitude = lat;
;cur.location.filterLongitude = lng;
;variance = (1-k) * variance;
;
;return locations;

(defn filter-too-frequent-samples
  "Filter too frequent sample and favor the one with better accuracy."
  [locs min-interval-millis]
  (loop [[loc next-loc & rest-locs] locs ret []]
    (if next-loc
      (let [time (timestamp loc)
            next-time (timestamp next-loc)
            cur-accuracy (accuracy loc)
            next-accuracy (accuracy next-loc)]
        (if (< (t/in-millis (t/interval time next-time)) min-interval-millis)
          (if (< cur-accuracy next-accuracy)
            (recur rest-locs (conj ret loc))
            (recur (concat [next-loc] rest-locs) ret)
            )
          (recur (concat [next-loc] rest-locs) (conj ret loc))
          )
        )
      (conj ret loc)
      )
    ))


(defn kalman-filter
  "Apply kalman filter to the given location samples
  with minimun speed and minimun interval.
  See: http://stackoverflow.com/a/15657798"
  [locs speed min-interval-millis]

  (let [
        ; downsample the location samples if two samples are too close in time
        locs  (filter-too-frequent-samples locs min-interval-millis)
        ; initialize the filter
        head (first locs)
        head-accuracy (accuracy head)
        ]
    (loop [[cur & locs] (rest locs) lat (latitude head) lng (longitude head)
           var (* head-accuracy head-accuracy) time (timestamp head) ret [head]]
      (if cur
        (let [cur-lat (latitude cur)
              cur-lng (longitude cur)
              cur-accuracy (accuracy cur)
              cur-time  (timestamp cur)]
          (let [time-diff (/ (t/in-millis (t/interval time cur-time)) 1000.0)
                var (+ var (* time-diff speed speed))
                k (/ var (+ var (* cur-accuracy cur-accuracy)))
                new-lat (+ lat (* k (- cur-lat lat)))
                new-lng (+ lng (* k (- cur-lng lng)))
                var (* (- 1 k) var)]
            (recur locs new-lat new-lng var cur-time
                   (conj ret (LocationSample. cur-time new-lat new-lng (Math/sqrt var))))
            )

          )
        ret)
      )

    ))

(defn trace-distance [location-trace]
  (loop [origin (first location-trace) [cur & rest] (rest location-trace) sum 0]
    (if cur (recur cur rest (+ sum (haversine origin cur)))
            sum)
    )
  )
(defn median-location
  [locs]
  (if (seq locs)
    (let [middle (/ (count locs) 2)
          times (sort (map timestamp locs))
          lats (sort (map latitude locs))
          lngs (sort (map longitude locs))
          accu (sort (map accuracy locs))]
      (->LocationSample (nth times middle)
                        (nth lats middle)
                        (nth lngs middle)
                        (nth accu middle)
                        )
      )
    nil)
  )