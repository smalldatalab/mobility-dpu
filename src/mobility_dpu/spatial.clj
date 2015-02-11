(ns mobility-dpu.spatial
  (:require [clj-time.core :as t]))

(defn haversine
  [{lon1 :longitude lat1 :latitude} {lon2 :longitude lat2 :latitude}]
  (let [R 6372.8 ; kilometers
        dlat (Math/toRadians (- lat2 lat1))
        dlon (Math/toRadians (- lon2 lon1))
        lat1 (Math/toRadians lat1)
        lat2 (Math/toRadians lat2)
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
      (let [time (:timestamp loc)
            next-time (:timestamp next-loc)
            accuracy (get-in loc [:location :accuracy])
            next-accuracy (get-in next-loc [:location :accuracy])]
        (if (< (t/in-millis (t/interval time next-time)) min-interval-millis)
          (if (< accuracy next-accuracy)
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
  [locs min-speed min-interval-millis]

  (let [
        ; downsample the location samples if two samples are too close in time
        locs  (filter-too-frequent-samples locs min-interval-millis)
        ; initialize the filter
        f (first locs)
        lat (get-in f [:location :latitude])
        lng (get-in f [:location :longitude])
        accuracy (get-in f [:location :accuracy])
        var (* accuracy accuracy)
        time (:timestamp f)
        init-ret [(-> f
                      (assoc-in [:location :filtered-latitude] lat)
                      (assoc-in [:location :filtered-longitude] lng)
                      )]
        ]
    (loop [[cur & locs] (rest locs) lat lat lng lng var var time time ret init-ret]
      (if cur
        (let [cur-lat (get-in cur [:location :latitude])
              cur-lng (get-in cur [:location :longitude])
              accuracy (get-in cur [:location :accuracy])
              speed (get-in cur [:location :speed])
              speed (max speed min-speed)
              cur-time  (:timestamp cur)]
          (let [time-diff (/ (t/in-millis (t/interval time cur-time)) 1000.0)
                var (+ var (* time-diff speed speed))
                k (/ var (+ var (* accuracy accuracy)))
                new-lat (+ lat (* k (- cur-lat lat)))
                new-lng (+ lng (* k (- cur-lng lng)))
                var (* (- 1 k) var)
                filtered-distance (haversine {:latitude lat :longitude lng}
                                             {:latitude new-lat :longitude new-lng})
                filtered-speed (* 1000
                                  (/ filtered-distance time-diff))]
            (recur locs new-lat new-lng  var cur-time
                   (conj ret (-> cur
                                 (assoc-in [:location :filtered-latitude] new-lat)
                                 (assoc-in [:location :filtered-longitude] new-lng)
                                 (assoc-in [:location :filtered-speed] filtered-speed)
                                 (assoc-in [:location :filtered-distance] filtered-distance)
                                 )))
            )

          )
        ret)
      )

    ))


(defn median-location
  [locs]
  (if (seq locs)
    (let [middle (/ (count locs) 2)
          lats (sort (map (comp :latitude :location) locs))
          lngs (sort (map (comp :longitude :location) locs))]
      {:latitude (nth lats middle)
       :longitude (nth lngs middle)}
      )
    nil)
  )