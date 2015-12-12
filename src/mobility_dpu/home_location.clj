(ns mobility-dpu.home-location
  (use [mobility-dpu.protocols])
  (:require [schema.core :as s]
            [clj-http.client :as client])
  (:use [mobility-dpu.config])
  )



(s/defn geocoding :- (s/maybe Location)
  "Use google map geocoding api to get the location of the given address + zipcode"
  [address zipCode]
  (let [ret (client/get "https://maps.googleapis.com/maps/api/geocode/json"
                        {:query-params     {"address"  address
                                            "components" (str "postal_code:" zipCode)
                                            "key" (:gmap-geo-coding-server-key @config)}
                         :as               :json
                         :throw-exceptions true})

        ]
    (if (= 200 (:status ret))
      (let [{:keys [lat lng]} (-> (:body ret)
                                  :results
                                  (first)
                                  :geometry
                                  :location)
            ]
        (if (and lat lng)
          {:latitude lat
           :longitude lng})
        )
      (throw (Exception. (str ret)))
      )
    )
  )
(def mem-geocoding (memoize geocoding))

(s/defn provided-home-location :- (s/maybe Location)
  "Get the coordinates of the home location provided by the user through home-location-survey.
  Return nil if no home location is provided."
  [user db :- (s/protocol DatabaseProtocol)]
  (let [{:keys [streetNumberName zipCode]} (:body (last (query db "io.smalldata" "home-location-survey" user)))]
    (if (and streetNumberName zipCode)
        (mem-geocoding streetNumberName zipCode)
      )
    )
  )
