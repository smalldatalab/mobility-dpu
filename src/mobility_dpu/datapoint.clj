(ns mobility-dpu.datapoint
  (:require [clj-time.coerce :as c]
            [schema.core :as s])

  (:use [mobility-dpu.config]
        [mobility-dpu.protocols]))





(s/defn ^:always-validate datapoint :- DataPoint
  [user :- s/Str
   namespace :- s/Str
   schema :- s/Str
   version :- s/Int
   version-minor :- s/Int
   source :- s/Str
   modality :- s/Str
   unique-datapoint-id  :- s/Any
   creation-datetime  :- DateTimeSchema
   body & params]
  (let [{:keys [source_origin_id]} (apply hash-map params)
        id (clojure.string/join
             "_"
             [(str namespace "." schema)
              (str "v" version "." version-minor)
              user
              (clojure.string/lower-case source)
              unique-datapoint-id]
             )]
    {:_id     id
     :_class  "org.openmhealth.dsu.domain.DataPoint"
     :user_id user
     :header  {:id                             id,
               :schema_id                      {:namespace namespace,
                                                :name      schema,
                                                :version   {:major version, :minor version-minor}},
               :creation_date_time             creation-datetime,
               :creation_date_time_epoch_milli (c/to-long creation-datetime)
               :acquisition_provenance         (cond-> {:source_name source,
                                                        :modality    modality}
                                                       source_origin_id
                                                       (assoc :source_origin_id source_origin_id))},
     :body    body
     }
    )



  )


