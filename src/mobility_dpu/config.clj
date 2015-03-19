(ns mobility-dpu.config)


(def config
  {:filter-walking-speed 8
   :n-meters-of-gait-speed 50
   :quantile-of-gait-speed 0.9
   :moves-shim-endpoint "http://localhost:8083/data/moves/"
   }
  )
