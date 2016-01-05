(defproject mobility-dpu "0.4.2-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}
             ; to test environment-based config
             :test {:jvm-opts ["-Dmongodb.uri=mongodb://127.0.0.1/test"
                               "-Dlog.file=test-file.log"
                               "-Dgmap.geo.coding.server.key=test-key"
                               "-Dshim.endpoint=test-endpoint"
                               "-Dsync.tasks=test2:type1,test1:type2,test2:type3"
                               ]
                    }}
  :jvm-opts ["-Xmx256m" "-server"]
  :main mobility-dpu.main
  :plugins [[lein-localrepo "0.5.3"]
            [lein-environ "1.0.1"]]

  :dependencies [[org.clojure/clojure "1.6.0"]
                 [aprint "0.1.1"]
                 [clj-time "0.7.0"]
                 [cheshire "5.4.0"]
                 [com.taoensso/timbre "4.1.4"]
                 [org.ocpsoft.prettytime/prettytime "3.2.7.Final"]
                 [com.novemberain/monger "2.0.1"]
                 [clj-http "1.0.1"]
                 [org.apache.commons/commons-math3 "3.5"]
                 [prismatic/schema "1.0.3"]
                 [environ "1.0.1"]
                 ])
