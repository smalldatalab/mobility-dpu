(defproject mobility-dpu "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main ^:skip-aot mobility-dpu.main
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}}
  :resource-paths ["/home/changun/Desktop/inflib.jar"]
  :plugins [[lein-localrepo "0.5.3"]]
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [aprint "0.1.1"]
                 [clj-time "0.7.0"]
                 [cheshire "5.4.0"]
                 [com.taoensso/timbre "3.3.1"]
                 [org.ocpsoft.prettytime/prettytime "3.2.7.Final"]
                 [com.novemberain/monger "2.0.1"]


                 ])
