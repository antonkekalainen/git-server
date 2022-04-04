(defproject git-server "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/core.match "1.0.0"]
                 [org.clojure/java.jdbc "0.7.12"]
                 [org.xerial/sqlite-jdbc "3.36.0.3"]
                 [org.postgresql/postgresql "42.3.3"]
                 [compojure "1.6.1"]
                 [ring/ring-defaults "0.3.2"]]
  ;; For profiling
  :plugins [[lein-ring "0.12.5"]]
  :ring {:handler git-server.handler/app}
  :profiles
  {:dev {:dependencies [[javax.servlet/servlet-api "2.5"]
                        [ring/ring-mock "0.3.2"]]}
   :uberjar {:ring {:port 3000}}})
