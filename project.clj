(defproject zabbix-tee-riemann "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :main zabbix-tee-riemann.core
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [io.netty/netty-all "4.1.16.Final"]
                 [org.clojure/data.json "0.2.6"]]
  :profiles {:uberjar {:aot :all}})
