(defproject paysim-neo4j "0.1.0-SNAPSHOT"
  :description "Populates a Neo4j instance with the output of PaySim runs"
  :url "https://github.com/voutilad/paysim-neo4j"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.paysim/PaySim "2.0-voutilad-4"]
                 [org.neo4j.driver/neo4j-java-driver "4.0.0"]
                 [clojure.java-time "0.3.2"]]
  :main ^:skip-aot paysim-neo4j.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
