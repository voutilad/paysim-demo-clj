(defproject paysim-neo4j "0.1.0"
  :description "Populates a Neo4j instance with the output of PaySim runs"
  :url "https://github.com/voutilad/paysim-neo4j"
  :license {:name "GPL 3.0"
            :url "https://www.gnu.org/licenses/gpl-3.0.en.html"}
  :repositories [["local-paysim-jar" {:url "file:repo" :username "" :password "" :checksum :ignore}]]
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.paysim/paysim "2.0-voutilad-5"]
                 [org.neo4j.driver/neo4j-java-driver "4.0.0"]
                 [clojure.java-time "0.3.2"]
                 [cli-matic "0.3.11"]]
  :main ^:skip-aot paysim-neo4j.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
