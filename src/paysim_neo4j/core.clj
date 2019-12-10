(ns paysim-neo4j.core
  (:import (org.paysim IteratingPaySim)
           (org.paysim.parameters Parameters)
           (org.neo4j.driver AuthTokens
                             Driver
                             GraphDatabase
                             Session
                             Query))
  (:gen-class))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Neo4j stuff

(def db-config {:uri "bolt://localhost:7687"
                :username "neo4j"
                :password "password"})

(defn connect!
  "Establish a Bolt connection to a Neo4j instance, returning a Driver."
  [{:keys [uri username password]}]
  (GraphDatabase/driver uri (AuthTokens/basic username password)))

(defn compose-query
  "Compose a Cypher Query that's passable to an open Session for execution."
  ([query]
   {:pre [(string? query)]}
   (Query. query))
  ([query m]
   {:pre [(string? query)
          (map? m)]}
   (Query. query (clojure.walk/stringify-keys m))))

(defn execute!
  [driver query]
  {:pre [driver (instance? Driver driver)
         query (instance? Query query)]}
  (with-open [sess (.session driver)]
    (let [result (.run sess query)]
      (.list result))))

(def init-schema-queries
  (map compose-query ["CREATE CONSTRAINT ON (c:Client) ASSERT c.name IS UNIQUE"
                      "CREATE CONSTRAINT ON (b:Bank) ASSERT b.name IS UNIQUE"
                      "CREATE CONSTRAINT ON (m:Merchant) ASSERT m.name IS UNIQUE"]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; PaySim stuff

(def batch-size 500)

(defn transaction->map
  [t]
  (hash-map :step (.getStep t)
            :action (.getAction t)
            :amount (.getAmount t)
            :source (.getNameOrig t)
            :source-type (str (.getOrigType t))
            :source-balance (.getNewBalanceOrig t)
            :dest (.getNameDest t)
            :dest-type (str (.getDestType t))
            :dest-balance (.getNewBalanceDest t)))

(def agent-cache (atom #{}))

(defn match-or-create-agent
  [agent-name agent-type ref-name]
  (if (contains? @agent-cache agent-name)
    (format "MATCH (%s:%s {name: '%s'})" ref-name agent-type agent-name)
    (do
      (swap! agent-cache conj agent-name)
      (format "CREATE (%s:%s {name: '%s'})" ref-name agent-type agent-name))))

(defn link)

(defn sim-and-load
  [driver params-file]
  (let [params (new Parameters params-file)
        sim (new IteratingPaySim params)]
    (.run sim)
    (let [txns (take 25 (iterator-seq sim))]
      (doseq [tx txns]
        (let [{:keys [step action amount source source-type
                      source-balance dest dest-type dest-balance]} (transaction->map tx)]
          (if (not (contains? @agent-cache source))
            (do (cond (= "CLIENT" source-type) (println "new client" source)
                      (= "MERCHANT" source-type) (println "new merchant" source))
                (swap! agent-cache conj source)))
          (if (not (contains? @agent-cache dest))
            (do (cond (= "CLIENT" dest-type) (println "new client" dest)
                      (= "MERCHANT" dest-type) (println "new merchant" dest))
                (swap! agent-cache conj dest))))))
    (println "Final: " @agent-cache)
    (.abort sim)))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (with-open [driver (connect! db-config)]
    (map #(execute! driver %) init-schema-queries)
    (sim-and-load driver "PaySim.properties")))
