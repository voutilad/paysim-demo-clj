(ns paysim-neo4j.core
  (:require [clojure.pprint :as pp])
  (:import (org.paysim IteratingPaySim)
           (org.paysim.parameters Parameters)
           (org.neo4j.driver AuthTokens
                             Config
                             Config$TrustStrategy
                             Driver
                             GraphDatabase
                             Session
                             Query))
  (:gen-class))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Neo4j stuff

(def db-config {:uri "bolt://localhost:443"
                :username "neo4j"
                :password "password"})

(def strategy (Config$TrustStrategy/trustAllCertificates))

(def cfg (-> (Config/builder)
              (.withEncryption)
              ;(.withTrustStrategy strategy)
              (.build)))

(defn connect!
  "Establish a Bolt connection to a Neo4j instance, returning a Driver."
  [{:keys [uri username password]}]
  (GraphDatabase/driver uri (AuthTokens/basic username password) cfg))

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
            :fraud (.isFraud t)
            :flagged-fraud (.isFlaggedFraud t)
            :source (hash-map :name (.getNameOrig t)
                              :type (str (.getOrigType t))
                              :balance (.getNewBalanceOrig t))
            :dest (hash-map :name (.getNameDest t)
                            :type (str (.getDestType t))
                            :balance (.getNewBalanceDest t))))


;; Cache by actor name
(def agent-cache (atom {}))

(defn match-or-create-agent
  [agent-name agent-type ref-name]
  (if (contains? @agent-cache agent-name)
    (format "MATCH (%s:%s {name: '%s'})" ref-name agent-type agent-name)
    (do
      (swap! agent-cache conj agent-name)
      (format "CREATE (%s:%s {name: '%s'})" ref-name agent-type agent-name))))


(defn sim-and-load
  [driver params-file]
  (let [params (new Parameters params-file)
        sim (new IteratingPaySim params)]
    (.run sim)
    (let [txns (eduction (comp (take 5) (map transaction->map)) (iterator-seq sim))]
      (doseq [tx txns]
        (pp/pprint tx)))
    (.abort sim)))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (with-open [driver (connect! db-config)]
    (map #(execute! driver %) init-schema-queries)
    (sim-and-load driver "PaySim.properties")))
