(ns paysim-neo4j.core
  (:require [clojure.pprint :as pp]
            [cli-matic.core :refer [run-cmd]]
            [clojure.string :as s]
            [java-time :as time]
            [paysim-neo4j.db :as db])
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
;;
;; Neo4j stuff

(def init-schema-queries
  (map db/compose-query ["CREATE CONSTRAINT ON (c:Client) ASSERT c.name IS UNIQUE"
                         "CREATE CONSTRAINT ON (m:Mule) ASSERT m.name IS UNIQUE"
                         "CREATE CONSTRAINT ON (b:Bank) ASSERT b.name IS UNIQUE"
                         "CREATE CONSTRAINT ON (m:Merchant) ASSERT m.name IS UNIQUE"
                         "CREATE CONSTRAINT ON (c:CashIn) ASSERT c.id IS UNIQUE"
                         "CREATE CONSTRAINT ON (c:CashOut) ASSERT c.id IS UNIQUE"
                         "CREATE CONSTRAINT ON (d:Debit) ASSERT d.id IS UNIQUE"
                         "CREATE CONSTRAINT ON (p:Payment) ASSERT p.id IS UNIQUE"
                         "CREATE CONSTRAINT ON (t:Transfer) ASSERT t.id IS UNIQUE"
                         "CREATE CONSTRAINT ON (tx:Transaction) ASSERT tx.id IS UNIQUE"
                         "CREATE INDEX ON :Transaction(globalStep)"
                         "CREATE INDEX ON :CashIn(globalStep)"
                         "CREATE INDEX ON :CashOut(globalStep)"
                         "CREATE INDEX ON :Debit(globalStep)"
                         "CREATE INDEX ON :Payment(globalStep)"
                         "CREATE INDEX ON :Transfer(globalStep)"]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; PaySim wrapper and processing stuff

(def start-date (time/zoned-date-time 2019 10 1))

(defn transaction->map
  [t]
  (if (nil? t) nil
      (hash-map :step (.getStep t)
                :global-step (.getGlobalStep t)
                :action (.getAction t)
                :amount (.getAmount t)
                :fraud (.isFraud t)
                :flagged-fraud (.isFlaggedFraud t)
                :source (hash-map :name (.getNameOrig t)
                                  :type (str (.getOrigType t))
                                  :balance (.getNewBalanceOrig t))
                :dest (hash-map :name (.getNameDest t)
                                :type (str (.getDestType t))
                                :balance (.getNewBalanceDest t)))))

(def tx-keys #{:global-step :step :action :amount :fraud :flagged-fraud :source :dest})
(defn tx?
  [tx]
  (and (some? tx)
       (map? tx)
       (every? some? (map #(get tx %) tx-keys))
       (and (map? (:source tx)) (every? some? (map #(get (:source tx) %) [:name :type :balance])))
       (and (map? (:dest tx)) (every? some? (map #(get (:dest tx) %) [:name :type :balance])))))

(defn tx->label
  "Convert a transaction type to a proper label"
  [tx]
  (let [{:keys [action]} tx]
    (s/join "" (map s/capitalize (s/split action #"_")))))

(defn step->ts
  [step]
  (time/plus start-date (time/hours step)))

(defn ts->daystring
  "Generate a localized date string from a timestamp"
  [ts]
  (time/format "YYYY_MM_dd" ts))

(def query-pattern
  (s/join "\n" ["MERGE (s:~STYPE~ { name: $senderName })"
                "MERGE (r:~RTYPE~ { name: $receiverName })"
                "CREATE (tx:Transaction:~XTYPE~ { id: $txId })"
                "SET tx.ts = $ts, tx.amount = $amount, tx.fraud = $fraud,"
                "    tx.step = $step, tx.globalStep = $globalStep"
                "CREATE (s)-[:PERFORMED]->(tx)"
                "CREATE (tx)-[:TO]->(r)"]))

(def threading-query-pattern
  "
UNWIND $rows AS row
  MATCH (c:Client {name: row.name})-[:PERFORMED]->(tx:Transaction)-[:TO]-() WHERE NOT (c)-[:FIRST_TX]->()
  WITH c, collect(tx) AS txs
  WITH c, txs, head(txs) AS _start, last(txs) AS _last

  MERGE (c)-[:FIRST_TX]->(_start)
  MERGE (c)-[:LAST_TX]->(_last)
  WITH c, apoc.coll.pairsMin(txs) AS pairs

  UNWIND pairs AS pair
    WITH pair[0] AS a, pair[1] AS b
    MERGE (a)-[n:NEXT]->(b)
    RETURN COUNT(n)")

(defn build-merge-query
  [tx]
  (let [{:keys [amount source dest step]} tx]
    (-> query-pattern
        (s/replace #"~STYPE~" (s/capitalize (:type source)))
        (s/replace #"~RTYPE~" (s/capitalize (:type dest)))
        (s/replace #"~XTYPE~" (tx->label tx))
        ;(s/replace #"~DATE~" (ts->daystring (step->ts step)))
        )))

(defn tx->props
  "Convert a transaction to a map of properties"
  [tx]
  (let [{:keys [amount dest fraud flagged-fraud source step global-step]} tx
        senderName (:name source)
        receiverName (:name dest)
        txId (.toString (java.util.UUID/randomUUID))
        ts (step->ts step)]
    {:amount amount :fraud fraud :flagged-fraud flagged-fraud
     :senderName senderName :receiverName receiverName
     :txId txId :ts ts
     :step step :globalStep global-step}))

(defn naive-query
  "Generate a naive Neo4j query to populate the labels and relationships from a Transaction"
  ([tx]
   (let [query (build-merge-query tx)
         props (tx->props tx)]
     (db/compose-query query props))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Simulation Routines and Settings
;;

;; Somewhat arbitrarily chosen...not really optimized.
(def default-batch-size 1000)
(def default-sim-queue-depth 100000)

(def transducer-pipeline
  (comp (map transaction->map)               ; convert to a clj map
        (filter tx?)                         ; filter non-compliant data
        (partition-all default-batch-size))) ; partition the stream into batches

(defn sim-and-load!
  "Step through an IteratingPaySim, loading data as we go"
  [driver params-file]
  (let [params (new Parameters params-file)
        sim (new IteratingPaySim params default-sim-queue-depth)]
    (.run sim)
    (let [batches (eduction transducer-pipeline (iterator-seq sim))]
      (doseq [batch batches]
        (apply db/write! driver (map naive-query batch))))
    (try (.abort sim) (catch Exception e))))

(defn get-client-names!
  "Get all names of clients known in our database. For default settings, it's ~20k."
  [session]
  (let [tx (comp (map #(.asMap %))
                 (map #(hash-map :name (get % "c.name"))))
        result (.run session "MATCH (c:Client) RETURN c.name")]
    (transduce tx conj (iterator-seq result))))

(defn thread-transactions!
  "String all the transaction events together temporally in a chain"
  [driver]
  (with-open [session (.session driver)]
    ;; First make sure our Mules are also Clients...it's a data issue
    (.run session "MATCH (m:Mule) SET m :Client RETURN m.name")
    ;; Batch iterate over the client names, threading them in groups
    (let [client-names (get-client-names! session)]
      (doseq [batch (partition-all 1000 client-names)]
        (let [query (db/compose-query threading-query-pattern {:rows (vec batch)})]
          (.writeTransaction session (db/single-query-txn query)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Main routines and CLI config

(defn run-threading!
  [opts]
  (with-open [driver (db/connect! opts)]
    (println "Threading transactions into nice chains...")
    (time (thread-transactions! driver))))

(defn run-sim!
  [opts]
  (with-open [driver (db/connect! opts)]
    ;; make sure we have a valid schema with constraints
    (println "Initializing schema constraints...")
    (doseq [q init-schema-queries] (db/execute! driver q))
    (println "Running simulation and loading data...")
    (time (sim-and-load! driver "PaySim.properties"))
    (println "Threading transactions into nice chains...")
    (time (thread-transactions! driver))))

(def cli-configuration
  {:app {:command "paysim-neo4j"
         :description "Run a PaySim simulation and populate a target Neo4j database."
         :version "0.1.0"}
   :global-opts [{:option "uri"
                  :as "Bolt URI to a target Neo4j instance"
                  :type :string
                  :default "bolt://localhost:7687"}
                 {:option "username"
                  :short "u"
                  :as "Username for connecting to Neo4j"
                  :type :string
                  :default "neo4j"}
                 {:option "password"
                  :short "p"
                  :as "Password for Neo4j user"
                  :type :string
                  :default "password"}
                 {:option "tls"
                  :as "Use TLS (encryption) for connecting to Neo4j?"
                  :type :flag
                  :default false}
                 {:option "trust-all-certs"
                  :as "Trust all certificates? (Warning: only use for local certs you trust.)"
                  :type :flag
                  :default false}]
   :commands [{:command "run"
               :description "Run all tasks: init schema, simulate/load transactions,  and thread them."
               :runs run-sim!}
              {:command "thread"
               :description "Thread existing transactions based on global step counter"
               :runs run-threading!}]})

(defn -main
  "Simulate and load into Neo4j"
  [& args]
  (run-cmd args cli-configuration))
