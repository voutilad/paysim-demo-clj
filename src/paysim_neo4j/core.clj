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
;; Neo4j stuff

(def db-config {:uri "bolt://localhost:7687"
                :username "neo4j"
                :password "password"})

(def init-schema-queries
  (map db/compose-query ["CREATE CONSTRAINT ON (c:Client) ASSERT c.name IS UNIQUE"
                         "CREATE CONSTRAINT ON (b:Bank) ASSERT b.name IS UNIQUE"
                         "CREATE CONSTRAINT ON (m:Merchant) ASSERT m.name IS UNIQUE"
                         "CREATE CONSTRAINT ON (c:CashIn) ASSERT c.id IS UNIQUE"
                         "CREATE CONSTRAINT ON (c:CashOut) ASSERT c.id IS UNIQUE"
                         "CREATE CONSTRAINT ON (d:Debit) ASSERT d.id IS UNIQUE"
                         "CREATE CONSTRAINT ON (p:Payment) ASSERT p.id IS UNIQUE"
                         "CREATE CONSTRAINT ON (t:Transfer) ASSERT t.id IS UNIQUE"]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; PaySim stuff
(def start-date (time/zoned-date-time 2019 10 1))

(defn transaction->map
  [t]
  (if (nil? t) nil
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
                                :balance (.getNewBalanceDest t)))))

(def tx-keys #{:step :action :amount :fraud :flagged-fraud :source :dest})
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
                "CREATE (tx:~XTYPE~ { id: $txId })"
                "SET tx.ts = $ts, tx.amount = $amount, tx.fraud = $fraud, tx.step = $step"
                "CREATE (s)-[:SENT_ON_~DATE~]->(tx)"
                "CREATE (tx)-[:TO]->(r)"]))

(defn build-merge-query
  [tx]
  (let [{:keys [amount source dest step]} tx]
    (-> query-pattern
        (s/replace #"~STYPE~" (s/capitalize (:type source)))
        (s/replace #"~RTYPE~" (s/capitalize (:type dest)))
        (s/replace #"~XTYPE~" (tx->label tx))
        (s/replace #"~DATE~" (ts->daystring (step->ts step))))))

(defn tx->props
  [tx]
  (let [{:keys [amount dest fraud flagged-fraud source step]} tx
        senderName (:name source)
        receiverName (:name dest)
        txId (.toString (java.util.UUID/randomUUID))
        ts (step->ts step)]
    {:amount amount :fraud fraud :flagged-fraud flagged-fraud
     :senderName senderName :receiverName receiverName
     :txId txId :ts ts :step step}))

(defn naive-query
  "Generate a naive Neo4j query to populate the labels and relationships from a Transaction"
  ([tx]
   (let [query (build-merge-query tx)
         props (tx->props tx)]
     (db/compose-query query props))))




;;;;;;;;
(def default-batch-size 1000)
(def default-log-interval 100)
(def default-sim-queue-depth 100000)

(def transducer-pipeline
  (comp (map transaction->map)
        (filter tx?)
        (partition-all default-batch-size)))

(defn sim-and-load
  "Step through an IteratingPaySim, loading data as we go"
  [driver params-file]
  (let [params (new Parameters params-file)
        sim (new IteratingPaySim params default-sim-queue-depth)]
    (.run sim)
    (let [cnt (volatile! 0)
          batch-size default-batch-size
          clock (volatile! (System/currentTimeMillis))
          batches (eduction transducer-pipeline (iterator-seq sim))]
      (doseq [batch batches]
        (apply db/write! driver (map naive-query batch))
        (let [cur (vswap! cnt inc)
              log-interval default-log-interval]
          (if (= 0 (mod cur log-interval))
            (let [start @clock
                  finish (vreset! clock (System/currentTimeMillis))
                  delta-s (/ (- finish start) 1000)]
              (println (format "%d\t%d\t%f\t%f"
                               cur
                               (* cur batch-size)
                               (double delta-s)
                               (double (/ (* log-interval batch-size) delta-s)))))))))
    (try (.abort sim) (catch Exception e))))

;;;;;;;;;;;;;;;;;;
(defn run-sim!
  [opts]
  (with-open [driver (db/connect! opts)]
    ;; make sure we have a valid schema with constraints
    (doseq [q init-schema-queries] (db/execute! driver q))
    ;; do the heavy lifting
    (sim-and-load driver "PaySim.properties")))

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
               :description "Run the simulation and load the data."
               :runs run-sim!}]})

(defn -main
  "Simulate and load into Neo4j"
  [& args]
  (run-cmd args cli-configuration))
