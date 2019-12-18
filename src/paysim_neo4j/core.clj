(ns paysim-neo4j.core
  (:require [clojure.pprint :as pp]
            [clojure.string :as s]
            [java-time :as time])
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

(def strategy (Config$TrustStrategy/trustAllCertificates))

(def cfg (-> (Config/builder)
              ;(.withEncryption)
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
                      "CREATE CONSTRAINT ON (m:Merchant) ASSERT m.name IS UNIQUE"
                      "CREATE CONSTRAINT ON (c:CashIn) ASSERT c.id IS UNIQUE"
                      "CREATE CONSTRAINT ON (c:CashOut) ASSERT c.id IS UNIQUE"
                      "CREATE CONSTRAINT ON (d:Debit) ASSERT d.id IS UNIQUE"
                      "CREATE CONSTRAINT ON (p:Payment) ASSERT p.id IS UNIQUE"
                      "CREATE CONSTRAINT ON (t:Transfer) ASSERT t.id IS UNIQUE"]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; PaySim stuff
(def batch-size 20)
(def start-date (time/zoned-date-time 2019 1 1))

(defn transaction->map
  [t]
  (if (nil? t) {}
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
  (s/join "\n" ["MERGE (s~IDX~:~STYPE~ { name: $data[~IDX~].senderName })"
                "MERGE (r~IDX~:~RTYPE~ { name: $data[~IDX~].receiverName })"
                "CREATE (tx~IDX~:~XTYPE~ { id: $data[~IDX~].txId })"
                "  SET tx~IDX~.ts = $data[~IDX~].ts, tx~IDX~.amount = $data[~IDX~].amount, tx~IDX~.fraud = $data[~IDX~].fraud, tx~IDX~.step = $data[~IDX~].step"
                "CREATE (s~IDX~)-[:SENT_ON_~DATE~]->(tx~IDX~)-[:TO]->(r~IDX~)"]))

(defn build-merge-query
  ([tx] (build-merge-query tx 0))
  ([tx idx]
   (let [{:keys [amount source dest step]} tx]
     (-> query-pattern
         (s/replace #"~IDX~" (str idx))
         (s/replace #"~STYPE~" (s/capitalize (:type source)))
         (s/replace #"~RTYPE~" (s/capitalize (:type dest)))
         (s/replace #"~XTYPE~" (tx->label tx))
         (s/replace #"~DATE~" (ts->daystring (step->ts step)))))))

(defn tx->props
  [tx]
  (let [{:keys [amount dest fraud source step]} tx
        senderName (:name source)
        receiverName (:name dest)
        txId (.toString (java.util.UUID/randomUUID))
        ts (step->ts step)]
    {:amount amount :fraud fraud :senderName senderName :receiverName receiverName :txId txId :ts ts}))

(defn naive-query
  "Generate a naive Neo4j query to populate the labels and relationships from a Transaction"
  ([tx & txs]
   (let [all (cons tx txs)
         idxs (range (count all))
         query (s/join "\n\n" (map (fn [[k v]] (build-merge-query v k)) (zipmap idxs all)))
         data (map tx->props all)]
     (compose-query query {:data data}))))

(def transducer-pipeline
  (comp (map transaction->map)
        (partition-all batch-size)))

(defn sim-and-load
  "Step through an IteratingPaySim, loading data as we go"
  [driver params-file]
  (let [params (new Parameters params-file)
        sim (new IteratingPaySim params)]
    (.run sim)
    (let [cnt (volatile! 0)
          batches (eduction transducer-pipeline (iterator-seq sim))]
      (doseq [batch batches]
        (execute! driver (apply naive-query batch))
        (let [cur (vswap! cnt inc)]
          (if (= 0 (mod cur 1))
            (println (format "...batch %d (%d txns)" cur (* cur batch-size)))))))
    (try (.abort sim))))

(defn -main
  "Simulate and load into Neo4j"
  [& args]
  (with-open [driver (connect! db-config)]
    (map #(execute! driver %) init-schema-queries)
    (sim-and-load driver "PaySim.properties")))
