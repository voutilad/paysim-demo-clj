(ns paysim-neo4j.db
  (:import (org.neo4j.driver AuthTokens
                             Config
                             Config$TrustStrategy
                             Driver
                             GraphDatabase
                             Query
                             Session
                             TransactionWork)))

(def trust-all (Config$TrustStrategy/trustAllCertificates))

(defn -build-config
  [tls trust-all-certs]
  (let [builder (volatile! (Config/builder))]
    (if tls (vswap! builder .withEncryption))
    (if trust-all-certs (vswap! builder #(.withTrustStrategy % trust-all)))
    (.build @builder)))

(defn connect!
  "Establish a Bolt connection to a Neo4j instance, returning a Driver."
  [{:keys [uri username password tls trust-all-certs]
    :or {uri "bolt://localhost:7687"
         username "neo4j"
         password "password"
         tls false
         trust-all-certs false}}]
  (GraphDatabase/driver uri
                        (AuthTokens/basic username password)
                        (-build-config tls trust-all-certs)))

(defn compose-query
  "Compose a Cypher Query that's passable to an open Session for execution."
  ([query]
   {:pre [(string? query)]}
   (Query. query))
  ([query m]
   {:pre [(string? query)
          (map? m)]}
   (Query. query (clojure.walk/stringify-keys m))))
9
(defn single-query-txn
  "Run a single `query` in a unit of TransactionWork"
  [query]
  (reify TransactionWork
    (execute [this txn] (.run txn query))))

(defn multi-query-txn
  "Run multiple queries in a single unit of TransactionWork"
  [query & queries]
  (reify TransactionWork
    (execute [this txn]
      (doseq [q (cons query queries)]
        (.run txn q)))))

(defn execute!
  "Execute a simple read and/or write query outside of a transaction block."
  [driver query]
  {:pre ([instance? Driver driver])}
  (with-open [session (.session driver)]
    (.run session query)))

(defn write!
  "Execute one or many writing transaction `stmt`s in a session using the provided `driver` instance."
  ([driver query]
   {:pre [(instance? Driver driver)]}
   (with-open [session (.session driver)]
     (let [txn (single-query-txn query)]
       (.writeTransaction session txn)
       (.lastBookmark session))))
  ([driver query & more]
   {:pre [(instance? Driver driver)]}
   (with-open [session (.session driver)]
     (let [queries (cons query more)]
       (.writeTransaction session (apply multi-query-txn queries)))
     (.lastBookmark session))))
