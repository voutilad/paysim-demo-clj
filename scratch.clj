(require '[paysim-neo4j.core :as ps])

;; Test connection
(with-open [conn (ps/connect! ps/db-config)]
  (let [q (ps/compose-query "MATCH (n) RETURN COUNT(n);")]
    (ps/execute! conn q)))

;; Manually test -main
(ps/match-or-create-agent "C123a" "CLIENT" "c")
