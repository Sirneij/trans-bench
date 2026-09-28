#!/bin/bash
# The full verified campaign (results/verified_2026), one system at a time: only the server of the
# system being measured is running during its phase (a PostgreSQL service may stay up throughout,
# as it did in 2026). Run from anywhere; paths are relative to the repository root.
#
#   SCRATCH=<dir with server data> scripts/run_all.sh <results dir> [phase ...]
#
# Phases (default: all, in this order): duckdb xsb postgres cockroachdb mariadb mariadb_tuned
# mongodb neo4j singlestore. Server installation and configuration: docs/SYSTEMS.md. Credentials:
# systems/<name>/credentials.yaml (see credentials.example.yaml) or --config-file via $CONFIG.
# Machine-specific locations can be overridden through the environment variables below.
set -u
cd "$(dirname "$0")/.."
R=${1:?results dir}; shift
PHASES=${*:-"duckdb xsb postgres cockroachdb mariadb mariadb_tuned mongodb neo4j singlestore"}
S=${SCRATCH:?set SCRATCH to the directory holding the server data (crdb-data, mongo-data, neo4j/)}
PY=${PYTHON:-./virtualenv/bin/python}
CONFIG=${CONFIG:-config.yaml}
[ -n "${XSB_BIN:-}" ] && export PATH=$XSB_BIN:$PATH      # directory containing the xsb executable
export JAVA_HOME=${JAVA_HOME:-/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home} NEO4J_CONF=${NEO4J_CONF:-$S/neo4j/conf}
MARIADB=${MARIADB:-/opt/homebrew/opt/mariadb/bin}; CRDB=${CRDB:-/opt/homebrew/opt/cockroach/bin/cockroach}
NEO4J=${NEO4J:-/opt/homebrew/opt/neo4j/libexec/bin/neo4j}
G12="complete max_acyclic cycle cycle_with_shortcuts path multi_path grid binary_tree reverse_binary_tree x y w"
N12="100 200 300 400 500 600 700 800 900 1000"
NSF="10000 20000 30000 40000 50000 60000 70000 80000 90000"
NBA="10000 20000 30000 40000 50000 60000 70000 80000 90000 100000"
LR="left_recursion right_recursion"
bench() { $PY benchmark.py --runs 5 --timeout 600 --config-file "$CONFIG" "$@"; }
suite() {  # $1 system, $2 modes for the 12 graphs, $3 out-dir suffix, $4 modes for the large graphs
  bench --systems $1 --graphs $G12 --modes $2 --sizes $N12 --out $R/$1$3
  bench --systems $1 --graphs scale_free --modes ${4:-$LR} --sizes $NSF --out $R/$1$3
  bench --systems $1 --graphs barabasi_albert --modes ${4:-$LR} --sizes $NBA --out $R/$1$3
}
stop_all() {
  $MARIADB/mariadb-admin -u "$(whoami)" shutdown 2>/dev/null
  $CRDB node drain --self --shutdown --insecure --host=localhost:26257 >/dev/null 2>&1
  pkill -f "mongod --dbpath $S/mongo-data" 2>/dev/null
  $NEO4J stop >/dev/null 2>&1
  docker stop singlestoredb-dev >/dev/null 2>&1; colima stop >/dev/null 2>&1
  sleep 5
}
wait_port() { for i in $(seq 1 90); do lsof -iTCP:$1 -sTCP:LISTEN >/dev/null 2>&1 && return 0; sleep 2; done; echo "port $1 not up"; return 1; }

stop_all
for ph in $PHASES; do
  echo "=== phase $ph start $(date -u +%FT%TZ)"
  case $ph in
    duckdb)      suite duckdb "left_recursion right_recursion double_recursion doublerecurring_recursion" "" ;;
    xsb)         suite xsb "left_recursion right_recursion double_recursion" "" ;;
    postgres)    suite postgres "left_recursion right_recursion double_recursion" "" ;;
    cockroachdb) mkdir -p $S/crdb-extern   # = externalDirectory in the cockroachdb credentials
                 nohup $CRDB start-single-node --store=$S/crdb-data --external-io-dir=$S/crdb-extern --http-port=26256 \
                   --insecure --listen-addr=localhost:26257 > $S/crdb.out 2>&1 &
                 wait_port 26257; sleep 5
                 suite cockroachdb "left_recursion right_recursion double_recursion" ""
                 stop_all ;;
    mariadb|mariadb_tuned)
                 nohup $MARIADB/mariadbd-safe --datadir=${MARIADB_DATADIR:-/opt/homebrew/var/mysql} > $S/mariadb.out 2>&1 &
                 wait_port 3306; sleep 3
                 if [ $ph = mariadb ]; then
                   suite mariadb "left_recursion right_recursion double_recursion" ""
                 else  # in-memory temporary tables raised from the 16 MB defaults to 4 GB
                   $MARIADB/mariadb -u "$(whoami)" -e "SET GLOBAL tmp_table_size=4294967296; SET GLOBAL max_heap_table_size=4294967296;"
                   bench --systems mariadb --graphs $G12 --modes $LR --sizes $N12 --out $R/mariadb_tuned --label tmp_table_size=4G
                   $MARIADB/mariadb -u "$(whoami)" -e "SET GLOBAL tmp_table_size=16777216; SET GLOBAL max_heap_table_size=16777216;"
                 fi
                 stop_all ;;
    mongodb)     mkdir -p $S/mongo-data
                 nohup mongod --dbpath $S/mongo-data --port 27027 --bind_ip 127.0.0.1 --logpath $S/mongod.log >/dev/null 2>&1 &
                 wait_port 27027; sleep 3
                 suite mongodb "left_recursion" ""   # $graphLookup has a single, fixed formulation
                 stop_all ;;
    neo4j)       $NEO4J start >/dev/null 2>&1; wait_port 7687; sleep 10
                 suite neo4j "left_recursion" "" left_recursion  # the Cypher query has a single formulation
                 stop_all ;;
    singlestore) colima start --cpu 8 --memory 10 --disk 40 --vm-type vz --vz-rosetta >/dev/null 2>&1
                 docker start singlestoredb-dev >/dev/null; wait_port 3307
                 for i in $(seq 1 60); do docker inspect --format '{{.State.Health.Status}}' singlestoredb-dev | grep -q '^healthy' && break; sleep 5; done
                 suite singlestore "left_recursion right_recursion double_recursion" ""
                 stop_all ;;
    *)           echo "unknown phase $ph"; exit 2 ;;
  esac
  echo "=== phase $ph end $(date -u +%FT%TZ)"
done
