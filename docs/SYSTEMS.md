# Systems: setup, implementation and pitfalls

This document describes every system of the verified campaigns: how it was installed and
configured, how the connector runs one trial, what the timed query is, and the behaviour that
affects the results. Everything here was checked on the machine and with the versions of those
campaigns, an Apple M3 Pro with 11 cores and 18 GB under macOS 27.0 (see
`results/verified_2026/versions.txt`). The commands are for macOS with Homebrew; on Linux, the
distribution's packages serve, and the configuration is the same.

## What every system shares

The database systems read `input/souffle/<graph>/<n>/edge.facts`, which holds tab-separated edges,
while XSB reads `edge(x, y).` facts from `input/clingo_xsb/<graph>/graph_<n>.lp`. Both files are
written by `generate_db.py` and checked with `scripts/verify_inputs.py`. Only one family,
`scale_free`, is a multigraph. Its TSV file keeps the parallel edges, whereas its `.lp` file lists
every distinct edge once, sorted; the transitive closure is the same either way.

One trial runs in its own process (`python -m engine.run_one`), started by the campaign engine. It
drops what an earlier trial left behind, creates the edge table, loads it, indexes it on `(y, x)` and
analyzes it. Thereafter, it runs the recursive query that stores the closure, exports the result and
drops the tables. Each step is one timing phase (`timing_phases` in `descriptor.yaml`), and the phase
the paper reports is the one named `query_phase`.

The connector writes the result to `result_file` in the trial's output folder. The engine then
checks it, by count and hash as described in [VERIFICATION.md](VERIFICATION.md), and deletes it.
Exceptions are recorded in `connector.errors`, and `run_one` exits with code 1. After a timeout, the
engine kills the trial's process group and calls the connector's `cancel_running()`, which stops the
statement the server is still executing. Finally, `scripts/run_all.sh` starts only the server of the
system being measured, and stops it afterwards.

| System | Version (2026) | Protocol and connector | Query phase | Result file | Modes measured |
| --- | --- | --- | --- | --- | --- |
| PostgreSQL | 17.11 | `psycopg2`, `PostgreSQLConnector` | ExecuteQuery | postgres_results.csv | left, right, double (rejected) |
| MariaDB | 13.0.2 | `mysqlclient`, `MariaDBConnector` | ExecuteQuery | mariadb_results.csv | left, right, double |
| DuckDB | 1.5.5 | `duckdb`, `DuckDBConnector` | ExecuteQuery | duckdb_results.csv | left, right, double, doublerecurring |
| CockroachDB | 26.3.2 | `cockroachdb`, `CockroachDBConnector` | ExecuteQuery | cockroachdb_results.csv | left, right, double (rejected) |
| SingleStore | 9.0.9 | `singlestore`, `SingleStoreConnector` | ExecuteQuery | singlestore_results.csv | left, right, double (rejected) |
| MongoDB | 8.3.11 | `pymongo`, `MongoDBConnector` | ExecuteQuery | mongodb_results.csv | left (the single pipeline) |
| Neo4j | 2026.09.0 with APOC | `neo4j`, `Neo4jConnector` | Query | neo4j_results.csv | left (the single Cypher query) |
| XSB | 5.0.0 | `subprocess`, `XSBConnector` | Query | xsb_results.txt | left, right, double |

Clingo, Soufflé and ALDA are supported by the suite as well, but they were not part of the verified
campaigns and are not described here.

## PostgreSQL

### Setup

PostgreSQL 17 was installed with `brew install postgresql@17` and run as a service, and the
database was created with `psql -d postgres -c "create database benchmarkdb"`. The credentials
consist of a `dbURL` (see `systems/postgres/credentials.example.yaml`). The configuration was left at
its defaults.

### Trial

The connector loads the edges with `COPY edge FROM STDIN` (client side, tab-delimited), then runs
`CREATE INDEX edge_yx ON edge(y, x)` and `ANALYZE edge`. The timed statement, ExecuteQuery, is
`CREATE TABLE tc_result AS WITH RECURSIVE tc AS (... UNION ...) SELECT * FROM tc`. The result is
written with `COPY (SELECT ...) TO STDOUT WITH CSV HEADER`.

### Behaviour

Double recursion is rejected with `recursive reference to query "tc" must not appear more than
once`; the run is recorded as `error` and shown as `n/s` in the tables. Cancellation calls
`pg_terminate_backend` for every other session on the benchmark database. Hence the database should
serve the benchmark alone.

## MariaDB

### Setup

MariaDB was installed with `brew install mariadb`, the server started with
`mariadbd-safe --datadir=/opt/homebrew/var/mysql &`, and the database created with
`mariadb -u "$(whoami)" -e "create database if not exists benchmark"`. The credentials are shown in
`systems/mariadb/credentials.example.yaml`.

### Trial

The connector sets `SET @@standard_compliant_cte=0` for the session, because MariaDB otherwise
rejects non-linear (double) recursion. The data are loaded with `LOAD DATA LOCAL INFILE`, which needs
`local_infile` on the client (the connector sets it) and on the server (on by default).

The result is written by the server itself, with `SELECT ... INTO OUTFILE '/tmp/mariadb_results.csv'`.
As a result, the server must run on the benchmark machine, and `secure_file_priv` must allow `/tmp`,
which the Homebrew default does. `INTO OUTFILE` refuses to overwrite an existing file, so the
connector removes a leftover file before the trial and moves the new one to the output folder after
it.

### Behaviour

In the default configuration (`tmp_table_size` = `max_heap_table_size` = 16 MB), MariaDB returned
too few pairs on two configurations, identically in all five runs and without a warning, once its
temporary tables spilled to disk:

* scale_free/right/20000 returned 19,108,898 of 19,108,900 pairs;
* barabasi_albert/left/100000 returned 7,768,407 of 7,768,408 pairs.

The missing pairs have short paths, so `max_recursive_iterations` (default 1000) is not the cause,
and they are missing with `standard_compliant_cte=1` as well. With both limits raised to 4 GB
(`SET GLOBAL tmp_table_size=4294967296; SET GLOBAL max_heap_table_size=4294967296;`, the
`mariadb_tuned` series), the results are correct and the queries run 11 to 16 times faster: 28 s in
place of 440 s, and 12.5 s in place of 143 s. `scripts/run_investigation.sh` reproduces this. Its
output is in `results/verified_2026_v2/mariadb_investigation.jsonl`, and the first campaign's in
`results/verified_2026/`.

MariaDB's own accounting of the query's memory (`MEMORY_USED`) never exceeded 49 MB in the default
configuration, since temporary tables beyond 16 MB go to disk; with 4 GB tables, the same queries
used up to 1,408 MB. Cancellation runs `KILL <id>` for every other session on the benchmark
database.

## DuckDB

### Setup

DuckDB needs no server. It is installed with `pip install duckdb==1.5.5`, the version pinned in
`requirements.txt`.

### Trial

The rule file is a SQL script (`systems/duckdb/rules/*.sql`) with the placeholders `{data_file}`
and `{output_file}`. It is split at each `;`, and statement *i* is timing phase *i*: CREATE TABLE,
COPY, CREATE INDEX, ANALYZE, `CREATE TABLE tc_result AS ...` and `COPY ... TO`.

Each trial uses a new database file, `systems/duckdb/rules/duckdb/duckdb_file.db`, which git
ignores. The connector removes this file and its `.wal` before and after the trial, because a killed
trial leaves them behind and the next `CREATE TABLE edge` would fail. For the same reason, two DuckDB
trials must not run at the same time on one checkout; the test suite keeps its DuckDB tests on one
worker.

### Behaviour

DuckDB runs inside the measuring process, so the process CPU time covers all of its threads, and on
dense graphs the CPU time exceeds the elapsed time.

Double recursion (`double_recursion`) is incomplete, without an error, on 7 of the 12 graph families
(binary_tree, cycle, grid, multi_path, path, reverse_binary_tree and y) at every size, in every
version from 1.0.0 to 1.5.5. Each self-reference sees only the rows of the previous iteration, so
iteration *i* derives only paths of length 2^i. The mode `doublerecurring_recursion` reads both
self-references as `recurring.tc`, available since DuckDB 1.5, and returns the correct closure
everywhere. Both modes are kept, because the difference between them is one of the paper's findings.

The intermediate results must fit in memory: scale_free/90000 failed with
`Out of Memory Error: failed to allocate data of size 8.0 GiB`. The mean times on sparse graphs are
erratic because of occasional slow runs, so the medians in `summary.csv` are the better guide there.

## CockroachDB

### Setup

CockroachDB was installed with `brew install cockroachdb/tap/cockroach` and run as a single node:

```sh
cockroach start-single-node --store=<data> --external-io-dir=<extern> --insecure \
    --listen-addr=localhost:26257 --http-port=26256
```

The credentials are `dbURL: postgresql://root@localhost:26257/defaultdb?sslmode=disable` and
`externalDirectory: <extern>/`. The latter must be the directory given to `--external-io-dir`; a
trailing slash, which the 2026 harness needed, is now optional.

### Trial

The input is copied into the external directory and loaded with
`IMPORT INTO edge (x, y) CSV DATA ('nodelocal://1/<file>')`. The result is written with
`EXPORT INTO CSV 'nodelocal://1/tmp'`, which may split it into several chunk files. The connector
concatenates all `<extern>/tmp/*.csv` files in name order, since copying one chunk alone loses rows,
and it removes `<extern>/tmp` before and after every trial, so that chunks of an earlier trial are
never mixed in.

The descriptor must say `protocol: cockroachdb`. With `psycopg2`, CockroachDB would be run by the
PostgreSQL connector, which fails because the loading and export differ.

### Behaviour

Double recursion is rejected with the same message as in PostgreSQL. Cancelling a query, however,
is not enough. `CREATE TABLE ... AS` runs as a schema-change job, which survives the client,
`CANCEL QUERY` and even a restart of the server. While it runs, the table is "being added", so the
next trial fails with `table "tc_result" is being added`. Hence `cancel_running()` proceeds in three
steps. First, it cancels the running queries. Next, it cancels every running `SCHEMA CHANGE` job one
by one, because a bulk `CANCEL JOBS (SELECT ...)` fails as a whole when it also selects the
non-cancelable `SCHEMA CHANGE GC` jobs. Finally, it retries `DROP TABLE IF EXISTS tc_result, edge`
until it succeeds, for at most 10 minutes.

In 2026 this cleanup was missing at first. The affected runs are kept in
`results/verified_2026/cockroachdb/runs_invalid_cleanup_bug.jsonl` with a README, and those
configurations were run again.

## SingleStore

### Setup

SingleStore does not run natively on macOS. In 2026 it ran in the official development container,
inside a colima virtual machine with 8 CPUs, 10 GB of memory and x86-64 emulation (vz with Rosetta):

```sh
colima start --cpu 8 --memory 10 --disk 40 --vm-type vz --vz-rosetta
docker run -d --name singlestoredb-dev --platform linux/amd64 -p 3307:3306 -p 8080:8080 \
  -e ROOT_PASSWORD='<password>' ghcr.io/singlestore-labs/singlestoredb-dev:0.2.62
```

Image 0.2.62 (SingleStore 9.0.9) is the newest one that runs under Rosetta, since newer images need
AVX2 (x86-64-v3), which Rosetta does not provide. `maximum_memory` was lowered from 8930 to 6000 MB
on both nodes. Without this, the virtual machine's out-of-memory killer ends the leaf node on the
first large query, and the database stays unusable ("Failed to find a master partition"); with the
lower limit, SingleStore fails the query with its own error 1712.

```sh
docker exec singlestoredb-dev bash -c 'for id in $(memsqlctl -j list-nodes | python3 -c "import sys,json;[print(n[\"memsqlId\"]) for n in json.load(sys.stdin)[\"nodes\"]]"); do
  memsqlctl -y update-config --memsql-id $id --key maximum_memory --value 6000
  memsqlctl -y query --memsql-id $id --sql "SET GLOBAL maximum_memory = 6000"; done'
```

The database `benchmark` must exist, and the credentials are shown in
`systems/singlestore/credentials.example.yaml`. Because of the emulation and the virtual machine,
SingleStore's times are not comparable with those of the other systems; they show only what it can
compute.

### Implementation

The base and recursive terms must be combined with `UNION ALL` (`systems/singlestore/`,
`SingleStoreConnector`). `UNION` and `DISTINCT` in the recursive term are rejected with errors 2709
and 2730, and so is double recursion ("must not appear more than once"). The closure is therefore
computed as `SELECT DISTINCT x, y FROM tc` over all paths, which terminates only on acyclic graphs.

The iteration limit `max_recursive_cte_iterations`, 32 by default, is raised to 10,000 in every
session; otherwise no path longer than 32 edges would be followed. The result is fetched through the
client and written locally with an `x,y` header, because `INTO OUTFILE` would write inside the
container. Cancellation uses `KILL QUERY <id>`.

### Behaviour

Cyclic graphs fail with error 2741 (iteration limit) or 1712 (out of memory). max_acyclic and the
larger grids run out of memory: they are acyclic, but they have exponentially many paths. SingleStore
is correct on path, multi_path, both binary trees, x, y and w up to n = 1000, on grid up to 200, and
on all Barabási-Albert graphs.

## MongoDB

### Setup

MongoDB was installed with `brew tap mongodb/brew && brew install mongodb-community`. The campaign
used a separate server, so that a server already running on port 27017 was not disturbed:

```sh
mongod --dbpath <data> --port 27027 --bind_ip 127.0.0.1 --logpath <log> &   # --fork is not supported on macOS
```

The credentials are a `uri` and a `database`.

### Trial

The edges are inserted as documents `{x, y}` into `edge`, and an index is created on `(x, y)`. The
closure is computed by one aggregation: `$graphLookup` (start at `$x`, connect `y` to `x`), followed
by `$unwind`, `$group` on (x, y) and `$out` into `tc_result`. This whole pipeline is the ExecuteQuery
phase. The result is then exported to CSV.

### Behaviour

`$graphLookup` has one fixed search, so the left, right and double rule files contain the same
pipeline, and only `left_recursion` was measured on the twelve families; for the large graphs,
`scripts/run_all.sh` ran the identical right recursion as well. The pipeline starts one search per
edge document, so the times grow steeply: complete and max_acyclic exceeded 600 s at n = 200.
Cancellation uses `killOp` on the operations of the benchmark database.

## Neo4j

### Setup

The Community Edition 2026.09.0 was used with the APOC core plugin, kept apart from the Homebrew
defaults:

```sh
brew install neo4j            # with openjdk@21
N=<dir>; H=/opt/homebrew/opt/neo4j/libexec
mkdir -p $N/{conf,data,plugins,import,logs,run}
cp $H/conf/* $N/conf/ && cp $H/labs/apoc-2026.09.0-core.jar $N/plugins/
# in $N/conf/neo4j.conf, comment out the Homebrew lines for server.directories.import/data/logs and
# server.https.enabled, and append:
#   server.directories.data=$N/data   server.directories.plugins=$N/plugins
#   server.directories.import=$N/import   server.directories.logs=$N/logs   server.directories.run=$N/run
#   server.bolt.listen_address=localhost:7687   server.http.listen_address=localhost:7474
#   server.https.enabled=false   dbms.security.procedures.unrestricted=apoc.*
printf "apoc.export.file.enabled=true\napoc.import.file.use_neo4j_config=true\n" > $N/conf/apoc.conf
export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home NEO4J_CONF=$N/conf
$H/bin/neo4j-admin dbms set-initial-password '<password>'
$H/bin/neo4j start
```

The credentials are `uri`, `user`, `password` and `import_directory`, the last being `$N/import`. The
default memory settings were kept.

### Trial

The rule file `systems/neo4j/rules/transitive_*.cypher` holds five statements, in this order:
`MATCH (n) DETACH DELETE n`; `LOAD CSV`, for which the input is copied into the import directory;
`CREATE INDEX`; the timed query; and `apoc.export.csv.query`, which writes the result into the
import directory, from where the connector copies it. The timed query is:

```cypher
MATCH (start:Node)-[:EDGE*1..]->(end:Node)
WITH DISTINCT start.id AS x, end.id AS y
RETURN count(*) AS pairs;
```

It computes the distinct pairs on the server and returns only their number. The full result is
written by the export, which is timed as WriteResult.

### The lazy `session.run()`

`session.run()` returns before the statement has been executed, and an unconsumed result is executed
as part of the next statement. The connector therefore finishes every statement inside its own timed
call. `consume()` is enough for the setup statements, which are updates. The query and the export,
however, are fetched with `list(...)`, because `consume()` on a read query discards the stream and
lets Neo4j skip computing it. The first attempt in 2026 timed `consume()` and measured 0.04 s where
the real query takes 27 s. Those runs are kept, marked invalid, in
`results/verified_2026/invalid/neo4j_consume_timing/`.

### Behaviour

The plan is `VarLengthExpand(Pruning,BFS,All)` followed by `Distinct`, a pruned breadth-first search;
Neo4j does not enumerate paths. The Cypher query has one formulation, so the three rule files are
identical and only `left_recursion` was run. scale_free/30000 failed with
`MemoryPoolOutOfMemoryError`, the limit being `dbms.memory.transaction.total.max` (3.1 GiB by
default). Cancellation uses `TERMINATE TRANSACTIONS` on every running transaction except its own.

## XSB

### Setup

XSB 5.0.0 was built from source, and `XSB/bin` put on `PATH`; `scripts/run_all.sh` also accepts it
through `XSB_BIN`.

```sh
curl -L -o XSB-5.0.tar.gz "https://downloads.sourceforge.net/project/xsb/xsb/5.0%20%28Green%20Tea%29/XSB-5.0.tar.gz"
tar xzf XSB-5.0.tar.gz          # one harmless error for the Windows helper build/MSVC.sh
cd XSB/build && ./configure && ./makexsb
```

### Trial

The rules (`systems/xsb/rules/*.P`) use `:- auto_table.`, that is, variant tabling.
`xsb_export/extfilequery.P` loads the rules and facts, runs the query and prints its timings, which
XSB measures itself with `statistics/2`. Each trial runs XSB twice, once with the query alone and
once with the query and the writing of the result; WriteTime is the difference between the two. The
600 s limit covers both runs, so it is reached at about half the query time. Compiled `.xwam` files
are removed after each trial. A run that exits with an error, or does not print its timings (when
memory is exhausted, say), is recorded as an error.

### Behaviour

XSB was the fastest system on sparse graphs with long paths. With right recursion it completed every
scale-free size, while left recursion exceeded the limit at 70,000 nodes.
