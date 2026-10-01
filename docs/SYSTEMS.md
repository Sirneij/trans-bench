# Systems: setup, implementation and pitfalls

This document describes, for every system of the verified campaign (`results/verified_2026`),
how it is installed and configured, how the connector runs one trial, what the timed query is,
and the behaviour that affects the results. Everything here was checked on the machine and
versions of that campaign (Apple M3 Pro, 11 cores, 18 GB, macOS 27.0; `results/verified_2026/versions.txt`).
The commands are for macOS with Homebrew. On Linux, use the distribution's packages; the
configuration is the same.

Common to all systems:

* **Input.** `input/souffle/<graph>/<n>/edge.facts` holds tab-separated edges for the database
  systems, and `input/clingo_xsb/<graph>/graph_<n>.lp` holds `edge(x, y).` facts for XSB. Both are
  produced by `generate_db.py` and checked with `scripts/verify_inputs.py`. For multigraphs (only
  `scale_free`), the TSV file keeps the parallel edges and the `.lp` file lists every distinct edge
  once, sorted. This does not change the transitive closure.
* **Trial.** One trial runs in its own process (`python -m engine.run_one`, started by
  `benchmark.py`). It drops leftovers, creates the edge table, loads, indexes (`edge(y, x)`) and
  analyzes it, runs the recursive query that stores the closure, exports the result, and drops the
  tables. Every step is one timing phase (`timing_phases` in `descriptor.yaml`). The phase
  reported in the paper is `query_phase`.
* **Result check.** The connector writes the result to `result_file` in the trial's output folder.
  `benchmark.py` checks it (count and hash, see [VERIFICATION.md](VERIFICATION.md)) and deletes it.
* **Failures.** The connector records exceptions in `connector.errors`, and `run_one` exits with
  code 1. After a timeout, `benchmark.py` kills the trial's process group and calls the
  connector's `cancel_running()`, which stops the statement that the server is still executing.
* **One server at a time.** `scripts/run_all.sh` starts only the server of the system being
  measured, and stops it afterwards.

| System | Version (2026) | Protocol / connector | Query phase | Result file | Modes measured |
| --- | --- | --- | --- | --- | --- |
| PostgreSQL | 17.11 | `psycopg2` / `PostgreSQLConnector` | ExecuteQuery | postgres_results.csv | left, right, double (rejected) |
| MariaDB | 13.0.2 | `mysqlclient` / `MariaDBConnector` | ExecuteQuery | mariadb_results.csv | left, right, double |
| DuckDB | 1.5.5 | `duckdb` / `DuckDBConnector` | ExecuteQuery | duckdb_results.csv | left, right, double, doublerecurring |
| CockroachDB | 26.3.2 | `cockroachdb` / `CockroachDBConnector` | ExecuteQuery | cockroachdb_results.csv | left, right, double (rejected) |
| SingleStore | 9.0.9 | `singlestore` / `SingleStoreConnector` | ExecuteQuery | singlestore_results.csv | left, right, double (rejected) |
| MongoDB | 8.3.11 | `pymongo` / `MongoDBConnector` | ExecuteQuery | mongodb_results.csv | left (the single pipeline) |
| Neo4j | 2026.09.0 + APOC | `neo4j` / `Neo4jConnector` | Query | neo4j_results.csv | left (the single Cypher query) |
| XSB | 5.0.0 | `subprocess` / `XSBConnector` | Query | xsb_results.txt | left, right, double |

Clingo, Soufflé and Alda are also supported by the suite, but they were not part of the verified
campaign and are not described here.

---

## PostgreSQL

**Setup.** `brew install postgresql@17` (run as a service), then
`psql -d postgres -c "create database benchmarkdb"`. Credentials: `dbURL` (see
`systems/postgres/credentials.example.yaml`). Default configuration.

**Trial.** The connector runs `COPY edge FROM STDIN` (client side, tab-delimited), then
`CREATE INDEX edge_yx ON edge(y, x)`, `ANALYZE edge`, and
`CREATE TABLE tc_result AS WITH RECURSIVE tc AS (… UNION …) SELECT * FROM tc` (timed as
ExecuteQuery). The result is written with `COPY (SELECT …) TO STDOUT WITH CSV HEADER`.

**Behaviour.**
* Double recursion is rejected with `recursive reference to query "tc" must not appear more than once`.
  This is recorded as `error` and shown as `n/s` in the tables.
* Cancellation calls `pg_terminate_backend` for every other session on the benchmark database. Use
  a database dedicated to the benchmark.

## MariaDB

**Setup.** Run `brew install mariadb`, then start the server with
`mariadbd-safe --datadir=/opt/homebrew/var/mysql &` and create the database with
`mariadb -u "$(whoami)" -e "create database if not exists benchmark"`. Credentials: see
`systems/mariadb/credentials.example.yaml`.

**Trial.**
* The connector sets `SET @@standard_compliant_cte=0` for the session. Otherwise MariaDB rejects
  non-linear (double) recursion.
* The data is loaded with `LOAD DATA LOCAL INFILE`, which needs `local_infile` on the client (the
  connector sets it) and on the server (on by default).
* The result is written by the *server* with `SELECT … INTO OUTFILE '/tmp/mariadb_results.csv'`,
  so the server must run on the benchmark machine and `secure_file_priv` must allow `/tmp`
  (the Homebrew default allows it). `INTO OUTFILE` refuses to overwrite an existing file, so the
  connector removes a leftover file before the trial. The file is then moved to the output folder.

**Behaviour.**
* **Silently incomplete results when temporary tables spill to disk.** In the default
  configuration (`tmp_table_size` = `max_heap_table_size` = 16 MB), two configurations returned too
  few pairs, identically in all 5 runs and without a warning:
  * scale_free/right/20000 returned 19,108,898 of 19,108,900 pairs;
  * barabasi_albert/left/100000 returned 7,768,407 of 7,768,408 pairs.

  The missing pairs have short paths, so `max_recursive_iterations` (default 1000) is not the
  cause, and they are missing with `standard_compliant_cte=1` as well. With both limits raised to
  4 GB (`SET GLOBAL tmp_table_size=4294967296; SET GLOBAL max_heap_table_size=4294967296;`, series
  `mariadb_tuned`), the results are correct and the queries run 11 to 16 times faster (28 instead
  of 440 s, and 12.5 instead of 143 s). Reproduce this with `scripts/run_investigation.sh`; the
  output is in `results/verified_2026_v2/mariadb_investigation.jsonl` (and, from the first
  campaign, in `results/verified_2026/`).
* MariaDB's own accounting of the query's memory (`MEMORY_USED`) never exceeded 49 MB in the
  default configuration, because temporary tables beyond 16 MB go to disk; with 4 GB tables the
  same queries used up to 1,408 MB.
* Cancellation runs `KILL <id>` for every other session on the benchmark database.

## DuckDB

**Setup.** No server is needed: install it with `pip install duckdb==1.5.5`, which
`requirements.txt` pins.

**Trial.**
* The rule file is a SQL script (`systems/duckdb/rules/*.sql`) with the placeholders `{data_file}`
  and `{output_file}`. It is split at `;`, and statement *i* is timing phase *i*: CREATE TABLE,
  COPY, CREATE INDEX, ANALYZE, CREATE TABLE tc_result AS …, and COPY … TO.
* Each trial uses a new database file, `systems/duckdb/rules/duckdb/duckdb_file.db` (gitignored).
  The connector removes this file and its `.wal` before and after the trial, because a killed trial
  leaves them behind and the next `CREATE TABLE edge` would fail.

**Behaviour.**
* DuckDB runs in the measuring process, so the process CPU time covers all of its threads. The CPU
  time exceeds the elapsed time on dense graphs (several threads).
* **Double recursion (`double_recursion`) is silently incomplete** on 7 of the 12 graph families
  (binary_tree, cycle, grid, multi_path, path, reverse_binary_tree, y) at every size, in 1.0.0
  through 1.5.5. Each self-reference sees only the rows of the previous iteration, so iteration *i*
  derives only paths of length 2^i. The mode `doublerecurring_recursion` reads both self-references
  as `recurring.tc` (available since DuckDB 1.5), and it returns the correct closure everywhere.
  Keep both modes: the difference between them is one of the paper's findings.
* The intermediate results must fit in memory. scale_free/90000 failed with
  `Out of Memory Error: failed to allocate data of size 8.0 GiB`.
* The mean times on sparse graphs are erratic because of occasional slow runs. Use the medians in
  `summary.csv`.

## CockroachDB

**Setup.**
* Install with `brew install cockroachdb/tap/cockroach`.
* Start a single node:
  `cockroach start-single-node --store=<data> --external-io-dir=<extern> --insecure --listen-addr=localhost:26257 --http-port=26256`.
* Credentials:
  * `dbURL: postgresql://root@localhost:26257/defaultdb?sslmode=disable`;
  * `externalDirectory: <extern>/`, which must be the same directory as `--external-io-dir` and
    must end with a slash.

**Trial.**
* The input is copied into the external directory and loaded with
  `IMPORT INTO edge (x, y) CSV DATA ('nodelocal://1/<file>')`.
* The result is written with `EXPORT INTO CSV 'nodelocal://1/tmp'`, which **may split it into
  several chunk files**. The connector concatenates all `<extern>/tmp/*.csv` files in name order
  (copying only one chunk loses rows). It removes `<extern>/tmp` before and after every trial, so
  that chunks of an earlier trial are never mixed in.
* The descriptor must say `protocol: cockroachdb`. With `psycopg2`, CockroachDB would be run by
  the PostgreSQL connector, which fails (the classes and loading differ).

**Behaviour.**
* Double recursion is rejected, with the same message as PostgreSQL.
* **Cancelling a query is not enough.** `CREATE TABLE … AS` runs as a *schema-change job*, which
  survives the client, `CANCEL QUERY` and even a server restart. While it runs, the table is
  "being added", so the next trial fails with `table "tc_result" is being added`.
  `cancel_running()` does the following:
  1. cancels the running queries;
  2. cancels every running `SCHEMA CHANGE` job **one by one** (a bulk `CANCEL JOBS (SELECT …)`
     fails as a whole if it also selects the non-cancelable `SCHEMA CHANGE GC` jobs);
  3. retries until `DROP TABLE IF EXISTS tc_result, edge` succeeds (at most 10 minutes).

  In 2026 this cleanup was missing at first. Affected runs are kept in
  `results/verified_2026/cockroachdb/runs_invalid_cleanup_bug.jsonl` with a README, and the
  configurations were re-run.

## SingleStore

**Setup.** SingleStore does not run natively on macOS.
* In 2026 it ran in the official development container, inside a colima VM with 8 CPUs, 10 GB of
  memory, and x86-64 emulation (vz + Rosetta):
  ```sh
  colima start --cpu 8 --memory 10 --disk 40 --vm-type vz --vz-rosetta
  docker run -d --name singlestoredb-dev --platform linux/amd64 -p 3307:3306 -p 8080:8080 \
    -e ROOT_PASSWORD='<password>' ghcr.io/singlestore-labs/singlestoredb-dev:0.2.62
  ```
* Image 0.2.62 (SingleStore 9.0.9) is the newest one that runs under Rosetta. Newer images need
  AVX2 (x86-64-v3), which Rosetta does not provide.
* **Lower `maximum_memory`** from 8930 to 6000 MB on both nodes. Otherwise the VM's out-of-memory
  killer terminates the leaf on the first large query, and the database stays unusable ("Failed
  to find a master partition"). With the lower limit, SingleStore fails the query with its own
  error 1712 instead:
  ```sh
  docker exec singlestoredb-dev bash -c 'for id in $(memsqlctl -j list-nodes | python3 -c "import sys,json;[print(n[\"memsqlId\"]) for n in json.load(sys.stdin)[\"nodes\"]]"); do
    memsqlctl -y update-config --memsql-id $id --key maximum_memory --value 6000
    memsqlctl -y query --memsql-id $id --sql "SET GLOBAL maximum_memory = 6000"; done'
  ```
* Create the database `benchmark`. Credentials: see `systems/singlestore/credentials.example.yaml`.
* The times are **not comparable** with those of the other systems (emulation, VM). They show only
  what SingleStore can compute.

**Implementation** (`systems/singlestore/`, `SingleStoreConnector`):
* The base and recursive terms must be combined with **`UNION ALL`**. `UNION` and `DISTINCT` in the
  recursive term are rejected (errors 2709 and 2730), and so is double recursion ("must not appear
  more than once"). The closure is therefore computed as `SELECT DISTINCT x, y FROM tc` over all
  paths, which terminates only on acyclic graphs.
* The iteration limit `max_recursive_cte_iterations` (default 32) is raised to 10,000 per session.
  Otherwise no path longer than 32 edges is followed.
* The result is fetched through the client and written locally with an `x,y` header. `INTO
  OUTFILE` would write inside the container.
* Cancellation uses `KILL QUERY <id>`.

**Behaviour.**
* Cyclic graphs fail with error 2741 (iteration limit) or 1712 (out of memory).
* max_acyclic and larger grids run out of memory: they are acyclic but have exponentially many
  paths.
* SingleStore is correct on path, multi_path, both binary trees, x, y and w up to n = 1000, on grid
  up to 200, and on all Barabási-Albert graphs.

## MongoDB

**Setup.** `brew tap mongodb/brew && brew install mongodb-community`.
* The campaign used a separate server, so that a server already running on 27017 was not
  disturbed:
  `mongod --dbpath <data> --port 27027 --bind_ip 127.0.0.1 --logpath <log> &`
  (`--fork` is not supported on macOS).
* Credentials: `uri`, `database`.

**Trial.**
* The edges are inserted as documents `{x, y}` into `edge`, and an index is created on `(x, y)`.
* The closure is computed by one aggregation, `$graphLookup` (start `$x`, connect `y` → `x`),
  followed by `$unwind`, `$group` on (x, y), and `$out` into `tc_result`. This whole pipeline is
  the ExecuteQuery phase.
* The result is exported to CSV.

**Behaviour.**
* `$graphLookup` has one fixed search, so the left, right and double rule files contain the same
  pipeline, and only `left_recursion` was measured on the twelve families. For the large graphs,
  `scripts/run_all.sh` also ran right (identical).
* The pipeline starts one search per *edge document*, so the times grow steeply: complete and
  max_acyclic exceeded 600 s at n = 200.
* Cancellation uses `killOp` on the operations of the benchmark database.

## Neo4j

**Setup** (Community Edition 2026.09.0 with the APOC core plugin, isolated from the Homebrew
defaults):
```sh
brew install neo4j            # brings openjdk@21
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
Credentials: `uri`, `user`, `password`, and `import_directory` (= `$N/import`). The default
memory settings were kept.

**Trial.**
* The rule file `systems/neo4j/rules/transitive_*.cypher` contains these statements:
  1. `MATCH (n) DETACH DELETE n`;
  2. `LOAD CSV` (the input is copied into the import directory);
  3. `CREATE INDEX`;
  4. the timed query;
  5. `apoc.export.csv.query`, which writes the result into the import directory, from where it is
     moved.
* The timed query is:
  ```cypher
  MATCH (start:Node)-[:EDGE*1..]->(end:Node)
  WITH DISTINCT start.id AS x, end.id AS y
  RETURN count(*) AS pairs;
  ```
  It computes the distinct pairs on the server and returns only their number. The full result is
  written by the export, which is timed as WriteResult.

**Pitfall: `session.run()` is lazy.** It returns before the statement has been executed, and an
unconsumed result is executed as part of the *next* statement. The connector therefore finishes
every statement inside its own timed call. The setup statements use `consume()`, which is enough
for updates. The query and the export are fetched with `list(...)`, because `consume()` on a
read query discards the stream and lets Neo4j skip computing it. The first attempt in 2026 timed
`consume()`, which gave 0.04 s instead of 27 s. Those runs are kept, but invalid, in
`results/verified_2026/invalid/neo4j_consume_timing/`.

**Behaviour.**
* The plan is `VarLengthExpand(Pruning,BFS,All)` + `Distinct`, a pruned breadth-first search.
  Neo4j does not enumerate paths.
* The Cypher query has one formulation, so the three rule files are identical and only
  `left_recursion` was run.
* scale_free/30000 failed with `MemoryPoolOutOfMemoryError` (`dbms.memory.transaction.total.max`,
  3.1 GiB by default).
* Cancellation uses `TERMINATE TRANSACTIONS` on all running transactions except its own.

## XSB

**Setup.** Build XSB 5.0.0 from source and put `XSB/bin` on `PATH` (or set `XSB_BIN` for
`scripts/run_all.sh`):
```sh
curl -L -o XSB-5.0.tar.gz "https://downloads.sourceforge.net/project/xsb/xsb/5.0%20%28Green%20Tea%29/XSB-5.0.tar.gz"
tar xzf XSB-5.0.tar.gz          # one harmless error for the Windows helper build/MSVC.sh
cd XSB/build && ./configure && ./makexsb
```

**Trial.**
* The rules (`systems/xsb/rules/*.P`) use `:- auto_table.` (variant tabling).
* `xsb_export/extfilequery.P` loads the rules and facts, runs the query, and prints its timings,
  which XSB measures itself with `statistics/2`.
* Each trial runs XSB **twice**, once with the query only and once with the query plus writing the
  result. WriteTime is the difference between the two. The 600 s limit therefore covers both runs,
  so it is reached at about half the query time.
* Compiled `.xwam` files are removed after each trial.
* A run that exits with an error, or does not print its timings, is recorded as an error (for
  example, when memory is exhausted).

**Behaviour.**
* XSB was the fastest on sparse graphs with long paths.
* With right recursion it completed all scale-free sizes. Left recursion exceeded the limit at
  70,000 nodes.
