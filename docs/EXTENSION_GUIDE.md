# Extension guide

This guide explains how the suite is extended: with a new system, a new connector protocol, a new
graph family or a new query domain. It ends with what a system needs before its measurements can be
trusted, and with the errors met most often. The examples are taken from the systems that ship with
the repository, so each of them can be compared with working code.

## How the suite is organized

Every system, graph family and domain is described by a descriptor file, and the engine discovers
them when it starts. A system lives in `systems/<name>/`:

```
systems/postgres/
├── descriptor.yaml            what the engine needs to know: protocol, phases, modes, result file
├── credentials.example.yaml   the connection settings of the published campaign, without secrets
├── credentials.yaml           your connection settings (ignored by git)
├── __init__.py                for the server databases: the steps of a run, shared by all modes
└── rules/                     one rule file per mode: transitive_left_recursion.py, ...
```

Graph families are described in `graph_types/<name>.yaml`, and domains in
`domains/<name>/descriptor.yaml`. Python code is needed in three places only: a new connector
protocol, the operation classes of a server database, and the generator of a new graph family.

## Adding a system

### 1. Create the directory

The fastest start for a server that speaks an existing protocol is a copy of a system that already
uses it. For another PostgreSQL-compatible server, say:

```sh
cp -r systems/postgres systems/my_db
```

Alternatively, a bare directory with a descriptor taken from `templates/` is created with:

```sh
python transitive.py --bootstrap-system my_db --bootstrap-system-template descriptor_sql_database.yaml
```

This writes `systems/my_db/descriptor.yaml` and an empty `rules/` directory; the credentials and the
rule files are then added by hand. The page Systems, Register a system of the web interface does the
same. `python transitive.py --list-templates` lists the available templates.

### 2. Edit the descriptor

```yaml
name: my_db                     # must match the directory name
display_name: My Database       # shown in the web interface and the figures
category: db                    # db | logic | hybrid
protocol: psycopg2              # the connector (see the protocol table below)

timing_phases:                  # one per step of a trial, in order; each gives three CSV columns
  - { id: create_table, label: CreateTable }
  - { id: load_data, label: LoadData }
  - { id: create_index, label: CreateIndex }
  - { id: analyze, label: Analyze }
  - { id: execute_query, label: ExecuteQuery }
  - { id: write_result, label: WriteResult }

query_phase: execute_query      # the phase that is the recursive query itself
result_file: my_db_results.csv  # the file the connector writes the result to
input_format: tsv               # tsv | lp | facts | pickle
modes: [right_recursion, left_recursion, double_recursion]
rule_extension: .py

flags:
  requires_credentials: true
  class_prefix: MyDB            # class MyDBLeftRecursion in rules/transitive_left_recursion.py
  module_prefix: my_db_rules    # the name the rule modules import __init__.py under
```

`input_format` decides which generated file the system reads. `tsv` is the tab-separated edge
file of Souffle, read by the databases; `lp` is the Prolog fact file shared by XSB and Clingo;
`facts` is Souffle's input directory, and `pickle` the edge set of ALDA.

### 3. Give it credentials

`systems/my_db/credentials.yaml` is ignored by git. Its keys depend on the protocol:

```yaml
# psycopg2 (PostgreSQL) and cockroachdb
dbURL: postgresql://user:password@localhost:5432/benchmarkdb
externalDirectory: /path/to/crdb-extern/    # cockroachdb only: the server's --external-io-dir

# mysqlclient (MariaDB) and singlestore
host: localhost
port: 3306
user: root
password: secret
database: benchmark

# neo4j
uri: neo4j://localhost:7687
user: neo4j
password: secret
import_directory: /path/to/neo4j/import

# pymongo
uri: mongodb://127.0.0.1:27017/
database: benchmark
```

The Credentials tab of a system's page in the web interface writes the same file.

### 4. Write the rule files

A rule file is named `rules/<domain>_<mode><rule_extension>`, for example
`rules/transitive_left_recursion.py`. What it contains depends on the protocol.

DuckDB (`protocol: duckdb`) runs plain SQL scripts. The script is split at each `;`, and statement
*i* is timed as phase *i*, so the number of statements must equal the number of timing phases.
`{data_file}` and `{output_file}` are replaced by the input and result paths:

```sql
CREATE TABLE edge (x INTEGER, y INTEGER);
COPY edge FROM '{data_file}' (DELIMITER '\t');
CREATE INDEX edge_yx ON edge (y, x);
ANALYZE;
CREATE TABLE tc_result AS
WITH RECURSIVE tc AS (
    SELECT x, y FROM edge
    UNION
    SELECT tc.x, edge.y FROM tc JOIN edge ON tc.y = edge.x
)
SELECT * FROM tc;
COPY (SELECT * FROM tc_result) TO '{output_file}' WITH (HEADER, DELIMITER ',');
```

The server databases (`psycopg2`, `mysqlclient`, `cockroachdb`, `singlestore`, `pymongo`) load
Python classes. The shared `systems/<name>/__init__.py` holds an operations class with the steps of
a run (create the table, import the data, index, analyze, export, drop), which the connector times one
by one. Each rule file adds only the recursive query, in a class named
`<class_prefix><Mode>Recursion`:

```python
# systems/postgres/rules/transitive_left_recursion.py
from postgres_rules import PostgresOperations   # postgres_rules = flags.module_prefix


class PostgreSQLLeftRecursion(PostgresOperations):
    def run_recursive_query(self) -> None:
        """Run the left recursion query for transitive closure."""
        self.execute_query(
            """
        CREATE TABLE tc_result AS
        WITH RECURSIVE tc AS (
            SELECT x, y FROM edge
            UNION
            SELECT tc.x, edge.y FROM tc JOIN edge ON tc.y = edge.x
        )
        SELECT * FROM tc;
        """
        )
```

The connector registers `__init__.py` under `module_prefix` before it imports the rule file, which
is why the import above works although no such package exists on disk. The method names that the
connector calls are those of the system it was written for; `engine/connectors/rdbms.py` lists them
for each protocol.

The logic systems read their rule files as they are. XSB (`.P`) uses tabling:

```prolog
:- auto_table.
path(X, Y) :- edge(X, Y).
path(X, Y) :- path(X, Z), edge(Z, Y).
```

Clingo (`.lp`) and Souffle (`.dl`) state the same rules in their own syntax, ending with
`#show path/2.` and `.output path` respectively. Neo4j (`.cypher`) runs a script of statements
separated by `;`: every statement but the last two is setup, the second to last is the timed query,
and the last exports the result. `systems/neo4j/rules/transitive_left_recursion.cypher` is the
working example, and [RULES.md](RULES.md) has more for every language.

### 5. Try it

```sh
python transitive.py --validate-rules my_db
python benchmark.py --systems my_db --graphs cycle path --sizes 100 200 --runs 2 --campaign /tmp/check
```

The second command runs through the whole engine, so it shows at once whether the results are
correct: every record in `/tmp/check/my_db/runs.jsonl` should have `"correct": true`.

## Protocols

| Protocol | Systems | Credentials | Rule files |
| --- | --- | --- | --- |
| `psycopg2` | PostgreSQL | `dbURL` | `.py` |
| `cockroachdb` | CockroachDB | `dbURL`, `externalDirectory` | `.py` |
| `mysqlclient` | MariaDB | `host`, `port`, `user`, `password`, `database` | `.py` |
| `singlestore` | SingleStore | `host`, `port`, `user`, `password`, `database` | `.py` |
| `duckdb` | DuckDB | none (in-process) | `.sql` |
| `neo4j` | Neo4j | `uri`, `user`, `password`, `import_directory` | `.cypher` |
| `pymongo` | MongoDB | `uri`, `database` | `.py` |
| `subprocess` | XSB | none (`xsb` on `PATH`) | `.P` |
| `souffle_subprocess` | Souffle | none (`souffle` and `g++` on `PATH`) | `.dl` |
| `clingo_python` | Clingo | none (Python binding) | `.lp` |
| `alda_subprocess` | ALDA (DistAlgo) | none | `.da` |

Installation and pitfalls of the measured systems are in [SYSTEMS.md](SYSTEMS.md).

### Adding a protocol

When no protocol fits, one connector class is enough. It can be dropped into the system's own
directory, where the engine finds it at start-up, so no engine file needs to change:

```python
# systems/my_system/connector.py
from engine.connectors.base import BaseConnector


class MySystemConnector(BaseConnector):
    """Runs transitive closure trials on My System."""

    def connect(self, credentials, descriptor):
        """Open the connection."""
        import my_driver

        self._connection = my_driver.connect(**credentials)

    def run_experiment(self, rule_path, input_path, output_folder, descriptor, config, query_bindings=None):
        """Time every step of one trial, one timing phase each."""
        phases = descriptor.timing_phases
        measurements = [(0.0, 0.0)] * len(phases)
        results_path = self.result_path(output_folder, descriptor, 'my_system_results.csv')
        try:
            measurements[0] = self.timed(self._load, input_path)[:2]
            ...
        except Exception as e:
            self._record_error(f'MySystem experiment error: {e}')   # a failure must never look like zeros
        return self.build_timing_row(phases, measurements)

    @classmethod
    def cancel_running(cls, credentials, descriptor):
        """Stop the statement the server is still running after a timeout."""

    def close(self):
        """Close the connection."""
        if self._connection:
            self._connection.close()
```

The descriptor then names the protocol, `protocol: my_system`. The class must be the only one in the
file whose name ends in `Connector`. A protocol meant for several systems belongs in
`engine/connectors/` instead, registered in `PROTOCOL_REGISTRY` in `engine/connectors/__init__.py`.
`self.timed_query` in place of `self.timed` for the query phase also records the query's memory, once
the connector returns a probe from `memory_sampler()`.

## Adding a graph family

A graph family needs a generator method and a descriptor. The method goes into the `DataGenerator`
class of `generate_db.py`, and its name must be `generate_<name>_graph`, because
`generate_db.py --graph-types <name>` looks it up by that name. It yields `(source, target)` edges for
n nodes:

```python
def generate_hexagonal_grid_graph(self, n: int) -> Generator[tuple[int, int], None, None]:
    """Generate a hexagonal grid of about n nodes."""
    rows = cols = int(n**0.5)
    for r in range(rows):
        for c in range(cols):
            node = r * cols + c + 1
            if c + 1 < cols:
                yield (node, node + 1)
            if r + 1 < rows:
                yield (node, node + cols - (c % 2))
```

The descriptor points to it through `engine/data_generator.py`, which re-exports the class so that
the dotted path resolves:

```yaml
# graph_types/hexagonal_grid.yaml
name: hexagonal_grid
display_name: Hexagonal Grid
description: A grid in which each cell connects to up to three neighbours.
generator: engine.data_generator.DataGenerator.generate_hexagonal_grid_graph
parameters: {}
```

The campaign engine generates the inputs of a new family the first time a campaign needs them. They
can also be written beforehand with `python generate_db.py --graph-types hexagonal_grid --size-list
100 200`. The topology pages of the web interface draw a small instance of every family, and
`ui/data.py` (`PREVIEW_N`, `_layout`) decides its size and layout; a family it does not know is laid
out by a force-directed layout.

## Adding a query domain

A domain is a kind of recursive query: the transitive closure, which is the domain all shipped rule
files implement, or shortest paths, reachability with constraints, and so on. A domain is described
by `domains/<name>/descriptor.yaml`; `templates/domain_shortest_path.yaml` and
`templates/domain_reachability_with_avoidance.yaml` are complete examples, and
`python transitive.py --bootstrap-domain shortest_path` copies one into place.

The descriptor lists the domain's modes, its query parameters and the columns of its result. The
engine runs, for each system, only the modes that the system's descriptor and the domain both
declare, and it looks for the rule files `rules/<domain>_<mode><extension>`:

```sh
python transitive.py --validate-domain shortest_path                    # every system has the rule files?
python transitive.py --domain shortest_path --modes iterative_deepening --systems duckdb --campaign results/sp
```

In the `shortest_path` domain, `generate_db.py` gives every edge a weight from a seeded random
generator and names the tab-separated file `edge_weighted.facts`. The engine, however, still looks for
`edge.facts` (`input_path()` in `engine/runner.py`), so the systems that read that file cannot run
this domain until the two names agree; this is the first thing a new domain with weights has to
settle. A query can be bound to a start node:
`generate_db.py` writes `queries_<n>.csv` next to each input, with one header naming the parameter
(`X`) and one row with its value. The script-based connectors (DuckDB, Neo4j) replace `?X` in the
rule text by that value, while the server databases pass the bindings to their operation classes
in `config['query_bindings']`. Results outside
the transitive-closure domain are not checked against anything (see
[VERIFICATION.md](VERIFICATION.md)).

## Checking rule files

```sh
python transitive.py --validate-rules my_db              # descriptor, rule file per mode, credentials
python transitive.py --validate-domain shortest_path --systems duckdb xsb
python transitive.py --test-rule systems/duckdb/rules/transitive_left_recursion.sql
python transitive.py --test-rule systems/duckdb/rules/transitive_left_recursion.sql --system duckdb
```

`--test-rule` checks a single file without running a benchmark: the brackets must balance, and each
language must show its own marks (a SELECT in SQL, `:-` in Datalog and Prolog, MATCH in Cypher). With
`--system`, it also asks a running PostgreSQL or DuckDB to parse the statement with `EXPLAIN`, inside a
transaction that is rolled back. The same checks are available on the system pages of the web
interface.

## Making a system ready for trustworthy measurements

Every campaign runs through the same engine (see [VERIFICATION.md](VERIFICATION.md)), so these points
apply to every system. Each comes from a problem met in the 2026 campaign, as
[SYSTEMS.md](SYSTEMS.md) records.

1. **Declare `query_phase` and `result_file`.** The first is the `id` of the phase that is the
   recursive query; the second is the file the connector writes the result to, through
   `self.result_path(output_folder, descriptor, default)`. The result must hold one pair per line,
   two integers separated by a comma, a tab or a space, with an optional header line.
2. **Report every failure.** Exceptions are caught only to clean up, and recorded with
   `self._record_error(...)`. `run_one` then exits with code 1, and the run is recorded as `error`. A
   timing row of zeros must never look like a successful run.
3. **Time complete work.** A timed call must return only when its step has been fully executed. Lazy
   drivers, Neo4j's `session.run()` among them, return early and run the statement later; their
   results must be fetched inside the timed call, and a driver that streams rows must be drained.
4. **Start clean.** A trial killed at the time limit leaves tables, files or server-side jobs
   behind. Hence tables are dropped and output files removed at the start of every trial, as the
   MariaDB, DuckDB and CockroachDB connectors do.
5. **Implement `cancel_running()` for client/server systems.** Killing the client does not stop the
   query in the server, and the next trial would then run on a loaded machine, or fail. It must be
   checked that the work really stops: CockroachDB, for instance, needs its schema-change jobs
   cancelled one by one.
6. **Declare only modes that are real formulations.** A system with one fixed formulation (MongoDB,
   Neo4j) should run one mode only, in `scripts/run_all.sh`. A mode the server rejects (double
   recursion in PostgreSQL) is still worth declaring, because the rejection is recorded.
7. **Check it.** A few configurations should give `"correct": true` in `runs.jsonl`, and the timeout
   and error paths should be tried as well (`--timeout 0.01`, wrong credentials). A connector test
   with a mocked driver belongs in `tests/`; `tests/test_verified_pipeline.py` has examples.

A system that is to appear in the paper's figures and tables needs two more things.
`analyze_verified.py` puts every series into `summary.csv` and `verification.json`, but its figures
and LaTeX tables list the paper's systems by name (the `series` list in `_draw_panel` and the
`table_*` functions). The system also needs its own entry in `engine/plot_style.py`
(`SYSTEM_STYLE`): a label, a colour and a marker that no other system uses, since the marker is what
tells the systems apart in black-and-white print. `tests/test_plot_style.py` checks that the markers
are unique, and that every figure uses them and orders its legends by the curves' last points. The
LaTeX version of each figure follows by itself, because `engine/figures_tex.py` transcribes whatever
the matplotlib code draws. It refuses artists it cannot transcribe, bars for example, and raises an
error for them, so nothing is left out quietly.

## Troubleshooting

**`Unknown protocol`.** The descriptor names a protocol that is neither in
the table above nor provided by a `connector.py` in the system's directory.

**`no rule file for my_db/transitive_left_recursion`.** For each mode in `modes`, the engine looks
for `rules/<domain>_<mode><rule_extension>`; with `modes: [left_recursion]` and
`rule_extension: .py`, that is `rules/transitive_left_recursion.py`. The first word of the domain
and the bare mode name are tried as well, so `rules/left_recursion.py` would also be found.

**`input ... not found`.** The input of a graph and size does not exist and was not generated,
which happens when a single trial is run with `python -m engine.run_one` directly. A campaign
generates its inputs first; `generate_db.py --graph-types <graph> --size-list <n>` writes one by
hand.

**A new graph family is not generated.** The method must be named `generate_<name>_graph`, and the
descriptor's `generator` must point to it. A quick test:
`python -c "from engine.data_generator import DataGenerator; print(list(DataGenerator().generate_my_graph(5)))"`.

**Credentials are not saved from the web interface.** The public, read-only deployment refuses every
change by design. Locally, the system's directory must exist and be writable.
