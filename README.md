# trans-bench · Plugin-Driven Benchmark Suite

> **Transitive closure benchmarking for logic and database systems — now extensible by anyone, no Python required.**

[![branch](https://img.shields.io/badge/branch-extends-6366f1)](#)
[![python](https://img.shields.io/badge/python-3.11%2B-3b82f6)](#)
[![license](https://img.shields.io/badge/license-MIT-10b981)](#)

---

## What changed in v2 (this branch)

The original suite required editing **6+ Python files** to add a new system. v2 introduces a **plugin-by-configuration** model:

| Before                                                                | After                                                                           |
| --------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| Edit `common.py`, `analyze_dbs.py`, `transitive.py`, `config.json`, … | Drop one `descriptor.yaml` + rule files                                         |
| Hardcoded system lists scattered through Python                       | Filesystem auto-discovery                                                       |
| No UI — CLI only                                                      | Full Web UI with live monitoring                                                |
| Credentials embedded in `config.json`                                 | Per-system `credentials.yaml` (gitignored)                                      |
| Single-domain (transitive closure only)                               | **Multi-domain benchmarking** (transitive, shortest_path, reachability, custom) |
| No extension templates                                                | **Bootstrap CLI + Web UI wizards + YAML templates**                             |
| No validation tooling                                                 | **Validation API + rule syntax checking**                                       |

---

## Published results and the verified benchmark

The measurements of *Database System Performance on Recursive Queries* (PostgreSQL, MariaDB,
DuckDB, CockroachDB, SingleStore, MongoDB, Neo4j and XSB; 12,279 runs, each checked for
correctness, with memory measurements) are in [`results/verified_2026_v2/`](results/verified_2026_v2/README.md),
together with
everything needed to check them:

```sh
python analyze_verified.py results/verified_2026_v2 --out /tmp/reanalysis   # re-derives the paper's tables
python -m pytest -q tests                                                # incl. a byte-for-byte check of them
```

There are two ways to run experiments:

| | `transitive.py` (and the Web UI) | `benchmark.py` |
| --- | --- | --- |
| Purpose | quick, interactive experiments | trustworthy measurements (used for the paper) |
| Isolation | all trials in one process | one process per trial (`engine/run_one.py`) |
| Time limit | none | per trial; process group killed and the server-side query cancelled |
| Failures | logged | recorded per run (`error`/`timeout`); larger sizes skipped |
| Correctness | not checked | every result checked against an independent closure (count + 64-bit hash) |
| Output | `timing/…` CSVs; `generate_plot_table.py` charts | `runs.jsonl` + logs + timing CSVs; at the end the campaign is analyzed automatically (`analyze_verified.py`): tables, and every figure as matplotlib PDF and as LaTeX (pgfplots/TikZ, compiled to PDF) |

* [docs/REPRODUCING.md](docs/REPRODUCING.md): verify the published data, or re-run the campaign (exact commands, incidents).
* [docs/SYSTEMS.md](docs/SYSTEMS.md): installation, configuration and pitfalls for each system (e.g. Neo4j's lazy results, CockroachDB's export chunks and schema-change jobs, MariaDB's silently incomplete results, SingleStore's `UNION ALL`-only recursion, DuckDB's `recurring`).
* [docs/VERIFICATION.md](docs/VERIFICATION.md): the correctness check, the run protocol, and the `runs.jsonl` format.

---

## Why Trans-Bench v2?

✨ **Zero-Python Extension** — Add systems, domains, and queries entirely via YAML + rule files  
🎯 **Multi-Domain Benchmarking** — Compare implementations of transitive closure, shortest path (with weighted graphs), reachability, etc.  
⚡ **Demand-Driven Execution** — Built-in support for generating `queries.csv` to benchmark specific point-to-point queries.
📊 **Hybrid Resource Profiling** — Track both runtime AND peak memory usage across distinct execution phases.
🎨 **Modern Interactive Web UI** — System creation wizards, live progress monitoring, and an advanced **Trend Analysis** dashboard for stacked/line comparative charting.
🧪 **Validation & Testing** — Built-in CLI commands to validate rules before benchmarking  
📚 **Comprehensive Docs** — EXTENSION_GUIDE, RULES reference, step-by-step COOKBOOK

---

## Architecture

```
trans-bench/
├── systems/                    ← One directory per benchmarked system
│   ├── postgres/
│   │   ├── descriptor.yaml     ← Single source of truth for PostgreSQL
│   │   ├── credentials.yaml    ← Local secrets (gitignored; see credentials.example.yaml)
│   │   └── rules/              ← SQL/Cypher/Prolog rule files
│   ├── neo4j/  xsb/  clingo/  souffle/  mariadb/  duckdb/  mongodb/  cockroachdb/  singlestore/  alda/
│   └── <your_new_system>/      ← Adding a system = creating this folder
│
├── graph_types/                ← One YAML per graph topology (15 included)
│   ├── cycle.yaml
│   ├── barabasi_albert.yaml
│   └── …
│
├── generate_db.py              ← Generates graph facts and demand-driven queries (queries_*.csv)
├── input/                      ← Generated inputs (≤ n=500 tracked), SHA256SUMS, expected_closures.json
├── engine/                     ← Core framework (rarely needs editing)
│   ├── loader.py               ← Reads descriptors at runtime
│   ├── runner.py               ← Orchestrates experiments
│   ├── run_one.py              ← Runs ONE trial in its own process (used by benchmark.py)
│   ├── verify.py               ← Independent correctness check (count + order-independent hash)
│   ├── figures_tex.py          ← matplotlib figure → standalone pgfplots/TikZ document (+ compilation)
│   └── connectors/
│       ├── base.py             ← Abstract connector interface (errors, cancel_running)
│       ├── rdbms.py            ← PostgreSQL, MariaDB, CockroachDB, SingleStore
│       ├── duckdb_conn.py
│       ├── neo4j_conn.py
│       ├── mongodb_conn.py
│       └── subprocess_conn.py  ← XSB, Clingo, Soufflé, Alda
│
├── ui/                         ← Flask Web UI (Phase 4)
│   ├── app.py
│   ├── templates/
│   └── static/css/app.css      ← Modernized responsive styling
│
├── config.yaml                 ← Global config (no credentials)
├── transitive.py               ← CLI entrypoint (also launches UI)
├── benchmark.py                ← Verified driver: time limit, isolation, per-run correctness check
├── analyze_verified.py         ← Summary, verification, figures (PDF + pgfplots/TikZ) and LaTeX tables
├── scripts/                    ← Campaign scripts (run_all.sh, capture_versions.sh, verify_inputs.py,
│                                 compare_results.py, MariaDB investigation)
├── results/verified_2026_v2/   ← The paper's campaign (per-run records incl. memory, logs, analysis)
├── results/verified_2026/      ← The first campaign (same protocol, without memory), and its incident log
└── docs/                       ← REPRODUCING, SYSTEMS, VERIFICATION, EXTENSION_GUIDE, RULES, COOKBOOK
```

---

## Quick Start

### 1. Clone & install

```sh
git clone https://github.com/Sirneij/trans-bench.git
cd trans-bench
git checkout extends

python3.12 -m venv virtualenv
source virtualenv/bin/activate
# mysqlclient builds against the MariaDB/MySQL client library:
PKG_CONFIG_PATH=/opt/homebrew/opt/mariadb/lib/pkgconfig pip install -r requirements.txt
```

### 2. Configure a system

Copy `systems/<name>/credentials.example.yaml` to `systems/<name>/credentials.yaml` and edit it (the UI also creates it on first save). How to install and configure each server: [docs/SYSTEMS.md](docs/SYSTEMS.md).

```yaml
# systems/postgres/credentials.yaml
dbURL: postgres://user:password@localhost:5432/benchmarkdb
```

```yaml
# systems/neo4j/credentials.yaml
uri: neo4j://localhost:7687
user: neo4j
password: secret
import_directory: /opt/homebrew/Cellar/neo4j/2026.04.0/libexec/import
```

### 3. Launch the Web UI

```sh
python transitive.py --ui
# → Open http://127.0.0.1:5000
```

### 4. Or run from CLI

```sh
# All discovered systems, all graph types
python transitive.py

# Specific systems and graphs
python transitive.py --systems postgres xsb --graphs cycle path --sizes 100 1001 100

# Custom recursion modes and runs
python transitive.py --modes right_recursion left_recursion --num-runs 5

# Verified runs (time limit, isolation, correctness check), e.g. 5 runs of DuckDB on two graphs
python benchmark.py --systems duckdb --graphs cycle path --sizes 100 200 300 --out results/my_run/duckdb
```

---

## Web UI Guide

| Page             | URL                    | What you can do                                                                                       |
| ---------------- | ---------------------- | ----------------------------------------------------------------------------------------------------- |
| Overview         | `/`                    | Latest campaigns with their outcomes, systems, topologies, the commands to start a campaign          |
| Campaigns        | `/campaigns`           | Every verified campaign under `results/` (benchmark.py), with completed, failed and skipped runs     |
| Campaign         | `/campaigns/<name>`    | Outcome per series, verification, environment; scaling chart; time/memory heat matrix; the paper's figures; filterable failures; README, versions and code patch |
| Results explorer | `/results`             | Timing files of transitive.py runs: phase-by-phase comparison of systems, and every run of a file    |
| Systems          | `/systems`             | Registered systems with connector, version, modes and credential status                              |
| System           | `/systems/<name>`      | Timing phases; edit the descriptor, rule files and credentials (hidden until revealed)                |
| Topologies       | `/graphs`              | Every graph family drawn from its own generator                                                      |
| Topology         | `/graphs/<name>`       | Change the size of a drawn instance, overlay the pairs its transitive closure adds, definition and generator code |
| New experiment   | `/experiment/new`      | Wizard: systems → topologies → settings → review; start here or copy the transitive.py / benchmark.py command |
| Live monitor     | `/experiment/live`     | Progress, current configuration, per-system counts, filterable output; stop after the current configuration |
| Domains          | `/domains/new`         | Existing query domains and a form to add one from a template                                         |

Press <kbd>⌘K</kbd> (or <kbd>/</kbd>) anywhere to jump to a page, system, topology or campaign; <kbd>n</kbd> starts a new
experiment and <kbd>t</kbd> switches between the system, light and dark themes. On macOS, port 5000 is often taken by
AirPlay Receiver; use `python transitive.py --ui --ui-port 5055` then.

---

## Adding a New System

No Python code changes required after Phase 3. Here's the complete workflow:

### Step A — Create the system folder

```sh
# Option 1: use the Web UI → /systems/new
# Option 2: CLI
cp -r systems/postgres systems/my_new_db
```

### Step B — Edit the descriptor

```yaml
# systems/my_new_db/descriptor.yaml
name: my_new_db
display_name: My New Database
category: db # db | logic | hybrid
protocol: psycopg2 # reuse an existing connector protocol

timing_phases:
  - { id: create_table, label: CreateTable }
  - { id: load_data, label: LoadData }
  - { id: execute_query, label: ExecuteQuery }
  - { id: write_result, label: WriteResult }

input_format: tsv
modes: [right_recursion, left_recursion]
rule_extension: .sql

flags:
  requires_credentials: true
```

### Step C — Add credentials

```yaml
# systems/my_new_db/credentials.yaml  (gitignored)
dbURL: postgres://user:pass@localhost:5433/mydb
```

### Step D — Write rule files

```sql
-- systems/my_new_db/rules/transitive_right_recursion.sql
CREATE TABLE tc_result AS
WITH RECURSIVE tc AS (
    SELECT x, y FROM edge
    UNION
    SELECT edge.x, tc.y FROM edge JOIN tc ON edge.y = tc.x
)
SELECT * FROM tc;
```

### Step E — Run

```sh
python transitive.py --systems my_new_db --graphs cycle path
```

> **That's it.** No edits to any Python source file.

---

## Adding a New Graph Type

### Step A — Write a generator function

Add to `generate_db.py` → `DataGenerator`:

```python
def generate_my_graph(self, n: int) -> Generator[tuple, None, None]:
    """My custom graph topology."""
    for i in range(1, n):
        # Yield (src, dst) for standard graphs
        # Or (src, dst, weight) for weighted domains like shortest_path
        yield (i, i + 2)
```

To support **Demand-Driven Queries**, graph generators automatically integrate with the sampling engine to produce `queries.csv` for targeted node-to-node evaluation.

### Step B — Create the descriptor

```yaml
# graph_types/my_graph.yaml
name: my_graph
display_name: My Custom Graph
description: Each node connects to the node two steps ahead.
generator: engine.data_generator.DataGenerator.generate_my_graph
parameters: {}
```

### Step C — Run

```sh
python transitive.py --graphs my_graph
```

---

## Adding a New Protocol (Connector)

If your system uses a driver not yet supported, implement one connector class:

```python
# engine/connectors/my_protocol.py
from engine.connectors.base import BaseConnector

class MyProtocolConnector(BaseConnector):
    def connect(self, credentials, descriptor):
        self._conn = my_driver.connect(**credentials)

    def run_experiment(self, rule_path, input_path, output_folder, descriptor, config, query_bindings=None):
        phases = descriptor.timing_phases
        measurements = [(0.0, 0.0)] * len(phases)
        results_path = self.result_path(output_folder, descriptor, 'my_results.csv')
        try:
            measurements[0] = self.timed(load, input_path)[:2]   # ... time each phase ...
        except Exception as e:
            self._record_error(f'MyProtocol error: {e}')      # never hide a failure behind zeros
        return self.build_timing_row(phases, measurements)

    @classmethod
    def cancel_running(cls, credentials, descriptor):
        ...  # client/server systems: stop the query the server is still running after a timeout

    def close(self):
        if self._conn:
            self._conn.close()
```

For verified runs with `benchmark.py`, also set `query_phase` and `result_file` in the descriptor
(see [docs/EXTENSION_GUIDE.md](docs/EXTENSION_GUIDE.md#making-a-system-ready-for-verified-runs)).

Register it in `engine/connectors/__init__.py`:

```python
from engine.connectors.my_protocol import MyProtocolConnector

PROTOCOL_REGISTRY['my_protocol'] = MyProtocolConnector
```

Then set `protocol: my_protocol` in your `descriptor.yaml`. This is a **one-time** addition per driver family — all future systems using that driver need zero connector code.

---

## Descriptor Reference

```yaml
name: system_name # snake_case, matches directory name
display_name: Human Name # shown in Web UI
category: db # db | logic | hybrid

protocol: psycopg2 # connector to use (see engine/connectors/__init__.py)

timing_phases: # defines CSV column headers AND execution order
  - id: create_table # internal ID (snake_case)
    label: CreateTable # CSV prefix → CreateTableRealTime, CreateTableCPUTime

query_phase: execute_query # id of the phase that is the query itself (reported by analyze_verified.py)
result_file: postgres_results.csv # file with the query result, checked by benchmark.py after every run

input_format: tsv # tsv | lp | facts | pickle
modes: # which rule files to look for
  - right_recursion
  - left_recursion
rule_extension: .sql # file extension for rule files

execution: {} # protocol-specific hints (see subprocess systems)

flags:
  requires_credentials: true
  class_prefix: PostgreSQL # for Python-class-based rules (RDBMS)
  module_prefix: postgres_rules
```

---

## Credential Files Reference

Each system that needs credentials gets a `systems/<name>/credentials.yaml`:

```yaml
# PostgreSQL / CockroachDB
dbURL: postgres://user:pass@host:port/db
# CockroachDB only: = the server's --external-io-dir, with a trailing slash
externalDirectory: /path/to/crdb-extern/

# MariaDB
host: localhost
user: root
password: secret
database: benchmark
port: 3306

# Neo4j
uri: neo4j://localhost:7687
user: neo4j
password: secret
import_directory: /path/to/neo4j/import

# MongoDB
uri: mongodb://127.0.0.1:27017/
database: test

# SingleStore (MySQL protocol)
host: 127.0.0.1
port: 3307
user: root
password: secret
database: benchmark
```

Every server system has a `credentials.example.yaml` with the values used for the published campaign.

Credential files are **gitignored** by default. The Web UI saves them through the System Detail → Credentials tab.

---

## Supported Systems (built-in)

| System          | Category | Protocol           | Modes                                        |
| --------------- | -------- | ------------------ | -------------------------------------------- |
| PostgreSQL      | db       | psycopg2           | right, left, double (rejected by the server) |
| MariaDB         | db       | mysqlclient        | right, left, double                          |
| DuckDB          | db       | duckdb             | right, left, double (incomplete), doublerecurring |
| Neo4j           | db       | neo4j              | one Cypher query (same file for all modes)   |
| MongoDB         | db       | pymongo            | one `$graphLookup` pipeline (same for all modes) |
| CockroachDB     | db       | cockroachdb        | right, left, double (rejected by the server) |
| SingleStore     | db       | singlestore        | right, left (acyclic graphs only), double (rejected) |
| XSB Prolog      | logic    | subprocess         | right, left, double                          |
| Clingo (ASP)    | logic    | clingo_python      | right, left, double |
| Soufflé         | logic    | souffle_subprocess | right, left, double |
| Alda (DistAlgo) | logic    | alda_subprocess    | right, left, double |

The first eight systems were measured in the published campaign; see [docs/SYSTEMS.md](docs/SYSTEMS.md) for why some modes are rejected or incomplete.

---

## Supported Graph Topologies (built-in)

`complete` · `cycle` · `cycle_with_shortcuts` · `star` · `max_acyclic` · `path` · `multi_path` · `binary_tree` · `reverse_binary_tree` · `grid` · `w` · `y` · `x` · `barabasi_albert` · `scale_free`

---

## Legacy CLI Compatibility

The original per-run programs `analyze_dbs.py` and `analyze_logic_systems.py` were replaced by the
connectors (`engine/connectors/`) and `engine/run_one.py`. The original `transitive.py` option
`--environments` is not accepted any more: use `--systems`.

```sh
python transitive.py --sizes 100 1001 100 --modes right_recursion left_recursion \
  --systems postgres mariadb duckdb --num-runs 5
```

---

## Extensibility for Everyone

**No Python knowledge required.** Trans-Bench is designed to be extended by anyone — database experts, domain researchers, or data engineers.

### What you can add without touching Python code:

| Extension Type          | Python? | Effort  | Method                                               | Guide                                                              |
| ----------------------- | ------- | ------- | ---------------------------------------------------- | ------------------------------------------------------------------ |
| New SQL/graph database  | ❌ No   | 5 min   | Copy descriptor, write SQL/Cypher rules              | [EXTENSION_GUIDE.md](docs/EXTENSION_GUIDE.md#adding-a-new-system)       |
| New logic engine (CLI)  | ❌ No   | 5 min   | Descriptor with `protocol: subprocess`               | [EXTENSION_GUIDE.md](docs/EXTENSION_GUIDE.md#adding-a-new-system)       |
| New connector protocol  | ⚠️ Once | 20 min  | Drop `systems/<name>/connector.py` (auto-discovered) | [EXTENSION_GUIDE.md](docs/EXTENSION_GUIDE.md#adding-a-new-protocol)     |
| New graph topology      | ⚠️ Once | 15 min  | Add Python method in `generate_db.py`                | [EXTENSION_GUIDE.md](docs/EXTENSION_GUIDE.md#adding-a-new-graph-type)   |
| New query domain        | ❌ No   | 20 min  | Create `domains/<name>/descriptor.yaml`, write rules | [EXTENSION_GUIDE.md](docs/EXTENSION_GUIDE.md#adding-a-new-query-domain) |
| Custom query rules      | ❌ No   | 10 min  | Edit SQL/Cypher/Datalog files                        | [RULES.md](docs/RULES.md)                                               |

> **Note on graph topologies**: The YAML descriptor still needs a Python generator method as its backing implementation. The method is a ~5-line function that yields `(src, dst)` tuples — minimal Python, but honest about the requirement.

### Quick-start for extensions

**Via CLI:**

```sh
# Bootstrap a new SQL system from template
python transitive.py --bootstrap-system my_database --bootstrap-system-template descriptor_sql_database.yaml

# Create a new query domain
python transitive.py --bootstrap-domain my_domain

# Validate all rule files for a system
python transitive.py --validate-rules my_system

# Validate a domain against all systems (checks every system has the right rule files)
python transitive.py --validate-domain shortest_path

# Validate a domain against specific systems only
python transitive.py --validate-domain shortest_path --systems postgres clingo

# Test a single rule file (static syntax check)
python transitive.py --test-rule systems/postgres/rules/transitive_right_recursion.sql

# Test a rule file with live dry-run against a connected system
python transitive.py --test-rule systems/postgres/rules/transitive_right_recursion.sql --system postgres

# Run with domain-specific modes (any mode string accepted — invalid modes skipped per system)
python transitive.py --domain shortest_path --modes dijkstra_style iterative_deepening
```

**Via Web UI:**

1. Launch the UI: `python transitive.py --ui`
2. Navigate to **Systems** → **Register a system**
3. Enter name, choose template, customize descriptor
4. Add credentials and rule files
5. Run experiments

### Extension Documentation

| Document                                     | Topic                                              | Audience          |
| -------------------------------------------- | -------------------------------------------------- | ----------------- |
| [**EXTENSION_GUIDE.md**](docs/EXTENSION_GUIDE.md) | Complete how-to for all extension types            | Everyone          |
| [**RULES.md**](docs/RULES.md)                     | Query rule examples for SQL, Datalog, Cypher, etc. | Rule writers      |
| [**COOKBOOK.md**](docs/COOKBOOK.md)               | Step-by-step recipes (SQLite, shortest path, etc.) | Hands-on learners |
| [**SYSTEMS.md**](docs/SYSTEMS.md)                 | Setup and pitfalls of every measured system        | Benchmark runners |
| [**REPRODUCING.md**](docs/REPRODUCING.md)         | Verify or re-run the published campaign            | Reviewers         |
| [**VERIFICATION.md**](docs/VERIFICATION.md)       | Correctness check, run protocol, record format     | Everyone          |
| [**templates/**](templates/)                 | Ready-to-customize YAML and rule templates         | Quick starters    |

### Bootstrap Templates

Pre-built templates for common scenarios:

```
templates/
├── descriptor_sql_database.yaml           # PostgreSQL, MySQL, CockroachDB
├── descriptor_graph_database.yaml         # Neo4j, Memgraph
├── descriptor_logic_engine.yaml           # XSB, Clingo, Soufflé
├── rule_template_sql_right_recursion.sql
├── rule_template_datalog_right_recursion.lp
├── rule_template_cypher_right_recursion.cypher
├── domain_shortest_path.yaml
├── domain_reachability_with_avoidance.yaml
└── README.md                              # Template usage guide
```

Copy, customize, and deploy — no Python edits required.

---

## Running Tests

```sh
python -m pytest -q tests
```

The tests need no database server. The end-to-end tests of `benchmark.py` and `engine/run_one.py`
use DuckDB on the tracked inputs, and `TestAnalysis` re-derives the published tables from
`results/verified_2026`.

---

## Requirements

`requirements.txt` pins the versions of the published campaign (Python 3.12). The pins matter most
for `duckdb` (results and the `recurring` feature), `networkx` (the seeded scale-free and
Barabási-Albert generators) and `numpy` (verification). Install everything with
`pip install -r requirements.txt`.
