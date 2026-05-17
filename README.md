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
│   │   ├── credentials.yaml    ← Local secrets (gitignored)
│   │   └── rules/              ← SQL/Cypher/Prolog rule files
│   ├── neo4j/  xsb/  clingo/  souffle/  mariadb/  duckdb/  mongodb/  cockroachdb/  alda/
│   └── <your_new_system>/      ← Adding a system = creating this folder
│
├── graph_types/                ← One YAML per graph topology (15 included)
│   ├── cycle.yaml
│   ├── barabasi_albert.yaml
│   └── …
│
├── engine/                     ← Core framework (rarely needs editing)
│   ├── loader.py               ← Reads descriptors at runtime
│   ├── runner.py               ← Orchestrates experiments
│   ├── data_generator.py       ← Generates graph facts and demand-driven queries (queries.csv)
│   └── connectors/
│       ├── base.py             ← Abstract connector interface
│       ├── rdbms.py            ← PostgreSQL, MariaDB, CockroachDB
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
└── transitive.py               ← CLI entrypoint (also launches UI)
```

---

## Quick Start

### 1. Clone & install

```sh
git clone https://github.com/Sirneij/trans-bench.git
cd trans-bench
git checkout extends

python3 -m venv virtualenv
source virtualenv/bin/activate
pip install -r requirements.txt
```

### 2. Configure a system

Edit `systems/<name>/credentials.yaml` (created automatically on first UI save, or manually):

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
```

---

## Web UI Guide

| Page           | URL                | What you can do                                           |
| -------------- | ------------------ | --------------------------------------------------------- |
| Dashboard      | `/`                | Overview stats, quick actions                             |
| Systems        | `/systems`         | See all registered systems, credential status             |
| System Detail  | `/systems/<name>`  | Edit descriptor YAML, save credentials, browse rule files |
| Add System     | `/systems/new`     | Clone a template to bootstrap a new system                |
| New Experiment | `/experiment/new`  | Multi-step wizard: pick systems → graphs → settings → run |
| Live Monitor   | `/experiment/live` | SSE-powered real-time progress + log stream               |
| Results        | `/results`         | Full **Data Explorer**, **File Viewer**, and **Trend Analysis** charts (Stacked & Line comparisons) |

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

    def run_experiment(self, rule_path, input_path, output_folder, descriptor, config):
        phases = descriptor.timing_phases
        measurements = []
        # ... time each phase ...
        return self.build_timing_row(phases, measurements)

    def close(self):
        if self._conn:
            self._conn.close()
```

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
```

Credential files are **gitignored** by default. The Web UI saves them through the System Detail → Credentials tab.

---

## Supported Systems (built-in)

| System          | Category | Protocol           | Modes               |
| --------------- | -------- | ------------------ | ------------------- |
| PostgreSQL      | db       | psycopg2           | right, left, double |
| MariaDB         | db       | mysqlclient        | right, left, double |
| DuckDB          | db       | duckdb             | right, left, double |
| Neo4j           | db       | neo4j              | right, left, double |
| MongoDB         | db       | pymongo            | right, left, double |
| CockroachDB     | db       | psycopg2           | right, left, double |
| XSB Prolog      | logic    | subprocess         | right, left, double |
| Clingo (ASP)    | logic    | clingo_python      | right, left, double |
| Soufflé         | logic    | souffle_subprocess | right, left, double |
| Alda (DistAlgo) | logic    | alda_subprocess    | right, left, double |

---

## Supported Graph Topologies (built-in)

`complete` · `cycle` · `cycle_with_shortcuts` · `star` · `max_acyclic` · `path` · `multi_path` · `binary_tree` · `reverse_binary_tree` · `grid` · `w` · `y` · `x` · `barabasi_albert` · `scale_free`

---

## Legacy CLI Compatibility

The original `transitive.py` arguments still work:

```sh
python transitive.py --sizes 100 1001 100 --modes right_recursion left_recursion \
  --environments postgres mariadb duckdb --num-runs 5
```

`--environments` is accepted as an alias for `--systems` in this version.

---

## Extensibility for Everyone

**No Python knowledge required.** Trans-Bench is designed to be extended by anyone — database experts, domain researchers, or data engineers.

### What you can add without touching Python code:

| Extension Type          | Python? | Effort  | Method                                               | Guide                                                              |
| ----------------------- | ------- | ------- | ---------------------------------------------------- | ------------------------------------------------------------------ |
| New SQL/graph database  | ❌ No   | 5 min   | Copy descriptor, write SQL/Cypher rules              | [EXTENSION_GUIDE.md](EXTENSION_GUIDE.md#adding-a-new-system)       |
| New logic engine (CLI)  | ❌ No   | 5 min   | Descriptor with `protocol: subprocess`               | [EXTENSION_GUIDE.md](EXTENSION_GUIDE.md#adding-a-new-system)       |
| New connector protocol  | ⚠️ Once | 20 min  | Drop `systems/<name>/connector.py` (auto-discovered) | [EXTENSION_GUIDE.md](EXTENSION_GUIDE.md#adding-a-new-protocol)     |
| New graph topology      | ⚠️ Once | 15 min  | Add Python method in `engine/data_generator.py`      | [EXTENSION_GUIDE.md](EXTENSION_GUIDE.md#adding-a-new-graph-type)   |
| New query domain        | ❌ No   | 20 min  | Create `domains/<name>/descriptor.yaml`, write rules | [EXTENSION_GUIDE.md](EXTENSION_GUIDE.md#adding-a-new-query-domain) |
| Custom query rules      | ❌ No   | 10 min  | Edit SQL/Cypher/Datalog files                        | [RULES.md](RULES.md)                                               |

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
2. Navigate to **→ Systems** → **Add New System**
3. Enter name, choose template, customize descriptor
4. Add credentials and rule files
5. Run experiments

### Extension Documentation

| Document                                     | Topic                                              | Audience          |
| -------------------------------------------- | -------------------------------------------------- | ----------------- |
| [**EXTENSION_GUIDE.md**](EXTENSION_GUIDE.md) | Complete how-to for all extension types            | Everyone          |
| [**RULES.md**](RULES.md)                     | Query rule examples for SQL, Datalog, Cypher, etc. | Rule writers      |
| [**COOKBOOK.md**](COOKBOOK.md)               | Step-by-step recipes (SQLite, shortest path, etc.) | Hands-on learners |
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
python -m unittest discover -s tests
```

---

## Requirements

```
flask
pyyaml
psycopg2-binary
mysqlclient
duckdb
neo4j
pymongo
clingo
pexpect
networkx
matplotlib
```

Install all: `pip install -r requirements.txt`
