# Trans-Bench Extension Guide

> **For anyone — no Python knowledge required.** This guide covers everything you need to extend Trans-Bench with new systems, query domains, graph types, and custom rules.

---

## Table of Contents

1. [Quick Overview](#quick-overview)
2. [Adding a New System (Database/Logic Engine)](#adding-a-new-system)
3. [Adding a New Query Domain](#adding-a-new-query-domain)
4. [Adding a New Graph Type](#adding-a-new-graph-type)
5. [Adding Custom Query Rules](#adding-custom-query-rules)
6. [Validation & Testing](#validation--testing)
7. [Making a System Ready for Verified Runs](#making-a-system-ready-for-verified-runs)
8. [Troubleshooting](#troubleshooting)

---

## Quick Overview

The **entire** Trans-Bench framework is driven by **YAML configuration files** and **rule files** (SQL, Cypher, Datalog, etc.). You **never need to edit Python code** unless you're adding a brand new protocol that doesn't exist yet.

### What can you extend without Python?

✅ **Add new systems** (PostGIS, Memgraph, Cassandra, etc.) — just YAML + rule files  
✅ **Add new domains** (shortest_path, reachability_with_avoidance, etc.) — YAML + rule files  
✅ **Add new query rules** for any system — SQL, Cypher, Datalog, Prolog  
✅ **Add a new connector protocol** — drop `connector.py` into any system folder (auto-discovered)  
⚠️ **Add new graph topologies** — requires one small Python generator method (~5 lines)

### Core principle

Every "thing" is described by a **descriptor file** (`descriptor.yaml`), and the engine auto-discovers it:

```
systems/
├── postgres/
│   ├── descriptor.yaml      ← Tells the engine what postgres can do
│   ├── credentials.yaml     ← Your DB connection (gitignored)
│   └── rules/               ← SQL files with the actual queries
├── my_new_db/              ← You add this
│   ├── descriptor.yaml      ← Copy template, tweak a few fields
│   ├── credentials.yaml     ← Your credentials
│   └── rules/               ← Your SQL/Cypher/etc files
```

---

## Adding a New System

### Step 1: Bootstrap the system directory

**Option A — Use the bootstrap command:**

```sh
python transitive.py --bootstrap-system my_new_db --template postgres
```

This creates:

```
systems/my_new_db/
├── descriptor.yaml
├── credentials.yaml
└── rules/
    ├── transitive_right_recursion.sql
    ├── transitive_left_recursion.sql
    └── transitive_double_recursion.sql
```

**Option B — Manual copy (same result):**

```sh
cp -r systems/postgres systems/my_new_db
```

### Step 2: Edit the descriptor

Open `systems/my_new_db/descriptor.yaml`. Most fields are pre-filled correctly. Here's what you might need to change:

```yaml
name: my_new_db # Keep this: matches folder name
display_name: My New Database # What users see in the Web UI
category: db # db | logic | hybrid

protocol: psycopg2 # How to connect (see table below)

timing_phases: # What we measure (mostly pre-configured)
  - id: create_table
    label: CreateTable
  - id: load_data
    label: LoadData
  - id: execute_query
    label: ExecuteQuery
  - id: write_result
    label: WriteResult

input_format: tsv # Data format (tsv | lp | facts)
modes: # Which query patterns to use
  - right_recursion
  - left_recursion
  - double_recursion

rule_extension: .sql # File extension in rules/ folder
```

### Step 3: Save credentials

Create/edit `systems/my_new_db/credentials.yaml` (this file is **gitignored**):

```yaml
# PostgreSQL variant
dbURL: postgres://user:password@localhost:5432/benchmarkdb

# MySQL variant
host: localhost
user: root
password: secret
database: benchmark

# Neo4j variant
uri: neo4j://localhost:7687
user: neo4j
password: secret

# MongoDB variant
uri: mongodb://127.0.0.1:27017/
database: benchmark
```

> **Tip**: The Web UI has a "Credentials" tab in the System Detail page where you can enter these interactively instead of editing YAML.

### Step 4: Write query rules

Create/edit files in `systems/my_new_db/rules/`:

```sql
-- systems/my_new_db/rules/transitive_right_recursion.sql
CREATE TABLE tc_result AS
WITH RECURSIVE tc AS (
    SELECT x, y FROM edge
    UNION ALL
    SELECT edge.x, tc.y
    FROM edge
    JOIN tc ON edge.y = tc.x
)
SELECT * FROM tc;
```

> **Tip**: See [Adding Custom Query Rules](#adding-custom-query-rules) below for templates for each protocol.

### Step 5: Test it!

```sh
python transitive.py --systems my_new_db --graphs cycle path --sizes 100 1001 100
```

---

## Adding a New Query Domain

A **query domain** is a category of recursive queries (e.g., `transitive_closure`, `shortest_path`, `reachability_with_avoidance`). Multi-domain support lets you benchmark the same system across different query types.

### Step 1: Create a domain descriptor

Create `domains/<domain_name>/descriptor.yaml`:

```yaml
name: shortest_path
display_name: Shortest Path Computation
description: Find shortest paths from source to all reachable nodes
category: graph_optimization

# Query parameters that apply to this domain
query_parameters:
  source_node: int # Required: starting node
  target_node: int # Optional: if you want single-pair paths
  edge_weight: integer # Optional: if edges have weights

# Output schema: what columns the result should have
output_schema:
  - column: source
    type: int
  - column: target
    type: int
  - column: distance
    type: int

# Supported recursion modes for this domain
modes:
  - iterative_deepening # BFS-like level-by-level
  - dynamic_programming # Top-down memoization
  - bottom_up # Classic forward chaining

# Data generation hints
data_requirements:
  requires_weighted_edges: true
  allows_cycles: false
```

### Step 2: Create rule files for each system

Create `systems/<system>/rules/shortest_path_iterative_deepening.sql`:

```sql
-- Example for PostgreSQL
WITH RECURSIVE sp AS (
    SELECT x, y, weight AS dist FROM edge WHERE x = ?source_node
    UNION ALL
    SELECT sp.x, e.y, sp.dist + e.weight
    FROM sp
    JOIN edge e ON sp.y = e.x
    WHERE sp.dist + e.weight < 1000000  -- Prevent infinite loops
)
SELECT DISTINCT x, y, MIN(dist) FROM sp GROUP BY x, y;
```

> **Tip**: Use `?parameter_name` placeholders in your rules. The engine will substitute them at runtime.

### Step 3: Test the domain

```sh
python transitive.py --domains shortest_path --systems postgres --graphs cycle
```

---

## Adding a New Graph Type

A **graph type** is a topology generator (e.g., cycle, star, binary tree).

### Option 1: Use a simple Python generator (minimal Python required)

Add your method to the `DataGenerator` class in `generate_db.py` (the class that generates every benchmark input; `engine/data_generator.py` only re-exports it so that the dotted `generator:` paths resolve). The method name must be `generate_<graph type name>_graph`, because `generate_db.py --graph-types <name>` looks it up by that name:

```python
def generate_hexagonal_grid_graph(self, n: int) -> Generator[tuple[int, int], None, None]:
    """Generate a hexagonal grid of approximately n nodes."""
    rows = int(n ** 0.5)
    cols = rows
    for r in range(rows):
        for c in range(cols):
            node = r * cols + c
            # Right neighbor
            if c + 1 < cols:
                yield (node, node + 1)
            # Down-right neighbor
            if r + 1 < rows and c % 2 == 0:
                yield (node, (r + 1) * cols + c)
            # Down-left neighbor
            if r + 1 < rows and c % 2 == 1:
                yield (node, (r + 1) * cols + c - 1)
```

### Option 2: Create a descriptor (YAML only)

Create `graph_types/my_graph.yaml`:

```yaml
name: hexagonal_grid
display_name: Hexagonal Grid
description: A hexagonal grid where each cell connects to 3-6 neighbors

# This points to the Python generator above
generator: engine.data_generator.DataGenerator.generate_hexagonal_grid_graph

# Optional parameters the user can adjust
parameters:
  # Leave empty if no parameters needed
```

### Step 3: Test it

```sh
python transitive.py --graphs hexagonal_grid --systems postgres --sizes 100 1001 100
```

---

## Adding Custom Query Rules

Rules are the actual queries/programs that run on each system. They define "what" to compute; the system handles "how".

### SQL Rules (PostgreSQL, MariaDB, CockroachDB, DuckDB)

Create `systems/postgres/rules/transitive_right_recursion.sql`:

```sql
-- Right recursion: build up from base edges
-- rule_id: transitive_right_recursion
-- domain: transitive_closure
-- execution_mode: top_down_build

CREATE TEMP TABLE tc_result AS
WITH RECURSIVE tc AS (
    -- Base case: direct edges
    SELECT x, y FROM edge
    UNION ALL
    -- Recursive case: paths of length n from paths of length n-1
    SELECT tc.x, edge.y
    FROM tc
    JOIN edge ON tc.y = edge.x
)
SELECT * FROM tc;

-- Validation query
SELECT COUNT(*) AS reachable_pairs FROM tc_result;
```

**Key placeholders:**

- `{input_file}` → Path to TSV edge file
- `{output_file}` → Where to write results
- `?param_name` → Query parameters at runtime

### Cypher Rules (Neo4j)

A Cypher rule file is a script of statements separated by `;`, run by `Neo4jConnector` in this
order: all statements but the last two are setup (each timed with `consume()`), the second-to-last
is the timed query, and the last one exports the result (both fetched completely inside their
timed calls, because `session.run()` is lazy; see [SYSTEMS.md](SYSTEMS.md#neo4j)). `{data_file}` and
`{output_file}` are replaced by the input file name (copied into Neo4j's import directory) and the
export file name. The shipped `systems/neo4j/rules/transitive_left_recursion.cypher`:

```cypher
MATCH (n) DETACH DELETE n;

LOAD CSV FROM "file:///{data_file}" AS line FIELDTERMINATOR '\t'
MERGE (a:Node {id: toInteger(line[0])})
MERGE (b:Node {id: toInteger(trim(line[1]))})
CREATE (a)-[:EDGE]->(b);

CREATE INDEX IF NOT EXISTS FOR (n:Node) ON (n.id);

MATCH (start:Node)-[:EDGE*1..]->(end:Node)
WITH DISTINCT start.id AS x, end.id AS y
RETURN count(*) AS pairs;

CALL apoc.export.csv.query(
    "MATCH (start:Node)-[:EDGE*1..]->(end:Node) RETURN DISTINCT start.id AS x, end.id AS y",
    "{output_file}",
    {}
)
YIELD file, nodes, relationships, properties, time, rows, batchSize, batches, done, data
RETURN file, rows;
```

### Datalog Rules (Clingo, XSB, Soufflé)

Create `systems/clingo/rules/transitive_right_recursion.lp`:

```prolog
% Right recursion: forward chaining
% rule_id: transitive_right_recursion
% domain: transitive_closure

tc(X, Y) :- edge(X, Y).
tc(X, Z) :- tc(X, Y), edge(Y, Z).

#show tc/2.
```

### DistAlgo Rules (Alda)

Create `systems/alda/rules/transitive_right_recursion.da`:

```distalgo
# Right recursion with DistAlgo
# rule_id: transitive_right_recursion
# domain: transitive_closure

def compute_transitive_closure():
    tc = set(edge)
    changed = True
    while changed:
        changed = False
        for (x, y) in tc:
            for (z, w) in tc:
                if y == z and (x, w) not in tc:
                    tc.add((x, w))
                    changed = True
    return tc
```

---

## Validation & Testing

### Before running experiments, validate your rules

```sh
# Check descriptor + rule file existence for one system
python transitive.py --validate-rules my_new_db

# Check that all systems have rules for every mode in a domain
python transitive.py --validate-domain shortest_path

# Check domain against specific systems only
python transitive.py --validate-domain shortest_path --systems postgres clingo
```

These checks verify:

- ✅ Rule files exist for all declared modes
- ✅ YAML syntax is valid
- ✅ Required fields present in descriptors
- ✅ Credentials can load (for systems that require them)

### Test a single rule file

```sh
# Static syntax check only
python transitive.py --test-rule systems/postgres/rules/transitive_right_recursion.sql

# Static check + live dry-run (EXPLAIN) against the connected system
python transitive.py --test-rule systems/postgres/rules/transitive_right_recursion.sql \
  --system postgres
```

The tester checks:
- Balanced parentheses/brackets
- Language-specific rules (SELECT in SQL, :- in Datalog, MATCH in Cypher, etc.)
- For PostgreSQL/DuckDB: live `EXPLAIN` parse without executing any writes

### Run a quick smoke test

```sh
python transitive.py --systems my_new_db --graphs cycle --sizes 10 11 1 --num-runs 1
```

---

## Protocol Reference

When choosing a **protocol** in your descriptor, use one of these **built-in connectors**:

| Protocol          | Systems                    | Credentials Required             | Extension       |
| ----------------- | -------------------------- | -------------------------------- | --------------- |
| `psycopg2`           | PostgreSQL                 | `dbURL: postgres://...`                     | `.py`     |
| `cockroachdb`        | CockroachDB                | `dbURL`, `externalDirectory`                | `.py`     |
| `mysqlclient`        | MariaDB                    | `host, port, user, password, database`      | `.py`     |
| `singlestore`        | SingleStore                | `host, port, user, password, database`      | `.py`     |
| `duckdb`             | DuckDB                     | None (in-process)                           | `.sql`    |
| `neo4j`              | Neo4j                      | `uri, user, password, import_directory`     | `.cypher` |
| `pymongo`            | MongoDB                    | `uri, database`                             | `.py`     |
| `subprocess`         | XSB                        | None (`xsb` on `PATH`)                      | `.P`      |
| `souffle_subprocess` | Soufflé                    | None (`souffle` on `PATH`)                  | `.dl`     |
| `clingo_python`      | Clingo                     | None (Python binding)                       | `.lp`     |
| `alda_subprocess`    | Alda (DistAlgo)            | None                                        | `.da`     |

The `.py` rule files of the SQL systems define a class `<class_prefix><Mode>Recursion` (e.g.
`MariaDBLeftRecursion` in `rules/transitive_left_recursion.py`) that extends the operations class
of the system's `__init__.py`, which the rule imports as `<module_prefix>` (e.g. `mariadb_rules`);
both prefixes are `flags` in `descriptor.yaml`. Every server system has a
`credentials.example.yaml`; installation and pitfalls are in [SYSTEMS.md](SYSTEMS.md).

### Adding a new protocol without editing engine code

If none of the built-in protocols fit your system, you can add your own by dropping a single file into the system folder — **no edits to engine source required**:

```python
# systems/my_new_system/connector.py
from engine.connectors.base import BaseConnector

class MySystemConnector(BaseConnector):
    def connect(self, credentials, descriptor):
        import my_driver
        self._conn = my_driver.connect(**credentials)

    def run_experiment(self, rule_path, input_path, output_folder, descriptor, config, query_bindings=None):
        phases = descriptor.timing_phases
        measurements = [(0.0, 0.0)] * len(phases)
        results_path = self.result_path(output_folder, descriptor, 'my_system_results.csv')
        try:
            ...  # measurements[i] = self.timed(step_i, ...)[:2]
        except Exception as e:
            self._record_error(f'MySystem experiment error: {e}')
        return self.build_timing_row(phases, measurements)

    @classmethod
    def cancel_running(cls, credentials, descriptor):
        ...  # stop the statement the server is still running (called by benchmark.py after a timeout)

    def close(self):
        if self._conn:
            self._conn.close()
```

Then set `protocol: my_system` in `systems/my_new_system/descriptor.yaml`. The engine auto-discovers `connector.py` at startup and registers it. This is a one-time addition per driver family — no further source edits needed.

---

## Making a System Ready for Verified Runs

`transitive.py` only needs the steps above. To measure a system with `benchmark.py` (time limit,
one process per trial, correctness check; see [VERIFICATION.md](VERIFICATION.md)), also make sure
of the following. Each point comes from a problem found in the 2026 campaign
([SYSTEMS.md](SYSTEMS.md)).

1. **Declare `query_phase` and `result_file`** in `descriptor.yaml`. `query_phase` is the `id` of
   the timing phase that is the recursive query itself; `result_file` is the name of the file the
   connector writes the result to (use `self.result_path(output_folder, descriptor, default)`).
   The result must contain one pair per line, two integers separated by a comma, tab or space,
   optionally with a header line (quotes are ignored).
2. **Report failures.** Catch exceptions only to clean up, and record them with
   `self._record_error(...)`. `run_one` then exits with code 1 and the run is recorded as
   `error`. A timing row of zeros must never look like a successful run.
3. **Time complete work.** A timed call must return only when the step has been fully executed.
   Lazy drivers (Neo4j's `session.run()`) return early and run the statement later; fetch the
   results inside the timed call. A driver that streams rows must be drained.
4. **Start clean.** A trial killed at the time limit leaves tables, files or server-side jobs
   behind. Drop tables and remove output files at the *start* of every trial (see the MariaDB,
   DuckDB and CockroachDB connectors).
5. **Implement `cancel_running()`** for client/server systems: killing the client does not stop
   the query in the server, and the next trial would then run on a loaded machine or fail. Check
   that it really stops the work (CockroachDB, for example, needs its schema-change jobs cancelled
   one by one).
6. **Declare only modes that are real formulations.** If a system has one fixed formulation
   (MongoDB, Neo4j), run only one mode in `scripts/run_all.sh`; a mode the server rejects
   (double recursion in PostgreSQL) is still worth declaring, because the rejection is recorded.
7. **Check it.** Run a few configurations and confirm `"correct": true` in `runs.jsonl`, and a
   timeout and error path (`--timeout 0.01`, wrong credentials):

   ```sh
   python benchmark.py --systems my_system --graphs cycle path --sizes 100 200 --runs 2 --out /tmp/check/my_system
   ```

   Add a mocked connector test to `tests/` (see `tests/test_verified_pipeline.py`). To include
   the system in a campaign, add a phase to `scripts/run_all.sh`. `analyze_verified.py` puts every
   series into `summary.csv` and `verification.json`; its figures and LaTeX tables list the
   paper's systems explicitly (the `series` lists in `plot_graph` and the `table_*` functions), so
   add the system there to plot it. Give the system its own entry in `engine/plot_style.py`
   (`SYSTEM_STYLE`): a label, a colour and a marker that no other system uses, since the marker is
   what tells systems apart in black-and-white print. `tests/test_plot_style.py` checks that
   markers are unique and that every figure uses them and orders its legends by the curves'
   last points. The LaTeX version of each figure follows automatically:
   `engine/figures_tex.py` transcribes whatever the matplotlib code draws (lines and markers,
   log or linear axes, text, legends). It refuses artists it cannot transcribe, such as bars,
   instead of dropping them.

---

## Directory Structure Cheat Sheet

```
systems/
├── my_new_system/              ← New system directory
│   ├── descriptor.yaml         ← Required: System metadata
│   ├── credentials.yaml        ← Required: Connection secrets (gitignored)
│   ├── rules/                  ← Required: Query files
│   │   ├── domain_mode.sql     ← Rule format depends on protocol
│   │   ├── domain_mode.cypher
│   │   └── domain_mode.lp
│   └── tests/                  ← Optional: Unit tests
│       └── test_rules.py
│
domains/
├── my_domain/                  ← New domain directory
│   ├── descriptor.yaml         ← Required: Domain metadata
│   ├── templates/              ← Optional: Rule templates
│   │   ├── sql_template.jinja2
│   │   ├── cypher_template.jinja2
│   │   └── datalog_template.jinja2
│   └── README.md               ← Optional: Domain-specific guidance
│
graph_types/
├── my_graph.yaml               ← New graph type descriptor
```

---

## Troubleshooting

### "Protocol not found: my_protocol"

**Problem**: Your `descriptor.yaml` references a protocol that doesn't exist.

**Solution**: Check the [Protocol Reference](#protocol-reference) table. If you need a genuinely new protocol (e.g., a custom language), contact the maintainers.

### "Rule file not found: transitive_right_recursion.sql"

**Problem**: You forgot to create a rule file, or the filename doesn't match the mode.

**Solution**:

1. Check `descriptor.yaml` for the `modes:` list
2. For each mode, create `rules/{domain}_{mode}{rule_extension}`
3. Example: if `modes: [right_recursion]` and `rule_extension: .sql`, create `rules/transitive_right_recursion.sql`

### Credentials won't save in Web UI

**Problem**: Web UI shows an error when saving credentials.

**Solution**:

1. Check that `credentials.yaml` is gitignored: `echo "*/credentials.yaml" >> .gitignore`
2. Ensure `systems/my_system/` directory exists
3. Restart the Flask app: `python transitive.py --ui`

### My custom graph doesn't generate

**Problem**: Bootstrap or test commands fail with graph not found.

**Solution**:

1. Ensure `graph_types/my_graph.yaml` exists and has correct path
2. Generator path format: `engine.data_generator.DataGenerator.my_method_name`
3. Test Python syntax: `python -c "from engine.data_generator import DataGenerator; d = DataGenerator(); d.generate_my_graph(10)"`

---

## Next Steps

- **Configure your first system**: Use the [Quick Start](README.md#quick-start) to launch the Web UI
- **Explore templates**: Check `systems/` for examples of each protocol
- **Join discussions**: Open an issue or discussion for help with your domain/system

---

**You've got this!** Trans-Bench is designed so that domain experts and data engineers can extend it without becoming Python developers. If you find something unclear, open an issue — we'll improve it.
