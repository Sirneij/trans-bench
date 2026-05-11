# Trans-Bench Extension Cookbook

> Real-world examples and step-by-step guides for extending Trans-Bench.

---

## Table of Contents

1. [Recipe 1: Add a new SQL database system](#recipe-1-add-a-new-sql-database-system)
2. [Recipe 2: Add a shortest-path query domain](#recipe-2-add-a-shortest-path-query-domain)
3. [Recipe 3: Add a custom graph topology](#recipe-3-add-a-custom-graph-topology)
4. [Recipe 4: Benchmark a new logic engine](#recipe-4-benchmark-a-new-logic-engine)
5. [Recipe 5: Multi-domain benchmarking](#recipe-5-multi-domain-benchmarking)

---

## Recipe 1: Add a New SQL Database System

**Goal**: Add SQLite to Trans-Bench for transitive closure benchmarking.

### Step 1: Create the system directory

```bash
# Option A: Using bootstrap CLI
python transitive.py --bootstrap-system sqlite --bootstrap-system-template descriptor_sql_database.yaml

# Option B: Using Web UI
# Navigate to http://127.0.0.1:5000/systems/new
# Click "Create System", name it "sqlite", choose "SQL Database" template
```

### Step 2: Edit the descriptor

Edit `systems/sqlite/descriptor.yaml`:

```yaml
name: sqlite
display_name: SQLite
category: db
protocol:
  duckdb # SQLite uses duckdb connector in trans-bench
  # (or can use sqlite3 connector if added)

timing_phases:
  - id: load_data
    label: LoadData
  - id: execute_query
    label: ExecuteQuery
  - id: write_result
    label: WriteResult

input_format: tsv
modes:
  - right_recursion
  - left_recursion
  - double_recursion

rule_extension: .sql

flags:
  requires_credentials: true
```

### Step 3: Create credentials

Create `systems/sqlite/credentials.yaml`:

```yaml
# SQLite uses a file path as the connection string
database: ./benchmark.db
```

### Step 4: Add rule files

Create SQL rule files in `systems/sqlite/rules/`:

```bash
# Copy templates
cp templates/rule_template_sql_right_recursion.sql \
   systems/sqlite/rules/transitive_right_recursion.sql
cp templates/rule_template_sql_left_recursion.sql \
   systems/sqlite/rules/transitive_left_recursion.sql
```

Edit `systems/sqlite/rules/transitive_right_recursion.sql`:

```sql
-- SQLite recursive CTE for transitive closure
WITH RECURSIVE tc AS (
    SELECT x, y FROM edge
    UNION ALL
    SELECT tc.x, edge.y
    FROM tc
    JOIN edge ON tc.y = edge.x
)
SELECT x, y FROM tc;
```

### Step 5: Validate and test

```bash
# Validate the system
python transitive.py --validate-rules sqlite

# Quick test with small graphs
python transitive.py --systems sqlite --graphs cycle path --sizes 10 11 1 --num-runs 1
```

### Step 6: Run full benchmark

```bash
# Compare SQLite against other systems
python transitive.py --systems sqlite postgres duckdb --graphs cycle path --sizes 100 1001 100
```

---

## Recipe 2: Add a Shortest-Path Query Domain

**Goal**: Benchmark shortest-path computation across all systems.

### Step 1: Create the domain

```bash
# Using bootstrap CLI
python transitive.py --bootstrap-domain shortest_path --bootstrap-domain-template domain_shortest_path.yaml

# Or use Web UI at http://127.0.0.1:5000/domains/new
```

### Step 2: Verify domain descriptor

Check `domains/shortest_path/descriptor.yaml` — should define:

- Query parameters (source_node, max_distance)
- Output schema (source, target, distance)
- Supported modes (iterative_deepening, dijkstra_style)

### Step 3: Add rule files for each system

**For PostgreSQL** (`systems/postgres/rules/shortest_path_iterative_deepening.sql`):

```sql
CREATE TEMP TABLE sp_result AS
WITH RECURSIVE sp AS (
    SELECT x, y, weight AS dist
    FROM weighted_edge
    WHERE x = ?source_node

    UNION ALL

    SELECT sp.x, edge.y, sp.dist + edge.weight
    FROM sp
    JOIN weighted_edge edge ON sp.y = edge.x
    WHERE sp.dist + edge.weight < ?max_distance
)
SELECT x, y, MIN(dist) AS distance
FROM sp
GROUP BY x, y;
```

**For Clingo** (`systems/clingo/rules/shortest_path_iterative_deepening.lp`):

```prolog
sp(X, Y, W) :- edge(X, Y, W), X = source(?source_node).
sp(X, Z, D) :- sp(X, Y, D1), edge(Y, Z, W), D = D1 + W, D < ?max_distance.

% Keep only shortest paths
shortest(X, Y, D) :- sp(X, Y, D), not shorter(X, Y, D).
shorter(X, Y, D) :- shortest(X, Y, D1), sp(X, Y, D2), D2 < D1.

#show shortest/3.
```

**For Neo4j** (`systems/neo4j/rules/shortest_path_dijkstra_style.cypher`):

```cypher
MATCH (start {id: ?source_node})
CALL apoc.algo.dijkstra(start, null, 'EDGE', 'weight')
YIELD index, node, cost
RETURN node.id as target, cost as distance
WHERE cost < ?max_distance
ORDER BY cost;
```

### Step 4: Create test data

The engine will generate weighted edges automatically if you specify edge weights in the data generator. Otherwise, add weight data:

```bash
# In input/postgres/shortest_path_cycle_100.tsv
1	2	1.5
2	3	2.0
3	4	0.5
...
```

### Step 5: Run shortest-path experiments

```bash
# Benchmark shortest-path on cycle and path graphs
python transitive.py --domains shortest_path \
  --systems postgres clingo neo4j \
  --graphs cycle path \
  --sizes 100 1001 100 \
  --num-runs 5
```

### Step 6: View results

Results will be organized by domain:

```
timing/
└── shortest_path/
    ├── postgres/
    │   ├── cycle/
    │   │   ├── iterative_deepening_graph_100.csv
    │   │   └── dijkstra_style_graph_100.csv
    │   └── path/
    ├── clingo/
    └── neo4j/
```

---

## Recipe 3: Add a Custom Graph Topology

**Goal**: Add a "weighted random DAG" graph type that generates random directed acyclic graphs with weighted edges.

### Step 1: Create the graph descriptor

```bash
python transitive.py --bootstrap-graph weighted_random_dag \
  --bootstrap-graph-generator engine.data_generator.DataGenerator.generate_weighted_random_dag \
  --bootstrap-graph-description "Random directed acyclic graph with edge weights"
```

### Step 2: Implement the generator

Edit `engine/data_generator.py` and add:

```python
def generate_weighted_random_dag(self, n: int) -> Generator[tuple[int, int, float], None, None]:
    """
    Generate a random DAG with n nodes and weighted edges.

    Ensures acyclicity by using topological ordering: edge (i, j) only if i < j.
    """
    import random
    random.seed(42)  # For reproducibility

    # Determine edge probability based on graph size
    edge_prob = 2.0 / n  # Results in ~2 edges per node on average

    for i in range(n):
        for j in range(i + 1, n):
            if random.random() < edge_prob:
                weight = random.uniform(1.0, 10.0)
                yield (i, j, round(weight, 2))
```

### Step 3: Test the generator

```bash
# Quick test
python transitive.py --graphs weighted_random_dag --systems postgres --sizes 50 51 1 --num-runs 1
```

### Step 4: Use in experiments

```bash
# Benchmark shortest-path on the new graph
python transitive.py --domains shortest_path \
  --graphs weighted_random_dag cycle \
  --systems postgres clingo \
  --sizes 100 1001 100
```

---

## Recipe 4: Benchmark a New Logic Engine

**Goal**: Add Prolog (XSB) as a new benchmarked system.

> Assume XSB is already installed at `/usr/local/bin/xsb`

### Step 1: Create the system

```bash
python transitive.py --bootstrap-system xsb --bootstrap-system-template descriptor_logic_engine.yaml
```

### Step 2: Configure the descriptor

Edit `systems/xsb/descriptor.yaml`:

```yaml
name: xsb
display_name: XSB Prolog
category: logic
protocol: subprocess

timing_phases:
  - id: load_facts
    label: LoadFacts
  - id: execute_query
    label: ExecuteQuery
  - id: write_result
    label: WriteResult

input_format: lp # Datalog/Prolog format
modes:
  - right_recursion
  - left_recursion
  - double_recursion

rule_extension: .pl # Prolog files

flags:
  requires_credentials: false

execution:
  executable: /usr/local/bin/xsb
  timeout_seconds: 300
  # XSB reads from stdin, writes to stdout
```

### Step 3: Create Prolog rules

Create `systems/xsb/rules/transitive_right_recursion.pl`:

```prolog
:- table tc/2.  % Enable tabling for efficient recursion

% Base case: direct edges
tc(X, Y) :- edge(X, Y).

% Recursive case: transitive paths
tc(X, Z) :- tc(X, Y), edge(Y, Z).

% Output: all reachable pairs
?- tc(X, Y), format('~w,~w~n', [X, Y]), fail.
```

### Step 4: Test

```bash
# Validate
python transitive.py --validate-rules xsb

# Quick test
python transitive.py --systems xsb --graphs cycle path --sizes 10 11 1 --num-runs 1

# Compare with other logic engines
python transitive.py --systems xsb clingo souffle --graphs cycle --sizes 100 1001 100
```

---

## Recipe 5: Multi-Domain Benchmarking

**Goal**: Benchmark multiple query types (transitive closure, shortest path, reachability with avoidance) across all systems simultaneously.

### Step 1: Create all domains

```bash
# Create domains if not already present
python transitive.py --bootstrap-domain transitive
python transitive.py --bootstrap-domain shortest_path
python transitive.py --bootstrap-domain reachability_avoid
```

### Step 2: Add rules for each domain to each system

For each system and each domain, add rule files:

```
systems/postgres/rules/
├── transitive_right_recursion.sql
├── transitive_left_recursion.sql
├── shortest_path_iterative.sql
├── shortest_path_dijkstra.sql
├── reachability_avoid_constrained.sql
└── ...
```

### Step 3: Run multi-domain benchmark

```bash
python transitive.py \
  --domains transitive shortest_path reachability_avoid \
  --systems postgres mysql duckdb neo4j clingo souffle \
  --graphs cycle path star complete \
  --sizes 100 1001 100 \
  --num-runs 5
```

### Step 4: Analyze results

Results organized by domain:

```
timing/
├── transitive/
│   ├── postgres/cycle/
│   ├── postgres/path/
│   ├── clingo/cycle/
│   └── ...
├── shortest_path/
│   ├── postgres/cycle/
│   └── ...
└── reachability_avoid/
    └── ...
```

Compare:

```bash
# Generate LaTeX comparison tables
python generate_plot_table.py --domain transitive --systems postgres clingo souffle
python generate_plot_table.py --domain shortest_path --systems postgres clingo souffle
```

---

## Debugging Common Issues

### "Rule file not found"

**Problem**: System validation fails because a required rule file is missing.

**Solution**:

1. Check the system's modes in `descriptor.yaml`
2. For each mode, create a corresponding rule file: `{domain}_{mode}{extension}`
3. Validate: `python transitive.py --validate-rules {system}`

### Query times are suspiciously fast/slow

**Problem**: Results seem wrong.

**Solution**:

1. Check that the rule generates correct output
2. Test manually: `psql -U user -d db -f systems/postgres/rules/transitive_right_recursion.sql`
3. Verify that all phases are being timed (check CSV headers)

### "Connector not found: my_protocol"

**Problem**: A new protocol is needed that doesn't exist.

**Solution**: This requires Python development. See `engine/connectors/base.py` for the interface to implement. Consider opening an issue if this is a common protocol (e.g., Apache Spark, DuckDB WASM).

---

## Next Steps

- **Explore existing systems**: Check `systems/*/` for examples
- **Join discussions**: Ask for help on GitHub discussions
- **Share your extensions**: Submit pull requests to add new systems/domains
- **Read full docs**: See [EXTENSION_GUIDE.md](EXTENSION_GUIDE.md) and [RULES.md](RULES.md)

---

**Happy benchmarking!** With these recipes, you can extend Trans-Bench to support virtually any recursive query system and domain.
