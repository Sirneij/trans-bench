# Bootstrap Templates

This folder contains reusable **templates** for quickly extending Trans-Bench. Templates are starting points — copy, customize, and deploy.

## How to use templates

### 1. System descriptors

Choose a descriptor template based on your system type:

```sh
# For SQL-based systems
cp templates/descriptor_sql_database.yaml systems/my_system/descriptor.yaml
edit systems/my_system/descriptor.yaml  # Customize name, protocol, etc.

# For graph databases
cp templates/descriptor_graph_database.yaml systems/my_graph_db/descriptor.yaml

# For logic engines
cp templates/descriptor_logic_engine.yaml systems/my_logic_engine/descriptor.yaml
```

### 2. Query rule templates

Copy and modify rule templates:

```sh
# For SQL right-recursion
cp templates/rule_template_sql_right_recursion.sql systems/my_system/rules/transitive_right_recursion.sql

# For Datalog
cp templates/rule_template_datalog_right_recursion.lp systems/my_system/rules/transitive_right_recursion.lp

# For Neo4j Cypher
cp templates/rule_template_cypher_right_recursion.cypher systems/my_system/rules/transitive_right_recursion.cypher
```

### 3. Domain templates

Create multi-domain benchmarks:

```sh
# Copy a domain descriptor
cp templates/domain_shortest_path.yaml domains/shortest_path/descriptor.yaml

# Create rule files for each system
for system in postgres mysql neo4j; do
  mkdir -p systems/$system/rules/shortest_path
  cp templates/rule_template_sql_right_recursion.sql systems/$system/rules/shortest_path_iterative_deepening.sql
done
```

## Template files reference

### System Descriptors

- `descriptor_sql_database.yaml` — PostgreSQL, MySQL, CockroachDB, DuckDB
- `descriptor_graph_database.yaml` — Neo4j, Memgraph
- `descriptor_logic_engine.yaml` — XSB, Clingo, Soufflé, Alda

### Query Rule Templates

- `rule_template_sql_right_recursion.sql` — Standard SQL recursive CTE (top-down)
- `rule_template_sql_left_recursion.sql` — Backward-chaining SQL pattern
- `rule_template_datalog_right_recursion.lp` — Datalog/Clingo/Soufflé forward chaining
- `rule_template_cypher_right_recursion.cypher` — Neo4j pattern matching

### Domain Templates

- `domain_shortest_path.yaml` — Multi-source shortest path computation
- `domain_reachability_with_avoidance.yaml` — Constrained reachability queries

## Customization guide

### When editing a descriptor

Look for these common changes:

| Field            | Typical Changes                                                                                                      |
| ---------------- | -------------------------------------------------------------------------------------------------------------------- |
| `name`           | Keep as-is (matches directory name)                                                                                  |
| `display_name`   | Change to your system's actual name                                                                                  |
| `protocol`       | Pick from: `psycopg2`, `mysqlclient`, `duckdb`, `neo4j`, `pymongo`, `subprocess`, `clingo_python`, `alda_subprocess` |
| `timing_phases`  | Usually pre-configured; add/remove based on your system's workflow                                                   |
| `rule_extension` | `.sql` (SQL), `.cypher` (Neo4j), `.lp` (Datalog), `.da` (Alda), `.pl` (Prolog)                                       |
| `modes`          | Keep standard: `right_recursion`, `left_recursion`, `double_recursion`                                               |
| `flags`          | System-specific behavior flags (rarely needs change)                                                                 |

### When editing a rule template

1. **Parameter substitution**: Replace `?source` with actual parameter names used in your system
2. **Table names**: Change `edge`, `tc_result` to match your schema
3. **Data types**: Adjust `INT`, `FLOAT` to match your system's types
4. **Recursion depth**: Adjust limits (e.g., `..100` in Cypher) based on dataset size

## Testing your customizations

```sh
# Validate all descriptors in a system
python transitive.py --validate-rules my_system

# Test a specific rule
python transitive.py --test-rule systems/my_system/rules/transitive_right_recursion.sql

# Run a quick benchmark
python transitive.py --systems my_system --graphs cycle --sizes 10 11 1 --num-runs 1
```

## Getting help

If a template doesn't fit your use case:

1. Check [EXTENSION_GUIDE.md](../EXTENSION_GUIDE.md) for detailed instructions
2. Review existing systems in `systems/` for real-world examples
3. Open an issue with your use case — we'll add templates as needed

---

**Pro tip**: Keep your customizations organized. Name rule files clearly:

```
rules/
├── transitive_right_recursion.sql       ← Domain_Mode pattern
├── transitive_left_recursion.sql
├── shortest_path_dijkstra_style.sql
└── reachability_avoid_constrained.sql
```

This makes it easy to find rules later and helps others understand your benchmarks.
