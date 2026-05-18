# Query Rules Reference & Cookbook

> Practical examples and patterns for writing query rules for different systems and domains.

---

## Table of Contents

1. [Rule File Naming Convention](#rule-file-naming-convention)
2. [SQL Rules (PostgreSQL, MySQL, DuckDB, CockroachDB)](#sql-rules)
3. [Datalog Rules (Clingo, XSB, Soufflé)](#datalog-rules)
4. [Cypher Rules (Neo4j)](#cypher-rules)
5. [JavaScript Rules (MongoDB)](#javascript-rules)
6. [Prolog Rules (XSB)](#prolog-rules)
7. [DistAlgo Rules (Alda)](#distalgo-rules)
8. [Parameter Substitution](#parameter-substitution)
9. [Cookbook: Domain Patterns](#cookbook-domain-patterns)

---

## Rule File Naming Convention

All rule files follow this naming pattern:

```
{domain}_{recursion_mode}{extension}
```

### Examples:

```
rules/
├── transitive_right_recursion.sql          # SQL: transitive closure, right recursion
├── transitive_left_recursion.sql
├── transitive_double_recursion.sql
├── shortest_path_dijkstra_style.lp         # Datalog: shortest path, Dijkstra-style
├── reachability_avoid_constrained.cypher   # Cypher: constrained reachability
└── same_generation_level_sync.pl           # Prolog: peer relation
```

### Components:

| Component | Examples                                                                                         | Notes                               |
| --------- | ------------------------------------------------------------------------------------------------ | ----------------------------------- |
| Domain    | `transitive`, `shortest_path`, `reachability_avoid`, `same_generation`                           | Must match a domain descriptor name |
| Mode      | `right_recursion`, `left_recursion`, `double_recursion`, `iterative_deepening`, `dijkstra_style` | Defines the recursion strategy      |
| Extension | `.sql`, `.lp`, `.cypher`, `.js`, `.pl`, `.da`                                                    | Determines protocol                 |

---

## SQL Rules

SQL rules use **Common Table Expressions (CTEs)** with `WITH RECURSIVE` for transitive queries.

### Pattern 1: Right Recursion (Forward Chaining)

Build the closure by extending found paths.

```sql
-- systems/postgres/rules/transitive_right_recursion.sql
CREATE TEMP TABLE tc_result AS
WITH RECURSIVE tc AS (
    -- Base: all direct edges
    SELECT x, y FROM edge

    UNION ALL

    -- Recursive: extend paths
    SELECT tc.x, edge.y
    FROM tc
    JOIN edge ON tc.y = edge.x
)
SELECT x, y FROM tc;

-- Output for verification
SELECT COUNT(*) FROM tc_result;
```

### Pattern 2: Left Recursion (Backward Chaining)

Find all predecessors.

```sql
CREATE TEMP TABLE tc_result AS
WITH RECURSIVE tc AS (
    SELECT x, y FROM edge
    UNION ALL
    SELECT edge.x, tc.y
    FROM edge
    JOIN tc ON edge.y = tc.x
)
SELECT x, y FROM tc;
```

### Pattern 3: Double Recursion (Bidirectional)

Search from both endpoints simultaneously (more complex, harder to optimize).

```sql
CREATE TEMP TABLE tc_result AS
WITH RECURSIVE
forward AS (
    SELECT x, y FROM edge
    UNION ALL
    SELECT forward.x, edge.y
    FROM forward
    JOIN edge ON forward.y = edge.x
),
backward AS (
    SELECT x, y FROM edge
    UNION ALL
    SELECT edge.x, backward.y
    FROM edge
    JOIN backward ON edge.y = backward.x
)
SELECT DISTINCT f.x, b.y
FROM forward f
JOIN backward b ON f.y = b.x;
```

### Pattern 4: Iterative Deepening (Breadth-First)

Process paths layer by layer.

```sql
CREATE TEMP TABLE tc_result AS
WITH RECURSIVE layers AS (
    -- Layer 0: direct edges
    SELECT x, y, 1 AS depth FROM edge

    UNION ALL

    -- Layer k+1: paths of depth k+1
    SELECT layers.x, edge.y, layers.depth + 1
    FROM layers
    JOIN edge ON layers.y = edge.x
    WHERE layers.depth < 100  -- Limit recursion depth
)
SELECT DISTINCT x, y FROM layers;
```

### Pattern 5: Shortest Path (with weights)

```sql
CREATE TEMP TABLE tc_result AS
WITH RECURSIVE sp AS (
    SELECT x, y, weight AS dist, ARRAY[x, y] AS path
    FROM edge

    UNION ALL

    SELECT sp.x, edge.y, sp.dist + edge.weight, sp.path || edge.y
    FROM sp
    JOIN edge ON sp.y = edge.x
    WHERE sp.dist + edge.weight < 1000000  -- Avoid infinite loops
        AND NOT (edge.y = ANY(sp.path))     -- Prevent cycles
)
SELECT DISTINCT x, y, MIN(dist) AS min_dist
FROM sp
GROUP BY x, y;
```

### Common Optimizations

**Add a CYCLE detection clause** (PostgreSQL 13+):

```sql
WITH RECURSIVE tc AS (
    SELECT x, y FROM edge
    UNION ALL
    SELECT tc.x, edge.y
    FROM tc
    JOIN edge ON tc.y = edge.x
    WHERE NOT CYCLE  -- Built-in cycle detection
)
SELECT * FROM tc;
```

**Use LATERAL joins** (faster in some systems):

```sql
WITH RECURSIVE tc AS (
    SELECT x, y FROM edge
    UNION ALL
    SELECT tc.x, new_edges.y
    FROM tc,
    LATERAL (
        SELECT y FROM edge WHERE x = tc.y
    ) new_edges
)
SELECT * FROM tc;
```

---

## Datalog Rules

Datalog is a logic programming language used by Clingo, XSB, and Soufflé.

### Pattern 1: Right Recursion

Forward chaining (standard).

```prolog
% systems/clingo/rules/transitive_right_recursion.lp
tc(X, Y) :- edge(X, Y).
tc(X, Z) :- tc(X, Y), edge(Y, Z).

#show tc/2.
```

### Pattern 2: Left Recursion

Backward chaining (less common in Datalog).

```prolog
% Alternative: explicit backward chaining
tc(X, Y) :- edge(X, Y).
tc(X, Y) :- edge(X, Z), tc(Z, Y).

#show tc/2.
```

### Pattern 3: Double Recursion

```prolog
% Transitive closure via forward and backward rules
forward(X, Y) :- edge(X, Y).
forward(X, Z) :- forward(X, Y), edge(Y, Z).

backward(X, Y) :- edge(X, Y).
backward(X, Y) :- edge(X, Z), backward(Z, Y).

tc(X, Y) :- forward(X, Y).
tc(X, Y) :- backward(X, Y).

#show tc/2.
```

### Pattern 4: Shortest Path (with arithmetic)

```prolog
% systems/clingo/rules/shortest_path_iterative.lp
sp(X, Y, D) :- edge(X, Y, W), D = W.
sp(X, Z, D1 + D2) :- sp(X, Y, D1), edge(Y, Z, D2), D1 + D2 < max_dist.

% Remove suboptimal paths
shortest(X, Y, D) :- sp(X, Y, D), not shorter(X, Y, D).
shorter(X, Y, D) :- shortest(X, Y, D1), sp(X, Y, D2), D2 < D1.

#show shortest/3.
```

### Pattern 5: Reachability with Avoidance

```prolog
% Reachable nodes avoiding forbidden set
reachable(X, Y) :- edge(X, Y), not forbidden(Y).
reachable(X, Z) :- reachable(X, Y), edge(Y, Z), not forbidden(Z).

#show reachable/2.
```

### Common Features

**Use aggregates for statistics:**

```prolog
% Count reachable destinations per source
count_reach(X, N) :- N = #count { Y : tc(X, Y) }.

#show count_reach/2.
```

**Use constraints to filter:**

```prolog
% Only paths of length ≤ 5
tc(X, Y, 1) :- edge(X, Y).
tc(X, Z, L+1) :- tc(X, Y, L), edge(Y, Z), L < 5.

#show tc/3.
```

---

## Cypher Rules

Cypher is the query language for Neo4j.

### Pattern 1: Right Recursion (Variable-Length Paths)

```cypher
-- systems/neo4j/rules/transitive_right_recursion.cypher
MATCH (start)-[:EDGE*..100]->(end)
WHERE start <> end
RETURN start, end
ORDER BY start.id, end.id;
```

### Pattern 2: Left Recursion (Reversed Paths)

```cypher
MATCH (start)<-[:EDGE*..100]-(end)
RETURN start, end
ORDER BY start.id, end.id;
```

### Pattern 3: Double Recursion (Bidirectional)

```cypher
MATCH (start)-[:EDGE*..50]->(middle)-[:EDGE*..50]->(end)
RETURN DISTINCT start, end
ORDER BY start.id, end.id;
```

### Pattern 4: Shortest Path (with APOC)

Requires APOC library to be installed:

```cypher
CALL apoc.algo.allShortestPaths('MATCH (n)-[r:EDGE]->(m) RETURN n, r, m', {});
```

Or without APOC (simpler but slower):

```cypher
MATCH path = (start)-[:EDGE*..100]->(end)
WHERE start <> end
RETURN start, end, length(path) AS distance
ORDER BY distance ASC, start.id, end.id;
```

### Pattern 5: Reachability with Node Filtering

```cypher
-- Paths avoiding a set of "blocked" nodes
MATCH (start)-[:EDGE*..100]->(end)
WHERE start <> end
  AND NOT any(node IN nodes(path) WHERE node.blocked = true)
RETURN start, end
ORDER BY start.id, end.id;
```

### Common Features

**Limit path length:**

```cypher
MATCH (start)-[:EDGE*1..5]->(end)  -- 1 to 5 hops
RETURN start, end;
```

**Use WHERE for filtering:**

```cypher
MATCH (start)-[:EDGE*..100]->(end)
WHERE toInteger(start.id) < toInteger(end.id)  -- Avoid duplicates
RETURN start, end;
```

---

## JavaScript Rules

JavaScript rules are used by MongoDB for aggregation pipelines.

### Pattern 1: Map-Reduce (Right Recursion)

```javascript
// systems/mongodb/rules/transitive_right_recursion.js
db.edges.mapReduce(
  function () {
    emit(this.x, [this.y]);
  },
  function (key, values) {
    var result = [];
    values.forEach(function (v) {
      result = result.concat(v);
    });
    return result;
  },
  {
    out: "tc_result",
    finalize: function (key, value) {
      return { source: key, targets: value };
    },
  },
);
```

### Pattern 2: Aggregation Pipeline (Recommended)

```javascript
db.edges.aggregate([
  {
    $group: {
      _id: "$x",
      reachable: { $push: "$y" },
    },
  },
  {
    $out: "tc_result",
  },
]);
```

---

## Prolog Rules

Prolog rules (for XSB Prolog engine).

### Pattern 1: Basic Facts and Rules

```prolog
% systems/xsb/rules/transitive_right_recursion.pl
:- table(tc/2).  % Tabling for left recursion

tc(X, Y) :- edge(X, Y).
tc(X, Z) :- tc(X, Y), edge(Y, Z).

?- tc(X, Y), write(X), write(' -> '), write(Y), nl, fail.
```

### Pattern 2: With Negation (Complex)

```prolog
:- table(reachable/2).

% Paths avoiding forbidden nodes
reachable(X, Y) :- edge(X, Y), \+ forbidden(Y).
reachable(X, Z) :- reachable(X, Y), edge(Y, Z), \+ forbidden(Z).

forbidden(node_5).
forbidden(node_10).

?- reachable(X, Y), write_result(X, Y), fail.
```

---

## DistAlgo Rules

DistAlgo rules (for Alda engine).

### Pattern 1: Iterative Fixed-Point Computation

```distalgo
# systems/alda/rules/transitive_right_recursion.da
def compute_transitive_closure():
    # Initialize with direct edges
    tc = set(edge)

    # Iterate until no new pairs found
    changed = True
    while changed:
        changed = False
        new_pairs = set()

        for (x, y) in tc:
            for (z, w) in tc:
                if y == z and (x, w) not in tc:
                    new_pairs.add((x, w))
                    changed = True

        tc.update(new_pairs)

    return tc

def output_results():
    tc = compute_transitive_closure()
    for (x, y) in sorted(tc):
        send(('result', x, y), to=output_handler)
```

---

## Parameter Substitution

Rules can use **placeholders** that the engine substitutes at runtime.

### Supported placeholders:

| Placeholder     | Meaning             | Example                         |
| --------------- | ------------------- | ------------------------------- |
| `?param_name`   | Query parameter     | `WHERE source = ?start_node`    |
| `{input_file}`  | Path to input TSV   | `LOAD DATA FROM '{input_file}'` |
| `{output_file}` | Path to output CSV  | `OUTPUT TO '{output_file}'`     |
| `{domain}`      | Current domain name | `--domain {domain}`             |

### Example with parameters:

```sql
-- SQL with parameter substitution
CREATE TEMP TABLE shortest_paths AS
WITH RECURSIVE sp AS (
    SELECT x, y, weight AS dist
    FROM edge
    WHERE x = ?source_node

    UNION ALL

    SELECT sp.x, edge.y, sp.dist + edge.weight
    FROM sp
    JOIN edge ON sp.y = edge.x
    WHERE sp.dist + edge.weight < ?max_distance
)
SELECT x, y, MIN(dist) FROM sp GROUP BY x, y;
```

The engine will replace:

- `?source_node` with the actual start node (e.g., 5)
- `?max_distance` with the cutoff value (e.g., 1000)

---

## Cookbook: Domain Patterns

Real-world query patterns for common domains.

### Domain 1: Transitive Closure (Basic)

**SQL (Right Recursion):**

```sql
CREATE TEMP TABLE tc_result AS
WITH RECURSIVE tc AS (
    SELECT x, y FROM edge
    UNION ALL
    SELECT tc.x, edge.y FROM tc JOIN edge ON tc.y = edge.x
)
SELECT x, y FROM tc;
```

**Datalog:**

```prolog
tc(X, Y) :- edge(X, Y).
tc(X, Z) :- tc(X, Y), edge(Y, Z).
```

**Cypher:**

```cypher
MATCH (a)-[:EDGE*..100]->(b) RETURN a, b;
```

---

### Domain 2: Shortest Path

**SQL (with weights):**

```sql
WITH RECURSIVE sp AS (
    SELECT x, y, weight AS dist FROM weighted_edge
    UNION ALL
    SELECT sp.x, e.y, sp.dist + e.weight
    FROM sp JOIN weighted_edge e ON sp.y = e.x
    WHERE sp.dist + e.weight < 999999
)
SELECT x, y, MIN(dist) FROM sp GROUP BY x, y;
```

**Datalog (Clingo):**

```prolog
sp(X, Y, W) :- edge(X, Y, W).
sp(X, Z, D) :- sp(X, Y, D1), edge(Y, Z, W), D = D1 + W, D < 999999.
```

---

### Domain 3: Same Generation (Graph Hierarchy)

Find nodes at the same level in a tree or DAG.

**SQL:**

```sql
WITH RECURSIVE depth AS (
    SELECT id, parent_id, 0 AS level FROM nodes WHERE parent_id IS NULL
    UNION ALL
    SELECT n.id, n.parent_id, d.level + 1
    FROM nodes n JOIN depth d ON n.parent_id = d.id
)
SELECT d1.id, d2.id
FROM depth d1
JOIN depth d2 ON d1.level = d2.level AND d1.id < d2.id;
```

**Datalog:**

```prolog
depth(ID, Level) :- root(ID), Level = 0.
depth(ID, Level) :- depth(Parent, Level - 1), child(ID, Parent).

same_gen(X, Y) :- depth(X, L), depth(Y, L), X < Y.
```

---

### Domain 4: Reachability with Avoidance

Find paths that avoid certain "forbidden" nodes.

**SQL:**

```sql
WITH RECURSIVE reach AS (
    SELECT x, y FROM edge WHERE y NOT IN (select forbidden_node from forbidden_nodes)
    UNION ALL
    SELECT reach.x, edge.y FROM reach JOIN edge ON reach.y = edge.x
    WHERE edge.y NOT IN (select forbidden_node from forbidden_nodes)
)
SELECT x, y FROM reach;
```

**Datalog:**

```prolog
reachable(X, Y) :- edge(X, Y), \+ forbidden(Y).
reachable(X, Z) :- reachable(X, Y), edge(Y, Z), \+ forbidden(Z).
```

---

## Debugging Tips

### Rule not producing output?

1. **Check file extension**: Must match `rule_extension` in descriptor
2. **Check file naming**: Must be `{domain}_{mode}{extension}`
3. **Verify syntax**: Run `python transitive.py --validate-rules {system}`
4. **Test with simple data**: Use small test files first

### Query runs but gives wrong result?

1. **Check UNION vs UNION ALL**: UNION removes duplicates
2. **Check recursion termination**: Add depth limits if infinite loop suspected
3. **Verify base case**: Ensure base query returns expected results
4. **Test incrementally**: Run base case alone, then add recursion

### Performance issues?

1. **Add indexes**: `CREATE INDEX ON edge(y)` for recursive joins
2. **Limit recursion depth**: Use `WHERE depth < 100`
3. **Use materialized views** for intermediate results
4. **Consider rewriting**: Some patterns are inherently slower in certain systems

---

## Testing a Rule Locally

Before running the benchmark, test your rule:

```sh
# For SQL systems, use direct client:
psql -U user -d benchmark -f systems/postgres/rules/transitive_right_recursion.sql

# For Datalog/Clingo:
clingo systems/clingo/rules/transitive_right_recursion.lp 0

# For Neo4j (via cypher-shell):
cypher-shell < systems/neo4j/rules/transitive_right_recursion.cypher
```

---

**Next**: See [EXTENSION_GUIDE.md](EXTENSION_GUIDE.md) for full system/domain setup instructions.
