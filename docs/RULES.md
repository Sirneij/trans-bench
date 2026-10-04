# Rule files

A rule file holds the recursive query of one system for one mode. This document explains how rule
files are named, defines the three recursion modes the suite measures, and then shows the shipped
rule files of every language. All the examples are copied from `systems/*/rules/`, so they are known
to give the correct closure; where a system needs something unusual, the reason is given.

## Naming

A rule file is named `<domain>_<mode><extension>`, for example `transitive_left_recursion.sql`.
The domain of every shipped rule file is `transitive`; the extension is the `rule_extension` of the
system's descriptor. The engine looks first for the full name, then for the first word of the domain
(`transitive_closure` becomes `transitive`), and finally for the mode alone (`left_recursion.sql`).

## The three modes

All three modes compute the same relation, the transitive closure `path` of `edge`. They differ in
where the recursive reference sits in the rule:

| Mode | Rule | What one iteration does |
| --- | --- | --- |
| `left_recursion` | `path(X, Y) :- path(X, Z), edge(Z, Y).` | extends every known path by one edge at its end |
| `right_recursion` | `path(X, Y) :- edge(X, Z), path(Z, Y).` | puts one edge in front of every known path |
| `double_recursion` | `path(X, Y) :- path(X, Z), path(Z, Y).` | joins two known paths, so path lengths can double |

Each mode also has the base rule `path(X, Y) :- edge(X, Y).` Left and right recursion are linear,
since the rule refers to `path` once; double recursion is non-linear. SQL:1999 requires recursive
queries to be linear, and that is why PostgreSQL and CockroachDB reject double recursion. DuckDB has a
fourth mode, `doublerecurring_recursion`, explained under SQL below.

## SQL

The SQL systems express the closure as a recursive common table expression with `UNION`, which
removes duplicates in every iteration. Hence the query reaches a fixed point on cyclic graphs as
well. With `UNION ALL`, the same query would follow the cycles for ever; SingleStore, which accepts
only `UNION ALL`, is the exception discussed below.

### DuckDB (plain SQL script)

DuckDB's rule file is a complete script; each statement is one timing phase, and `{data_file}` and
`{output_file}` are replaced by the connector. Left recursion:

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

Right recursion differs only in the recursive term,
`SELECT edge.x, tc.y FROM edge JOIN tc ON edge.y = tc.x`, and double recursion joins `tc` with itself:
`SELECT tc1.x, tc2.y FROM tc AS tc1, tc AS tc2 WHERE tc1.y = tc2.x`. In DuckDB, however, each
self-reference sees only the rows of the previous iteration, so this double recursion misses pairs on
7 of the 12 graph families. The mode `doublerecurring_recursion` reads both references as
`recurring.tc`, which DuckDB provides since version 1.5, and is correct:

```sql
SELECT tc1.x, tc2.y FROM recurring.tc AS tc1, recurring.tc AS tc2 WHERE tc1.y = tc2.x
```

### PostgreSQL, MariaDB, CockroachDB and SingleStore (Python classes)

These systems run the steps of a trial from Python. The shared `systems/<name>/__init__.py` defines
the operations (creating, loading, indexing and analyzing the edge table, exporting the result,
dropping the tables), and each rule file adds the recursive query as `run_recursive_query()` in a
class named `<class_prefix><Mode>Recursion`. MariaDB's double recursion:

```python
from mariadb_rules import MariaDBOperations


class MariaDBDoubleRecursion(MariaDBOperations):
    def run_recursive_query(self) -> None:
        """Run the double recursion query for transitive closure."""
        self.execute_query(
            """
        CREATE TABLE tc_result AS
        WITH RECURSIVE tc AS (
            SELECT x, y FROM edge
            UNION
            SELECT tc1.x, tc2.y FROM tc AS tc1, tc AS tc2 WHERE tc1.y = tc2.x
        )
        SELECT * FROM tc;
        """
        )
```

The connector calls the operations in a fixed order, one timing phase each:
`create_tc_path_table`, the import, `create_tc_path_index`, `analyze_tc_path_table`,
`run_recursive_query` and the export. MariaDB first sets `standard_compliant_cte=0`, without which it
rejects double recursion, and SingleStore first raises its iteration limit.

SingleStore rejects `UNION` and `DISTINCT` inside the recursive term. Its rule files therefore
combine the terms with `UNION ALL` and remove duplicates once, at the end:

```sql
CREATE TABLE tc_result AS
WITH RECURSIVE tc AS (
    SELECT x, y FROM edge
    UNION ALL
    SELECT tc.x, edge.y FROM tc JOIN edge ON tc.y = edge.x
)
SELECT DISTINCT x, y FROM tc;
```

This enumerates every path, so it terminates only on acyclic graphs, and even there it runs out of
memory when the paths are many (max_acyclic, larger grids). [SYSTEMS.md](SYSTEMS.md) gives the
details.

## Prolog and Datalog

### XSB (`.P`)

XSB evaluates the rules top-down with tabling, which `:- auto_table.` turns on for every predicate.
Without tabling, left recursion would loop for ever. Left recursion:

```prolog
:- auto_table.
path(X, Y) :- edge(X, Y).
path(X, Y) :- path(X, Z), edge(Z, Y).
```

The right and double rule files change only the second rule, to `edge(X, Z), path(Z, Y)` and
`path(X, Z), path(Z, Y)`. The facts and the query are supplied by `xsb_export/extfilequery.P`, so
the rule file holds nothing else.

### Clingo (`.lp`)

Clingo grounds the program and computes its single answer set. The rules are those of XSB, followed
by a directive that selects the output:

```prolog
path(X, Y) :- edge(X, Y).
path(X, Y) :- path(X, Z), edge(Z, Y).

#show path/2.
```

### Souffle (`.dl`)

Souffle compiles the program to C++ and evaluates it bottom-up. Its relations are declared with
their types, and `.input` and `.output` name the files to read and write:

```prolog
.decl edge(x:number, y:number)
.input edge

.decl path(x:number, y:number)
path(x,y) :- edge(x,y).
path(x,y) :- path(x,z), edge(z,y).

.output path
```

### ALDA (`.da`)

ALDA, the DistAlgo language with rules, states the same two rules inside a process class; the
right-recursion rule set reads:

```python
path(x, y), if_(edge(x, y))
path(x, y), if_(edge(x, z), path(z, y))
```

The rule set is evaluated with `infer(rules=..., bindings=[('edge', E)], queries=['path'])`. The full
files in `systems/alda/rules/` are adapted from the benchmarks of Liu et al.
(https://github.com/DistAlgo/alda).

## Cypher (Neo4j)

A Cypher rule file is a script of statements separated by `;`. All statements but the last two
prepare the graph, the second to last is the timed query, and the last one exports the result
(`systems/neo4j/rules/transitive_left_recursion.cypher`):

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

The variable-length pattern `[:EDGE*1..]` has one formulation, so the three mode files are identical,
and only `left_recursion` is measured. The timed query returns only the number of distinct pairs,
because the export writes the pairs themselves.

## MongoDB

MongoDB's rule files are Python classes, like those of the SQL systems, and the closure is one
aggregation pipeline:

```python
self.db[input_collection].aggregate(
    [
        {'$graphLookup': {'from': input_collection, 'startWith': '$x', 'connectFromField': 'y',
                          'connectToField': 'x', 'as': 'paths', 'restrictSearchWithMatch': {}}},
        {'$unwind': '$paths'},
        {'$project': {'_id': 0, 'x': '$x', 'y': '$paths.y'}},
        {'$group': {'_id': {'x': '$x', 'y': '$y'}, 'x': {'$first': '$x'}, 'y': {'$first': '$y'}}},
        {'$project': {'_id': 0, 'x': 1, 'y': 1}},
        {'$out': output_collection},
    ],
    allowDiskUse=True,
)
```

`$graphLookup` performs one fixed search per document, so, as with Neo4j, the three mode files hold
the same pipeline.

## Placeholders

| Placeholder | Replaced by | Used by |
| --- | --- | --- |
| `{data_file}` | the input file (for Neo4j, its name in the import directory) | DuckDB, Neo4j |
| `{output_file}` | the result file | DuckDB, Neo4j |
| `?<name>` | the value of `<name>` in the input's `queries_<n>.csv` (header `X`) | DuckDB, Neo4j |

The server databases receive the same bindings in `config['query_bindings']`, and the logic systems
get their input and output paths from their connectors.

## When a rule gives a wrong result

The campaign engine checks every result against the closure computed in Python, so a wrong rule shows
up as `"correct": false` in `runs.jsonl`. The usual causes are these:

1. `UNION ALL` where `UNION` is meant, which repeats pairs on graphs with several paths between two
   nodes and never ends on a cycle;
2. a recursive term that joins the wrong columns, which computes some other relation;
3. an engine-specific limit that ends the recursion early without an error, as MariaDB's temporary
   tables and DuckDB's double recursion did in 2026 ([SYSTEMS.md](SYSTEMS.md)).

A single result can be checked by hand with
`python -m engine.verify input/souffle/<graph>/<n>/edge.facts <result file>`, and a rule file's syntax
with `python transitive.py --test-rule <file>`.
