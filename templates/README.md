# Templates

This directory holds the starting points for a new system, query domain or rule file. A template
only saves typing: the quickest way to add a server that speaks a protocol the suite already
supports is still to copy a shipped system, as [docs/EXTENSION_GUIDE.md](../docs/EXTENSION_GUIDE.md)
explains. `python transitive.py --list-templates` lists the files below.

## System descriptors

| Template | Starting point for | Protocol in the template |
| --- | --- | --- |
| `descriptor_sql_database.yaml` | a relational server whose rule files are Python classes | `psycopg2` |
| `descriptor_graph_database.yaml` | a graph or document database | `neo4j` |
| `descriptor_logic_engine.yaml` | a logic system run as a separate program | `subprocess` |

A new system directory is created from one of them with

```sh
python transitive.py --bootstrap-system my_db --bootstrap-system-template descriptor_sql_database.yaml
```

which writes `systems/my_db/descriptor.yaml`, with `name` set to `my_db`, and an empty `rules/`
directory. The Register a system page of the web interface does the same. The descriptor is written
back through PyYAML, so the comments of the template are not copied; they remain here for
reference. The fields that usually change are these:

- `display_name`, the name shown in tables, figures and the interface;
- `protocol`, which selects the connector, among those registered in
  `engine/connectors/__init__.py`;
- `timing_phases` and `query_phase`, the phases the connector times and the one reported as the
  query time;
- `result_file`, the file the connector writes the result to, which the engine checks after every
  run;
- `rule_extension`, and for the relational servers `class_prefix` and `module_prefix` under `flags`.

## Rule files

The rule templates compute the transitive closure correctly; each was checked against a closure
computed in Python on a graph with cycles. A rule file of a system is named
`<domain>_<mode><extension>`, so a template must be renamed when it is copied:

```sh
cp templates/rule_template_sql_left_recursion.sql systems/my_db/rules/transitive_left_recursion.sql
```

| Template | Content |
| --- | --- |
| `rule_template_sql_left_recursion.sql` | the recursive common table expression with left recursion |
| `rule_template_sql_right_recursion.sql` | the same with right recursion |
| `rule_template_datalog_right_recursion.lp` | the two rules of right recursion for Clingo, with `#show path/2` |
| `rule_template_cypher_right_recursion.cypher` | the complete Neo4j script: clear, load, index, timed query and export |

The SQL templates hold only the statement that builds `tc_result`. For DuckDB, that statement goes
into a script like those in `systems/duckdb/rules/`, where every statement is one timing phase. On
the servers that use Python classes, it becomes the body of `run_recursive_query()`, as in
`systems/postgres/rules/`. Both SQL templates use `UNION`. With `UNION ALL`, the query would repeat
pairs on graphs with several paths between two nodes and would never end on a cycle.
[docs/RULES.md](../docs/RULES.md) shows the shipped rule file of every language and explains the
three modes.

## Query domains

`domain_shortest_path.yaml` and `domain_reachability_with_avoidance.yaml` describe two recursive
queries other than the transitive closure: their parameters, output columns, modes and data needs,
with an example rule in SQL (and in Datalog for the shortest path). A domain is created from one of
them with

```sh
python transitive.py --bootstrap-domain shortest_path --bootstrap-domain-template domain_shortest_path.yaml
```

No system ships rule files for these domains yet, and for the shortest path the engine does not yet
find the weighted input that `generate_db.py` writes. [docs/EXTENSION_GUIDE.md](../docs/EXTENSION_GUIDE.md) describes
what remains to be done for such a domain.

## Checking a new system

```sh
python transitive.py --validate-rules my_db
python transitive.py --test-rule systems/my_db/rules/transitive_left_recursion.sql
python benchmark.py --systems my_db --graphs cycle path --sizes 10 20 --runs 1 --campaign results/my_db_check
```

The first command checks the descriptor and that every declared mode has a rule file, the second
checks the syntax of one rule file, and the third runs a small campaign in which every result is
compared with the closure computed in Python.
