# Cookbook

Five recipes, each a complete task from the first command to the result. The first two need no
database server, so they are the quickest way to see the suite at work; the others add a system or a
graph family, and check a measurement against the published campaign. The output quoted in the last
recipe comes from the machine of the 2026 campaigns.

## 1. A first campaign on DuckDB

DuckDB runs inside the harness, so this campaign needs nothing beyond `pip install -r
requirements.txt`. From the command line:

```sh
python transitive.py --systems duckdb --graphs cycle path grid --modes left_recursion right_recursion \
    --sizes 100 501 100 --num-runs 5 --campaign results/first_run
```

The same campaign can be started from the web interface (`python transitive.py --ui`): on New
experiment, choose DuckDB and the three topologies, name the campaign `first_run`, and press Start.
The live monitor then shows every configuration as a tile that changes colour when it finishes.

Either way, the runs are recorded in `results/first_run/duckdb/runs.jsonl`, and the analysis is
written to `results/first_run/analysis/` when the campaign ends. In the web interface, the campaign
appears under Campaigns, and every run's phases are under Results explorer. A campaign stopped half
way (Stop, or Ctrl-C) is resumed by the same command; the configurations already recorded are not
run again.

## 2. A logic engine next to DuckDB

Clingo is installed with the Python requirements, so it can be compared with DuckDB on the same
machine without any server:

```sh
python benchmark.py --systems clingo duckdb --graphs path cycle grid \
    --modes left_recursion right_recursion double_recursion --sizes 10 50 100 --runs 3 \
    --campaign results/clingo_vs_duckdb
```

Grounding precedes solving, so Clingo's LoadRules, LoadFacts, Ground, Query and
Write phases are timed separately, and `summary.csv` reports the Query phase. DuckDB's double
recursion is incomplete on all three of these graphs, as [SYSTEMS.md](SYSTEMS.md) explains, and the
records show it: those runs have `"correct": false`, and `summary.csv` reports `all_correct` as
false for them. XSB and
Souffle can be added in the same way once they are installed (`xsb` on `PATH`; `souffle` and `g++`
on `PATH`).

## 3. Another PostgreSQL-compatible server

Say a second PostgreSQL server, version 18, runs on port 5433 and should be measured beside the
version 17 of the campaigns. A copy of the PostgreSQL system directory is all it needs:

```sh
cp -r systems/postgres systems/postgres18
```

In `systems/postgres18/descriptor.yaml`, only `name: postgres18` and
`display_name: PostgreSQL 18` change. The flags `class_prefix: PostgreSQL` and
`module_prefix: postgres_rules` stay, because the copied rule files still define
`PostgreSQLLeftRecursion` and its siblings. The credentials point to the new server:

```yaml
# systems/postgres18/credentials.yaml
dbURL: postgresql://<user>@localhost:5433/benchmarkdb
```

```sh
python transitive.py --validate-rules postgres18
python benchmark.py --systems postgres postgres18 --graphs cycle path --sizes 100 200 300 --campaign results/pg17_vs_pg18
```

Two cautions apply. After a timeout, the connector ends every other session on the benchmark
database, so that database should serve the benchmark alone. Also, a new system appears in the
paper-style figures only after it is given a style in `engine/plot_style.py` and added to the lists
in `analyze_verified.py`; `summary.csv`, the campaign page and the results explorer show it without
that.

## 4. A new graph family

A ladder is two paths of n/2 nodes each, joined by a rung at every step. Its generator goes into the
`DataGenerator` class of `generate_db.py`:

```python
def generate_ladder_graph(self, n: int) -> Generator[tuple[int, int], None, None]:
    """Generate a ladder: two rails of n // 2 nodes, joined by a rung at every step."""
    m = max(1, n // 2)
    for i in range(1, m):
        yield (i, i + 1)          # upper rail
        yield (m + i, m + i + 1)  # lower rail
    for i in range(1, m + 1):
        yield (i, m + i)          # rung
```

For n = 6 it yields (1, 2), (4, 5), (2, 3), (5, 6), (1, 4), (2, 5) and (3, 6). The descriptor names
the method:

```yaml
# graph_types/ladder.yaml
name: ladder
display_name: Ladder
description: Two paths of n/2 nodes, joined by a rung at every step.
generator: engine.data_generator.DataGenerator.generate_ladder_graph
parameters: {}
```

The family is then available everywhere: on the Topologies page, in the wizard, and on the command
line, where the engine generates its inputs before the first run that needs them:

```sh
python benchmark.py --systems duckdb clingo --graphs ladder --sizes 100 200 400 --campaign results/ladder
```

## 5. Checking a measurement against the published campaign

A small campaign with the paper's settings (five runs, the paper's sizes) can be compared with the
published one. Results must agree exactly, whatever the machine; times may differ.

```sh
python benchmark.py --systems duckdb --graphs cycle path --modes left_recursion --sizes 100 200 \
    --runs 5 --campaign results/check
python scripts/compare_results.py results/check
```

On the machine of the 2026 campaigns, the output was:

```
4 configurations in common
results: 0 runs differ from the reference
status: 0 configurations differ (new vs reference)
median query time, new / reference (geometric mean over configurations):
  duckdb            0.97  (4 configurations)
```

A run whose result differs is listed with both counts and hashes. A status that differs, such as a
timeout here where the reference completed, is listed as well, but it is not counted as a failure,
because a slower machine can cross the time limit where a faster one did not.
