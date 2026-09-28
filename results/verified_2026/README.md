# Verified campaign, September 2026

These are the measurements of *Database System Performance on Recursive Queries* (Section 4):
12,279 runs with 5 runs per configuration and a 600 s limit per run, every completed run checked
against an independently computed transitive closure. The machine was an Apple M3 Pro (11
cores, 18 GB) with macOS 27.0. The exact versions are in `versions.txt` and `pip_freeze.txt`.
How to verify or reproduce the campaign: [docs/REPRODUCING.md](../../docs/REPRODUCING.md). The
record format: [docs/VERIFICATION.md](../../docs/VERIFICATION.md).

| Series | Records | ok | timeout | error | skipped (larger n after a failure) |
| --- | ---: | ---: | ---: | ---: | ---: |
| cockroachdb | 1450 | 1315 | 2 | 12 | 121 |
| duckdb | 2582 | 2580 | 0 | 2 | 0 |
| mariadb | 1802 | 1755 | 12 | 0 | 35 |
| mariadb_tuned (4 GB temporary tables) | 1200 | 1200 | 0 | 0 | 0 |
| mongodb | 598 | 550 | 6 | 0 | 42 |
| neo4j | 667 | 660 | 0 | 1 | 6 |
| postgres | 1478 | 1350 | 2 | 12 | 114 |
| singlestore | 1054 | 820 | 0 | 24 | 210 |
| xsb | 1978 | 1975 | 1 | 0 | 2 |
| **total** | 12809 | 12205 | 23 | 51 | 530 |

Completed runs with an incorrect result:
* DuckDB's plain double recursion, on 7 graph families at every size (the `recurring` variant is
  correct);
* MariaDB scale_free/right/20000 and barabasi_albert/left/100000, in its default configuration.

These are listed in `analysis/verification.json` and `analysis/summary.csv`.

## Layout

```
<series>/runs.jsonl          one record per run (command, times, timing row, result count+hash,
                             expected count+hash, correctness, errors)
<series>/logs.tar.gz         logs/<system>_<graph>_<mode>_<n>_run<i>.log: full output of every run
<series>/timing.tar.gz       the harness's timing CSVs (every row is also in runs.jsonl)
cockroachdb/runs_invalid_cleanup_bug.jsonl + README_invalid.txt
                             invalid runs (driver cleanup bug), re-run afterwards
invalid/                     invalid phases, kept for transparency (README.txt):
  neo4j_consume_timing/      Neo4j timed with consume() (query not computed)
  singlestore_server_crash/  SingleStore leaf killed by the VM's OOM killer
mariadb_investigation.jsonl  MariaDB missing pairs vs settings (scripts/run_investigation.sh)
run_all.log.gz               driver output of the whole campaign (phase start/end times)
versions.txt, pip_freeze.txt machine and software versions
analysis/                    output of analyze_verified.py: summary.csv, verification.json,
                             table_*.tex (rows of the paper's tables), figures/*.pdf (matplotlib,
                             the files in the paper), figures_tex/*.tex + .pdf (the same figures
                             as standalone pgfplots/TikZ documents)
harness/                     the 2026 harness changes as patches (see harness/README.md)
```

The records were written by the 2026 harness. Their `command` shows `analyze_dbs.py` or
`analyze_logic_systems.py` with the configuration JSON (which contained the credentials) replaced
by `'<config JSON>'`. The timing CSVs inside `timing.tar.gz` are named
`timing/<system>/<graph>/timing_<mode>_graph_<n>.csv`. `analyze_verified.py` reads this layout as
well as the current one.

Regenerate `analysis/`. Everything is identical except for the creation dates embedded in the PDFs:

```sh
python analyze_verified.py results/verified_2026 --out results/verified_2026/analysis
```
