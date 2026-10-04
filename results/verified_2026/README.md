# Verified campaign, September 2026

This is the first campaign in which every run of trans-bench was timed in its own process and its
result checked against an independently computed closure. It covers the same systems, topologies,
modes and sizes as the paper, with five runs per configuration and a 600 s limit per run. The
paper's figures come from verified_2026_v2, a complete re-run of this campaign that also records
memory; the two agree on every result.

## The campaign

The runs took place between 26 September 2026, 13:42 UTC, and 28 September, 05:16 UTC, on an Apple
M3 Pro with 11 cores and 18 GB of memory under macOS 27.0. The versions of the systems are in
`versions.txt` and `pip_freeze.txt`. There were 12,279 runs, and the sizes above a failure were
recorded as skipped.

| Series | Records | Completed | Timeout | Error | Skipped |
| --- | ---: | ---: | ---: | ---: | ---: |
| cockroachdb | 1,450 | 1,315 | 2 | 12 | 121 |
| duckdb | 2,582 | 2,580 | 0 | 2 | 0 |
| mariadb | 1,802 | 1,755 | 12 | 0 | 35 |
| mariadb_tuned (4 GB temporary tables) | 1,200 | 1,200 | 0 | 0 | 0 |
| mongodb | 598 | 550 | 6 | 0 | 42 |
| neo4j | 667 | 660 | 0 | 1 | 6 |
| postgres | 1,478 | 1,350 | 2 | 12 | 114 |
| singlestore | 1,054 | 820 | 0 | 24 | 210 |
| xsb | 1,978 | 1,975 | 1 | 0 | 2 |
| total | 12,809 | 12,205 | 23 | 51 | 530 |

The harness of that time recorded every failure other than a timeout as an error. In
verified_2026_v2, the same failures are split into out of memory, unsupported and iteration limit.

Two kinds of wrong result were found among the completed runs. DuckDB's plain double recursion
missed pairs on 7 of the 12 regular families at every size, while its variant over the `recurring`
table was correct. MariaDB, in its default configuration, returned too few pairs with right
recursion on the scale-free graph of 20,000 nodes and with left recursion on the Barabási-Albert
graph of 100,000 nodes; `mariadb_investigation.jsonl` holds the investigation of this case.
`analysis/verification.json` lists both, and [docs/VERIFICATION.md](../../docs/VERIFICATION.md)
explains the checks.

## Runs that were not used

Three groups of runs were found to be invalid while the campaign was running. They were kept for
transparency, and the affected configurations were run again.

- CockroachDB runs that followed a timeout failed with `table "tc_result" is being added`, because
  the cancelled `CREATE TABLE ... AS` had left a schema-change job behind. They are in
  `cockroachdb/runs_invalid_cleanup_bug.jsonl`, and `cockroachdb/README_invalid.txt` gives the
  details and the fixes.
- Neo4j runs from 27 September, 22:18 UTC, were timed with `consume()`, which discards the stream
  without computing it. Their results were checked, but their times are not used
  (`invalid/neo4j_consume_timing/`).
- SingleStore runs between 23:27 and 23:44 UTC on 27 September met a server that had crashed on its
  first query (`invalid/singlestore_server_crash/`).

## Files

```
<series>/runs.jsonl          one record per run: command, times, timing row, result and expected
                             count and hash, correctness, errors
<series>/logs.tar.gz         logs/<system>_<graph>_<mode>_<n>_run<i>.log, the output of every run
<series>/timing.tar.gz       the timing CSVs of the harness (every row is also in runs.jsonl)
invalid/                     the invalid runs described above, with README.txt
mariadb_investigation.jsonl  pairs missing from MariaDB's result under several settings
run_all.log.gz               output of the driver for the whole campaign, with phase start and end
versions.txt, pip_freeze.txt versions of the machine and the software
analysis/                    output of analyze_verified.py: summary.csv, verification.json,
                             table_*.tex, figures/*.pdf (matplotlib) and figures_tex/ (the same
                             figures as standalone pgfplots/TikZ documents)
harness/                     the changes made to the harness for this campaign, as patches
```

The records were written by the 2026 harness. Their `command` therefore names `analyze_dbs.py` or
`analyze_logic_systems.py`, with the configuration JSON, which held the credentials, replaced by
`'<config JSON>'`. Inside `timing.tar.gz`, the files are named
`timing/<system>/<graph>/timing_<mode>_graph_<n>.csv`. `analyze_verified.py` reads this layout as
well as the present one, and the analysis is regenerated with the command below; only the creation
dates inside the PDFs change.

```sh
python analyze_verified.py results/verified_2026 --out results/verified_2026/analysis
```

The way to rebuild the harness of this campaign is given in [harness/README.md](harness/README.md).
