# Verified campaign with memory measurements, September 2026

This campaign holds the measurements of the paper Database System Performance on Recursive Queries
(Section 4). It is a complete re-run of the first 2026 campaign (verified_2026) with the present
harness, in which every run is a separate process with a 600 s limit and every result is checked.
Unlike the first campaign, it also records the memory each query used and the kind of every failure.

## The campaign

The campaign ran from 29 September 2026, 02:26 UTC, to 30 September, 17:00 UTC (38.6 hours), on an
Apple M3 Pro with 11 cores and 18 GB of memory under macOS 27.0. Each configuration (system,
topology, mode and size) was run five times, which gave 12,289 runs; the 528 runs of the sizes above
a failure were recorded as skipped. The versions of the systems are in `versions.txt` and
`pip_freeze.txt`, and the harness is the commit named in `versions.txt` with `code.patch` applied.

| Series | Records | Completed | Timeout | Out of memory | Unsupported | Iteration limit | Skipped |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| cockroachdb | 1,450 | 1,315 | 2 | 0 | 12 | 0 | 121 |
| duckdb | 2,582 | 2,580 | 0 | 2 | 0 | 0 | 0 |
| mariadb | 1,802 | 1,755 | 12 | 0 | 0 | 0 | 35 |
| mariadb_tuned (4 GB temporary tables) | 1,200 | 1,200 | 0 | 0 | 0 | 0 | 0 |
| mongodb | 606 | 560 | 6 | 0 | 0 | 0 | 40 |
| neo4j | 667 | 660 | 0 | 1 | 0 | 0 | 6 |
| postgres | 1,478 | 1,350 | 2 | 0 | 12 | 0 | 114 |
| singlestore | 1,054 | 820 | 0 | 10 | 12 | 2 | 210 |
| xsb | 1,978 | 1,975 | 1 | 0 | 0 | 0 | 2 |
| total | 12,817 | 12,215 | 23 | 13 | 36 | 2 | 528 |

Compared with the first campaign, three fields were added to the record of each run. The query's
memory is the peak during the query minus the value just before it, as `engine/memory.py` measures
it. A failed run carries its kind in `failure` (timeout, oom, unsupported, iteration_limit, killed
or error), and the tables mark a size skipped after a failure with the kind of that failure.
Finally, every run stores its time limit in `timeout_s`.

## Correctness

Of the 12,215 completed runs of September, 11,540 were compared with the closure computed in Python. The other
675 are the scale-free and Barabási-Albert graphs above 20,000 nodes, where that computation is too
expensive; there, the systems were compared with one another. Two kinds of wrong result were found:

1. DuckDB's plain double recursion, on 7 of the 12 regular families at every size (the variant over
   the `recurring` table is correct);
2. MariaDB in its default configuration, with right recursion on the scale-free graph of 20,000
   nodes and with left recursion on the Barabási-Albert graph of 100,000 nodes, where it returned
   7,768,407 pairs, one short of the 7,768,408 that the other systems agreed on.

The second case is examined in `mariadb_investigation.jsonl`, written by
`scripts/run_investigation.sh`. With 4 GB temporary tables (`mariadb_tuned`), the results were
correct. [docs/VERIFICATION.md](../../docs/VERIFICATION.md) describes the record format and the
checks, and [docs/SYSTEMS.md](../../docs/SYSTEMS.md) the MariaDB case.

## DuckDB with one thread

The mean times of DuckDB on the sparse graphs jump from one size to the next, because single runs
take 10 to 50 times as long as the others. Garbage collection is disabled in every run, so it is not
the cause. In trials, the slow runs disappeared when DuckDB was limited to one thread, and the
series `duckdb_1thread` was added to the campaign for comparison. It was run on the same machine on
10 and 11 October 2026 (23:43 to 01:34 UTC, 1.8 hours), with left and right recursion on all 14
graph families, five runs per configuration, and `threads: 1` set through
`duckdb_1thread/duckdb_settings.yaml`. The commands are in `duckdb_1thread/run.sh`, and the log of
every run records the setting that DuckDB reported.

Of its 1,382 runs, 1,380 completed with the correct closure, or on the large random graphs with the
closure the other systems agreed on. The two failed runs ran out of memory on the scale-free graph of
90,000 nodes, at the same size as with the default setting. On the 12 regular families, 70 of the 240
configurations of the default series had a run more than three times as long as the median of its
five runs; with one thread, none had. The median coefficient of variation fell from 0.061 to 0.022,
and the medians were 11% lower on average (geometric mean). One thread was slower where parallel work
pays: 11.2 s against 9.8 s on the complete graph of 1,000 nodes with left recursion, about 20% on
the Barabási-Albert and scale-free graphs, and 259 s against 128 s on the scale-free graph of 80,000
nodes with right recursion.

The series appears in the figures as DuckDB (1 thread). In the tables of the paper, DuckDB
stays in its default configuration, and the one-thread series is not shown there.

## Agreement with the first campaign

The two campaigns were compared with

```sh
python scripts/compare_results.py results/verified_2026_v2 --reference results/verified_2026
```

They share 3,045 configurations, and every completed run returned the same count and hash in both.
Per system, the geometric mean of the ratios of median query times lies between 0.92 (CockroachDB)
and 1.07 (MariaDB and PostgreSQL). Eight configurations changed status, all at the edge of the time
limit:

- MongoDB on the Barabási-Albert graph of 30,000 nodes now completed in about 570 s with either
  mode, where it had exceeded 600 s; it then timed out at 40,000 nodes;
- PostgreSQL on the scale-free graphs with left recursion now completed 50,000 nodes (about 390 s)
  and timed out at 60,000, one size later than before;
- with right recursion, PostgreSQL timed out at 60,000 nodes, where it had completed in about 550 s
  in the first campaign.

## Files

The layout is that of [verified_2026](../verified_2026/README.md), with these additions:

```
code.patch                   uncommitted changes of the harness at the time of the runs
duckdb_1thread/              the series of DuckDB with one thread, with its settings and commands
analysis/failures.csv        first failed n per series, graph and mode, with the kind and error
analysis/table_memory_*.tex  memory used by the query (MB), in the layout of the time tables
```

The analysis is regenerated with the command below. Only the creation dates inside the PDFs change.

```sh
python analyze_verified.py results/verified_2026_v2 --out results/verified_2026_v2/analysis
```
