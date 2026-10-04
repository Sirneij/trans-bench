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

Of the 12,215 completed runs, 11,540 were compared with the closure computed in Python. The other
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
analysis/failures.csv        first failed n per series, graph and mode, with the kind and error
analysis/table_memory_*.tex  memory used by the query (MB), in the layout of the time tables
```

The analysis is regenerated with the command below. Only the creation dates inside the PDFs change.

```sh
python analyze_verified.py results/verified_2026_v2 --out results/verified_2026_v2/analysis
```
