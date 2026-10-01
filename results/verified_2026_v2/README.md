# Verified campaign with memory measurements, September 2026 (the paper's data)

These are the measurements of *Database System Performance on Recursive Queries* (Section 4). They
come from a complete re-run of the campaign in `../verified_2026/` with the current harness
(`benchmark.py` → `engine/run_one.py` → `engine/connectors/`). Three things differ from that campaign:

* **Memory.** Each run's query memory is recorded: the peak minus the value just before the query,
  as measured by `engine/memory.py`.
* **Explicit failures.** Each failed run records why it failed (`failure`: timeout, oom,
  unsupported, iteration_limit, killed, error). The tables mark a size skipped after a failure
  with that failure's kind (TO$^s$, OOM$^s$, …).
* **Time limit.** Every run stores its time limit (`timeout_s`, 600 s).

The campaign has 12,289 runs, 5 per configuration with a 600 s limit per run, each checked against
an independently computed transitive closure. It ran on 2026-09-29/30 (38.6 hours) on an Apple M3
Pro (11 cores, 18 GB) with macOS 27.0. Versions are in `versions.txt` and `pip_freeze.txt`, and
the harness is the commit in `versions.txt` plus `code.patch`. For the record format and the
memory probes, see [docs/VERIFICATION.md](../../docs/VERIFICATION.md); for reproduction, see
[docs/REPRODUCING.md](../../docs/REPRODUCING.md).

| Series | Records | ok | timeout | oom | unsupported | iteration limit | skipped |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| cockroachdb | 1450 | 1315 | 2 | 0 | 12 | 0 | 121 |
| duckdb | 2582 | 2580 | 0 | 2 | 0 | 0 | 0 |
| mariadb | 1802 | 1755 | 12 | 0 | 0 | 0 | 35 |
| mariadb_tuned (4 GB temporary tables) | 1200 | 1200 | 0 | 0 | 0 | 0 | 0 |
| mongodb | 606 | 560 | 6 | 0 | 0 | 0 | 40 |
| neo4j | 667 | 660 | 0 | 1 | 0 | 0 | 6 |
| postgres | 1478 | 1350 | 2 | 0 | 12 | 0 | 114 |
| singlestore | 1054 | 820 | 0 | 10 | 12 | 2 | 210 |
| xsb | 1978 | 1975 | 1 | 0 | 0 | 0 | 2 |
| **total** | 12817 | 12215 | 23 | 13 | 36 | 2 | 528 |

## Agreement with `../verified_2026`

`scripts/compare_results.py results/verified_2026_v2 --reference results/verified_2026`:

* **Results:** every completed run returned the same result (count and hash) in all 3,045
  configurations the two campaigns share.
* **Times:** median query times per system are within 8%.
* **Status:** 8 configurations changed status, all within a few seconds of the 600 s limit:
  * MongoDB, Barabási-Albert, 30,000 nodes: now completes (567 s), then times out at 40,000;
  * PostgreSQL, scale-free, left recursion: completes 50,000, times out at 60,000;
  * PostgreSQL, scale-free, right recursion: times out at 60,000 instead of 70,000.

## Layout

The layout is that of [`../verified_2026`](../verified_2026/README.md), plus:

```
code.patch                   uncommitted changes of the harness at the time of the runs
analysis/failures.csv        first failed n per series, graph and mode, with the kind and error
analysis/table_memory_*.tex  memory used by the query (MB), same layout as the time tables
```

Regenerate `analysis/`:

```sh
python analyze_verified.py results/verified_2026_v2 --out results/verified_2026_v2/analysis
```
