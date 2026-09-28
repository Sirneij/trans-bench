# Verification: how every run is checked, and what is recorded

## Correctness check (`engine/verify.py`)

A result, the set of pairs (x, y) that a system exported, is summarized as

* **count**: the number of output lines (pairs), and
* **hash**: the sum modulo 2^64 of `splitmix64((x << 32) | y)` over all output lines.

The sum does not depend on the order of the lines. A missing, duplicated or wrong pair changes the
count or the hash, and two different results with the same count collide with a probability of
about 2^-64. `summarize_file()` reads a result file in one streaming pass, in the format of any
system: CSV with or without a header, quoted CSV (APOC), TSV, or XSB's `x y` text. It skips a first
line that contains letters.

The **expected** summary is computed from the same edge file by a breadth-first search in plain
Python (`expected_summary()`), independently of every system under test, and cached in
`input/expected_closures.json` (keyed by input path). The inputs themselves are pinned by
`input/SHA256SUMS`.

For the scale-free and Barabási-Albert graphs with more than 20,000 nodes, the Python computation
is too expensive (`benchmark.py --expected-max-n`, default 20000). There, `analyze_verified.py`
accepts a result if it equals the result on which all other systems (at least two) agree. Such
checks are marked `checked_by = agreement` in `summary.csv`, and the agreement per graph is listed
under `scale_free_ba_cross_system_agreement` in `verification.json`.

Check any single result by hand:

```sh
python -m engine.verify input/souffle/cycle/100/edge.facts path/to/duckdb_results.csv
# {"expected": {"count": 10000, "hash": "e0a288f2f6749da9"}, "got": {...}, "correct": true}
```

## Run protocol (`benchmark.py`)

For each (system, graph, mode), sizes are run in increasing order, `--runs` times each (5 in the
campaign):

1. Delete a leftover result file.
2. Start `python -m engine.run_one …` as a new process in its own session (process group). Its
   stdout and stderr go to `logs/<system>_<graph>_<mode>_<n>_run<i>.log`.
3. Wait at most `--timeout` seconds (600 in the campaign). The limit covers the whole trial:
   connect, create, load, index, query, export.
   * If the run exceeds the limit, the whole process group is killed with SIGKILL, and
     `connector.cancel_running()` stops the statement still running in the server (see
     [SYSTEMS.md](SYSTEMS.md)). The driver then waits 5 s, and the run's status is `timeout`.
   * Otherwise the run's status is `error` if `run_one` reported errors, exited with a non-zero
     code, or appended no timing row. If not, its status is `ok`.
4. For an `ok` run, summarize the result file, compare it with the expected summary, and delete the
   file (unless `--keep-results` is given).
5. After a `timeout` or `error`, the remaining runs of this n are not started, and every larger n
   of the same (system, graph, mode) is recorded once as `skipped`.

Records are appended as the runs finish. Restarting the driver with the same `--out` skips every
(system, graph, mode, n) that already has a record.

## `runs.jsonl` schema

One JSON object per line.

| Field | Meaning |
| --- | --- |
| `system`, `graph`, `mode`, `n` | the configuration |
| `label` | free text from `--label` (e.g. `tmp_table_size=4G` for `mariadb_tuned`) |
| `run` | 1…runs; `null` for `skipped` records |
| `status` | `ok`, `error`, `timeout`, or `skipped` |
| `start`, `end` | UTC timestamps (ISO 8601) |
| `command` | the exact command of the run (no credentials: `run_one` reads them from files) |
| `exit_code` | exit code of the run (absent for `timeout`) |
| `wall_s` | elapsed time of the whole process, in seconds, measured by the driver |
| `timing_row` | the row that this run appended to its timing CSV (all phases: `<Label>RealTime`, `<Label>CPUTime`, and `<Label>MaxRAM_MB` in the current harness), or `null` |
| `result` | `{"count", "hash"}` of the exported result (`ok` and `error` runs), or `null` if there was no result file |
| `expected` | `{"count", "hash"}` of the independently computed closure, or `null` above `--expected-max-n` |
| `correct` | `true`/`false` if `expected` is known, `null` if not; meaningful only for status `ok` |
| `errors` | error messages of the run (connector errors, or ERROR lines of the log) |

The records in `results/verified_2026` were written by the original driver (`run_benchmark.py` in
`results/verified_2026/harness/`). Their `command` field shows `analyze_dbs.py` or
`analyze_logic_systems.py` with the configuration JSON replaced by `'<config JSON>'`, and their
timing rows have no `MaxRAM_MB` columns. Otherwise the schema is the same, and
`analyze_verified.py` reads both.

## Reported values (`analyze_verified.py`)

* **Time.** The real time of the system's `query_phase` (descriptor). The mean is reported only if
  all 5 runs completed. The median, standard deviation, minimum and maximum are in `summary.csv`.
* **CPU time.** Shown only for XSB and DuckDB, whose query runs inside the measured process.
* **Table cells.**

  | Cell | Meaning |
  | --- | --- |
  | `TO` | the first run exceeded the limit |
  | `TO^s` | not run, because a smaller n exceeded the limit or failed |
  | `OOM` | the run failed because memory ran out |
  | `n/s` | the query is not supported (e.g. double recursion) |
  | `IL` | the run hit an iteration limit |
  | `ERR` | any other error |
  | `†` | a completed but incorrect result |

  In the figures, a hollow marker at 600 s marks the first size that failed. Every figure is
  written as a matplotlib PDF (`figures/`) and as a standalone pgfplots/TikZ document
  (`figures_tex/`), which transcribes the matplotlib figure (`engine/figures_tex.py`).
* **`verification.json`** contains:
  * the counts of runs by status;
  * the configurations with incorrect results, as `<series>/<mode>: [graphs]`;
  * the cross-system agreement on the large graphs.

`tests/test_verified_pipeline.py::TestAnalysis` checks that re-analyzing
`results/verified_2026` reproduces the published tables, `summary.csv`, `verification.json` and the
28 LaTeX figure sources byte for byte.
