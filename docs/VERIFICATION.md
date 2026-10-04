# Verification: how every run is checked, and what is recorded

This document describes the correctness check first, then the protocol by which each run is
started and recorded, the format of the records, the memory measurement, and finally what the
analysis reports.

## The correctness check (`engine/verify.py`)

A result, the set of pairs (x, y) that a system exported, is summarized by two numbers:

* the count, which is the number of output lines (pairs);
* the hash, which is the sum modulo 2^64 of `splitmix64((x << 32) | y)` over all output lines.

The sum does not depend on the order of the lines. A missing, duplicated or wrong pair changes the
count or the hash, and two different results with the same count collide with a probability of about
2^-64. In other words, the hash is a checksum and not a proof, but a very strong one. It was chosen
because `summarize_file()` can compute it in one streaming pass over a result file of several
gigabytes, in the format of any system: CSV with or without a header, quoted CSV (APOC), TSV, or
XSB's `x y` text. A first line that contains letters is skipped as a header.

The expected summary is computed from the same edge file by a breadth-first search in plain Python
(`expected_summary()`), independently of every system under test, and cached in
`input/expected_closures.json` under the input's path. The inputs themselves are pinned by
`input/SHA256SUMS`.

For the scale-free and Barabási-Albert graphs with more than 20,000 nodes, the Python computation is
too expensive (`--expected-max-n`, default 20000). There, `analyze_verified.py` accepts a result if
it equals the result on which all other systems, at least two of them, agree. Such checks are marked
`checked_by = agreement` in `summary.csv`, and the agreement per graph is listed under
`scale_free_ba_cross_system_agreement` in `verification.json`.

The check applies to the transitive-closure domain with full materialization. In another domain, or
in demand-driven mode, a run's result is not compared with anything, and its record has no `result`
or `correct` field.

Any single result can be checked by hand:

```sh
python -m engine.verify input/souffle/cycle/100/edge.facts path/to/duckdb_results.csv
# {"expected": {"count": 10000, "hash": "e0a288f2f6749da9"}, "got": {...}, "correct": true}
```

## The run protocol (`engine/campaign.py`)

Every campaign follows this protocol, whether it was started from the web interface, from
`transitive.py` or from `benchmark.py`. For each system, graph and mode, the sizes are run in
increasing order, each `--runs` times (five in the published campaigns):

1. A result file left over from an earlier run is deleted.
2. `python -m engine.run_one ...` is started as a new process in its own session, that is, its own
   process group. Its standard output and error go to `logs/<system>_<graph>_<mode>_<n>_run<i>.log`.
3. The engine waits at most `--timeout` seconds (600 in the published campaigns). The limit covers the
   whole trial, from connecting to exporting the result.
   * If the run exceeds the limit, the whole process group is killed with SIGKILL, and
     `connector.cancel_running()` stops the statement still running in the server (see
     [SYSTEMS.md](SYSTEMS.md)). The engine then waits 5 s, so that the server can release what the
     cancelled query held, and the run's status is `timeout`.
   * Otherwise, the run's status is `error` if `run_one` reported errors, exited with a non-zero code
     or appended no timing row. If none of these happened, the status is `ok`.
4. The result file of an `ok` run is summarized, compared with the expected summary and deleted,
   unless `--keep-results` is given.
5. After a `timeout` or an `error`, the remaining runs of this n are not started, and every larger n of
   the same system, graph and mode is recorded once as `skipped`.

Records are appended as the runs finish. When a campaign is started again with the same settings,
every configuration that already has a record is passed over. A campaign can also be stopped: the
Stop button of the live monitor, or Ctrl-C in a terminal, kills the run in progress in the same way
as a timeout. That run is not recorded, hence it is repeated when the campaign is started again.

## The `runs.jsonl` format

Each line is one JSON object.

| Field | Meaning |
| --- | --- |
| `system`, `graph`, `mode`, `n` | the configuration |
| `label` | free text from `--label`, say `tmp_table_size=4G` for `mariadb_tuned` |
| `timeout_s` | the time limit of the run (`--timeout`); absent in the first campaign's records, whose limit was 600 s |
| `run` | 1 to the number of runs; `null` for `skipped` records |
| `status` | `ok`, `error`, `timeout` or `skipped` |
| `start`, `end` | UTC timestamps (ISO 8601) |
| `command` | the exact command of the run, without credentials (`run_one` reads them from files) |
| `exit_code` | the exit code of the run (absent for `timeout`) |
| `wall_s` | elapsed time of the whole process in seconds, measured by the engine |
| `timing_row` | the row this run appended to its timing CSV, with every phase as `<Label>RealTime`, `<Label>CPUTime` and `<Label>MaxRAM_MB`; or `null` |
| `result` | `{"count", "hash"}` of the exported result (`ok` and `error` runs), or `null` when there was no result file |
| `expected` | `{"count", "hash"}` of the closure computed independently, or `null` above `--expected-max-n` |
| `correct` | `true` or `false` when `expected` is known, otherwise `null`; meaningful only for status `ok` |
| `errors` | the error messages of the run (connector errors, or the ERROR lines of its log) |
| `failure` | why the run did not complete (`engine/failures.py`), or `null` for `ok`; see below |
| `memory` | memory used by the query phase (`engine/memory.py`): `probe` (what was measured), `before_mb`, `peak_mb`, `used_mb` (peak minus before), `samples`, `interval_s`; `null` if the system has no probe |

The kinds of failure are `timeout` (the run exceeded `timeout_s`), `oom` (the system reported that it
exceeded its memory limit), `unsupported` (the system rejects the query), `iteration_limit`, `killed`
(ended by a signal that the engine did not send, such as the operating system's memory killer) and
`error` for everything else.

The records in `results/verified_2026` were written by the original driver (`run_benchmark.py` in
`results/verified_2026/harness/`). Their `command` field shows `analyze_dbs.py` or
`analyze_logic_systems.py` with the configuration JSON replaced by `'<config JSON>'`, and their timing
rows have no `MaxRAM_MB` columns. Otherwise the format is the same, and `analyze_verified.py` reads
both.

## Memory

The memory of a run is sampled only during its query phase: once just before the query, every
`interval_s` while it runs (10 ms, or 50 ms where the probe is a query to the server), and once after
it. The difference between the highest sample and the first, `used_mb`, is the memory the query
added, and the analysis reports its mean over the five runs. What is sampled depends on where the
query executes:

| System | Probe |
| --- | --- |
| DuckDB | resident memory (RSS) of the benchmark process, inside which DuckDB runs (a new process per run) |
| XSB | RSS of the XSB process of the query-only run, sampled from outside; "before" is the sample just before the query started (run time minus the query time XSB reports) |
| PostgreSQL | RSS of the backend process serving the connection (a new backend per run) |
| MariaDB | MariaDB's own accounting of the memory allocated by the connection's thread (`information_schema.PROCESSLIST.MEMORY_USED`), its internal temporary tables included |
| CockroachDB | CockroachDB's own accounting of SQL memory (`sql_mem_root_current` on the node's metrics endpoint, the quantity `--max-sql-memory` limits); it reserves memory in large chunks, about 64 MB even for tiny queries |
| MongoDB | none: MongoDB reports no per-query memory, and its macOS build has no allocator statistics |
| Neo4j | Neo4j's own accounting of the query's heap memory (`SHOW TRANSACTIONS ... estimatedUsedHeapMemory`), the quantity its transaction memory limit applies to; the JVM keeps its heap, so its RSS says nothing about one query |
| SingleStore | none: SingleStore reports memory only for the whole server (`Total_server_memory`), which keeps memory across runs; it also runs in a virtual machine, so its numbers would not be comparable |

The RSS of a server that serves all runs was not used. In trials, identical runs of MariaDB, MongoDB
and CockroachDB showed RSS increases that differed by up to a factor of 10, as did SingleStore's
`Total_server_memory`, because the allocator (or the Go runtime) keeps the memory freed by one run
and reuses it in the next. The per-run processes (DuckDB, XSB, a PostgreSQL backend) and the servers'
own accounting, however, gave the same value within a few percent in every run.

The measurement has limits. A peak shorter than the sampling interval can be missed, which matters
for queries of a few milliseconds. RSS on macOS leaves out compressed and swapped pages, and the
servers' own accounting covers only what they track (not the operating system's file cache, say).

## Reported values (`analyze_verified.py`)

The reported time is the real time of the system's `query_phase`, as named in its descriptor. A mean
is reported only if all five runs completed; the median, standard deviation, minimum and maximum are
in `summary.csv`. CPU time is shown only for XSB and DuckDB, whose query runs inside the measured
process.

The table cells use these marks:

| Cell | Meaning |
| --- | --- |
| `TO` | the first run exceeded the limit |
| `TO^s` | not run, because a smaller n exceeded the limit or failed |
| `OOM` | the run failed because memory ran out |
| `n/s` | the query is not supported (double recursion, for example) |
| `IL` | the run hit an iteration limit |
| `ERR` | any other error |
| `†` | a completed but incorrect result |

In the figures, panel (a) shows left recursion and panel (b) right recursion. Each system has the
same colour and marker in every figure (`engine/plot_style.py`), and each panel's legend lists the
systems in the order in which their curves end, from top to bottom. A hollow marker at the time limit
marks the first size that failed. The analysis warns when a reported time exceeds half the time
limit, because the limit should be clearly larger than every reported time. Every figure is written
both as a matplotlib PDF (`figures/`) and as a standalone pgfplots/TikZ document (`figures_tex/`),
which transcribes the matplotlib figure (`engine/figures_tex.py`). A figure shows the sizes the
campaign has for its graph.

`verification.json` contains the counts of runs by status, the configurations with incorrect
results as `<series>/<mode>: [graphs]`, and the agreement between systems on the large graphs.

`tests/test_verified_pipeline.py::TestAnalysis` checks that analyzing the published campaigns again
reproduces the published tables, `summary.csv`, `verification.json` and the 28 LaTeX figure sources
byte for byte.
