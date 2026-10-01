# Reproducing and verifying the published measurements

The measurements in *Database System Performance on Recursive Queries* (Section 4) are the
campaign in [`results/verified_2026_v2/`](../results/verified_2026_v2/README.md), a complete re-run with memory
measurements of the first campaign in [`results/verified_2026/`](../results/verified_2026/README.md): 8 systems (MariaDB
also in a tuned setting), 12 graph families with n = 100…1000, scale-free graphs with
10k…90k nodes and Barabási-Albert graphs with 10k…100k nodes, 5 runs per configuration, and
12,279 runs, each checked for correctness. This document covers three things:

* [A](#a-verify-the-published-results-without-running-anything): checking the published data and
  analysis (minutes, no databases needed);
* [B](#b-re-run-the-campaign): re-running all or part of the campaign, and comparing it with the
  published one;
* [C](#c-what-happened-during-the-2026-campaign): what happened during the 2026 campaign
  (incidents, invalid runs, and harness history).

## A. Verify the published results without running anything

```sh
python3.12 -m venv virtualenv && source virtualenv/bin/activate
PKG_CONFIG_PATH=/opt/homebrew/opt/mariadb/lib/pkgconfig pip install -r requirements.txt

# 1. Re-derive every table row, summary.csv and verification.json of the paper from the per-run
#    records, and compare them with the published files (byte for byte):
python analyze_verified.py results/verified_2026_v2 --out /tmp/reanalysis
diff -r --exclude=figures --exclude=figures_tex results/verified_2026_v2/analysis /tmp/reanalysis && echo identical

# 2. The same check, plus the whole pipeline, as tests:
python -m pytest -q tests
```

Each record in `results/verified_2026_v2/<series>/runs.jsonl` contains the run's command, times,
harness timing row, result count and hash, expected count and hash, and correctness. The full
output of every run is in `logs.tar.gz`. The record format is described in
[VERIFICATION.md](VERIFICATION.md).

## B. Re-run the campaign

### 1. Software

These are the versions used in 2026. Newer versions of the database systems can be used, but the
results then belong to those versions.

```sh
brew install postgresql@17               # 17.11, run as a brew service
brew tap mongodb/brew && brew install mongodb-community   # 8.3.11
brew install mariadb                     # 13.0.2
brew install cockroachdb/tap/cockroach   # 26.3.2
brew install python@3.12 pkgconf
brew install neo4j colima docker         # neo4j 2026.09.0 (brings openjdk@21), colima 0.10.3
# XSB 5.0.0 from source, and the configuration of every server: docs/SYSTEMS.md
```

Python (3.12.14 in 2026), with the versions pinned in `requirements.txt`. These pins matter for
duckdb and for networkx, whose seeded graph generators must produce the same graphs:

```sh
python3.12 -m venv virtualenv
PKG_CONFIG_PATH=/opt/homebrew/opt/mariadb/lib/pkgconfig ./virtualenv/bin/pip install -r requirements.txt
```

### 2. Inputs

Only the inputs up to n = 500 are tracked in git. Generate the rest, then check that every input
is byte-identical to the published campaign's inputs (`input/SHA256SUMS`, 278 files):

```sh
./virtualenv/bin/python generate_db.py --sizes 100 1001 100 --graph-types complete max_acyclic cycle \
    cycle_with_shortcuts path multi_path grid binary_tree reverse_binary_tree x y w
./virtualenv/bin/python generate_db.py --sizes 10000 100001 10000 --graph-types scale_free barabasi_albert
./virtualenv/bin/python scripts/verify_inputs.py        # → 278 identical, 0 different, 0 missing
```

### 3. Servers and credentials

Install and configure each server as described in [SYSTEMS.md](SYSTEMS.md). Copy
`systems/<name>/credentials.example.yaml` to `credentials.yaml` (gitignored) and fill it in. The
2026 settings that differ from the defaults are all listed there:

* SingleStore: `maximum_memory` = 6000 MB, and `max_recursive_cte_iterations` = 10000 (set by the
  harness);
* the `mariadb_tuned` series: 4 GB temporary tables.

### 4. Run

Everything, one system at a time, exactly as in 2026. The runs took 37.6 hours in total on the M3 Pro (sum of `wall_s`, including re-runs):

```sh
export SCRATCH=<directory for server data: crdb-data, crdb-extern, mongo-data, neo4j/>
export XSB_BIN=<path to XSB/bin>
scripts/capture_versions.sh > results/my_run/versions.txt
./virtualenv/bin/pip freeze > results/my_run/pip_freeze.txt
scripts/run_all.sh results/my_run      # phases: duckdb xsb postgres cockroachdb mariadb mariadb_tuned mongodb neo4j singlestore
```

`run_all.sh` takes a list of phases (e.g. `scripts/run_all.sh results/my_run duckdb xsb`). Or run
any subset directly with the driver:

```sh
./virtualenv/bin/python benchmark.py --systems duckdb --graphs cycle path --modes left_recursion right_recursion \
    --sizes 100 200 300 --runs 5 --timeout 600 --out results/my_run/duckdb
```

The output directory is `results/<campaign>/<series>/`, where the series is the system name
optionally followed by a suffix (`mariadb_tuned`). The driver can be stopped and restarted; it
continues where it stopped.

The MariaDB wrong-result investigation (paper, Section 4.4) must run with MariaDB alone, because
it connects through the socket:

```sh
scripts/run_investigation.sh results/my_run/mariadb_investigation.jsonl
```

### 5. Analyze and compare

The analysis runs automatically: `benchmark.py` analyzes the campaign directory when it finishes, and
`scripts/run_all.sh` does it once at the end. It writes `results/my_run/analysis/`:

* `summary.csv` and `verification.json`;
* `table_*.tex` (the rows of the paper's tables);
* every figure twice, with the same content and layout:
  * `figures/<graph>_{elapsed,cpu}.pdf` (matplotlib);
  * `figures_tex/<graph>_{elapsed,cpu}.tex`, a standalone pgfplots/TikZ document, compiled to
    `figures_tex/<name>.pdf`.

The LaTeX version is a transcription of the matplotlib figure made by `engine/figures_tex.py`: the
same page size, axes, limits, ticks, data, markers, dashes, colours and legend, and every text at
the position matplotlib draws it, in the same font (DejaVu Sans). It is compiled with the first
engine found among tectonic, lualatex, xelatex and pdflatex. Without one, the `.tex` files are
still written and can be compiled later (`--no-latex-compile` does the same on purpose).

To run the analysis by hand, or to compare with the published campaign:

```sh
./virtualenv/bin/python analyze_verified.py results/my_run --out results/my_run/analysis   # --runs N if not 5
./virtualenv/bin/python scripts/compare_results.py results/my_run    # against results/verified_2026_v2
```

To use a LaTeX figure in a paper, include its compiled PDF
(`\includegraphics[width=\textwidth]{figures_tex/cycle_elapsed.pdf}`), exactly like the matplotlib
PDF. Don't `\input` the `.tex` file: its preamble sets the figure's own fonts, marker shapes and
colours. The `.tex` file is the editable source; change the plotting code in `analyze_verified.py`,
or edit the `.tex` directly for a one-off change.

`compare_results.py` checks that every completed run returned the **same result** (count and
hash) as in the published campaign; this must match on any machine. It lists the configurations
whose status differs (possible near the 600 s limit on a faster or slower machine) and the ratio
of the median query times per system.

## C. What happened during the 2026 campaigns

**Re-run with memory measurements** (`results/verified_2026_v2`, the paper's data), 2026-09-29
02:26Z to 2026-09-30 17:02Z, with the current harness. The harness was the commit in
`versions.txt` plus `code.patch`. It was run with:

```sh
SCRATCH=<server data> XSB_BIN=<XSB/bin> scripts/capture_versions.sh > results/verified_2026_v2/versions.txt
SCRATCH=<server data> XSB_BIN=<XSB/bin> caffeinate -i -s scripts/run_all.sh results/verified_2026_v2
```

This run had no incidents. Before it, every system was smoke-tested on a few configurations,
which is how the memory probes were chosen (docs/VERIFICATION.md). Its results agree with the
first campaign (`results/verified_2026_v2/README.md`).

**First campaign** (`results/verified_2026`):


Commands as executed (all times UTC; first run 2026-09-26 13:42Z, last run 2026-09-28 05:16Z; the full driver output, with the start and end of every phase, is
`results/verified_2026/run_all.log.gz`):

```sh
./capture_versions.sh > results/versions.txt; ./venv/bin/pip freeze > results/pip_freeze.txt
./run_all.sh results      # all phases
```

### Incidents and re-runs

All invalid records are kept, each with a README.

1. **Neo4j, 2026-09-27 22:18Z.** The first Neo4j phase timed the query with `consume()`, which lets
   Neo4j skip computing the rows: 0.04 s instead of 27 s for the export. The phase was stopped and
   the timing fixed (every statement finishes inside its timed call; the query returns the number
   of distinct pairs). Neo4j was then re-run. The invalid runs are in
   `results/verified_2026/invalid/neo4j_consume_timing/`.
2. **SingleStore, 2026-09-27 23:27Z.** The first query (Cmpl_100) made the VM's out-of-memory killer
   terminate the leaf node, and all later queries failed ("Failed to find a master partition").
   `maximum_memory` was lowered to 6000 MB, and the phase was re-run at 2026-09-28 03:08Z. The
   invalid runs are in `results/verified_2026/invalid/singlestore_server_crash/`.
3. **CockroachDB.** After the timeout of scale_free/left/20000, the cancelled `CREATE TABLE … AS`
   kept running as a schema-change job, and the following scale_free/right and BA runs failed
   (`table "tc_result" is being added`). The driver was fixed (it cancels the jobs one by one and
   waits until the tables can be dropped). The stuck jobs were cancelled manually
   (`CANCEL JOB 1213804023738073089`, `CANCEL JOB 1214007923699646465`), and the affected
   configurations were re-run at 2026-09-27 23:46Z and 2026-09-28 01:09Z. The invalid runs are in
   `results/verified_2026/cockroachdb/runs_invalid_cleanup_bug.jsonl`.

### Harness used in 2026, and how it maps to this code

The campaign ran on the original harness (`analyze_dbs.py` / `analyze_logic_systems.py`, one
process per run) at upstream `main` 92ecc775 plus 17 commits. The 15 code commits are in
`results/verified_2026/harness/` as patches. The other two commits added the scale-free/BA input
files; they are replaced here by `generate_db.py` and `input/SHA256SUMS`. This branch ports every
change into the plugin architecture:

| 2026 harness commit(s) | Change | Here |
| --- | --- | --- |
| 085be459 | CockroachDB: concatenate all EXPORT chunks, clean the export directory | `CockroachDBConnector`, `concatenate_chunks` |
| 013f8231, 443e4e72, 6b1dd055 | Neo4j: finish every result inside its timed call; the timed query counts the distinct pairs | `Neo4jConnector`, `systems/neo4j/rules/*.cypher` |
| ffd9cb69 | SingleStore support | `systems/singlestore/`, `SingleStoreConnector` |
| bdc0ab6a | DuckDB double recursion over `recurring.tc` | `systems/duckdb/rules/transitive_doublerecurring_recursion.sql` |
| 6e647478 | per-run driver with time limit, cancellation and verification; `verify.py` | `benchmark.py`, `engine/run_one.py`, `engine/verify.py`, `cancel_running()` |
| ef67c8ee, 551ac62b | scale-free/BA inputs; XSB facts deduplicated | `generate_db.py` (distinct sorted `.lp` facts for multigraphs), `input/SHA256SUMS` |
| 15d040e1 | `run_all.sh`, `capture_versions.sh` | `scripts/` |
| ad14a66b, 3fe8df8e | CockroachDB: cancel schema-change jobs one by one after a timeout | `CockroachDBConnector.cancel_running` |
| 1af79aea | Neo4j: single query once per large graph | `scripts/run_all.sh` |
| 95233a7f, 78b204b8 | MariaDB wrong-result investigation | `scripts/investigate_mariadb.py`, `scripts/run_investigation.sh` |
| c1d758cb | analysis | `analyze_verified.py` |

For each (system, mode), the statements, their order, and what each timing phase covers are the
same as in the 2026 harness. The rule files are identical except for formatting. The DuckDB
results of the current code were checked against the published records: same counts and hashes,
e.g. `tests/test_verified_pipeline.py::TestBenchmarkDriver`. There are three small differences in
the records, none of which affects any measured phase:

* timing rows have an extra `MaxRAM_MB` column per phase;
* `command` shows `python -m engine.run_one …` (credentials come from files);
* timing CSVs are laid out as `timing/<domain>/<system>/<graph>/<mode>_graph_<n>.csv`.
