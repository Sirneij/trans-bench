# Reproducing and verifying the published measurements

The measurements in Section 4 of *Database System Performance on Recursive Queries* are the campaign
in [`results/verified_2026_v2/`](../results/verified_2026_v2/README.md). It repeats the first
campaign, in [`results/verified_2026/`](../results/verified_2026/README.md), with memory
measurements added. Eight systems were measured, MariaDB also in a tuned setting. The twelve linear
graph families ran with n = 100 to 1,000, the scale-free graphs with 10,000 to 90,000 nodes and the
Barabási-Albert graphs with 10,000 to 100,000 nodes. With five runs per configuration, the campaign
executed 12,289 runs, and every completed run was checked.

This document has three parts. Part A checks the published data and analysis, which takes minutes
and needs no database. Repeating all or part of the campaign, and comparing it with the published
one, is the subject of Part B. Finally, Part C records what happened during the 2026 campaigns,
from the incidents and invalid runs to the history of the harness.

## A. Verify the published results without running anything

```sh
python3.12 -m venv virtualenv && source virtualenv/bin/activate
PKG_CONFIG_PATH=/opt/homebrew/opt/mariadb/lib/pkgconfig pip install -r requirements.txt

# 1. Derive every table row, summary.csv and verification.json of the paper again from the per-run
#    records, and compare them with the published files byte for byte
python analyze_verified.py results/verified_2026_v2 --out /tmp/reanalysis
diff -r --exclude=figures --exclude=figures_tex results/verified_2026_v2/analysis /tmp/reanalysis && echo identical

# 2. The same check, with the rest of the pipeline, as tests
python -m pytest -q tests
```

Each record in `results/verified_2026_v2/<series>/runs.jsonl` holds the run's command, its times
and harness timing row, the count and hash of its result, the expected count and hash, and the
verdict. The full output of every run is in `logs.tar.gz`. The record format is described in
[VERIFICATION.md](VERIFICATION.md).

## B. Repeat the campaign

### 1. Software

These are the versions used in 2026. Newer versions of the database systems can be used, but the
results then belong to those versions.

```sh
brew install postgresql@17               # 17.11, run as a brew service
brew tap mongodb/brew && brew install mongodb-community   # 8.3.11
brew install mariadb                     # 13.0.2
brew install cockroachdb/tap/cockroach   # 26.3.2
brew install python@3.12 pkgconf
brew install neo4j colima docker         # neo4j 2026.09.0 (with openjdk@21), colima 0.10.3
# XSB 5.0.0 is built from source; it and the configuration of every server are in docs/SYSTEMS.md
```

Python was 3.12.14, with the versions pinned in `requirements.txt`. The pins matter most for
duckdb, and for networkx, whose seeded generators must produce the same graphs:

```sh
python3.12 -m venv virtualenv
PKG_CONFIG_PATH=/opt/homebrew/opt/mariadb/lib/pkgconfig ./virtualenv/bin/pip install -r requirements.txt
```

### 2. Inputs

Only the inputs up to n = 500 are tracked in git. The rest are generated, and then every input is
compared with the published campaign's inputs (`input/SHA256SUMS`, 278 files):

```sh
./virtualenv/bin/python generate_db.py --sizes 100 1001 100 --graph-types complete max_acyclic cycle \
    cycle_with_shortcuts path multi_path grid binary_tree reverse_binary_tree x y w
./virtualenv/bin/python generate_db.py --sizes 10000 100001 10000 --graph-types scale_free barabasi_albert
./virtualenv/bin/python scripts/verify_inputs.py        # 278 identical, 0 different, 0 missing
```

The campaign engine would also generate a missing input on its own before the first run that needs
it, so this step mainly serves the byte-for-byte check.

### 3. Servers and credentials

Each server is installed and configured as described in [SYSTEMS.md](SYSTEMS.md). The credentials
go into `systems/<name>/credentials.yaml`, which git ignores; `credentials.example.yaml` in the same
directory is the starting point. Two settings of 2026 differ from the defaults:

* SingleStore ran with `maximum_memory` = 6000 MB, and the harness raises
  `max_recursive_cte_iterations` to 10,000 in every session;
* the `mariadb_tuned` series ran with 4 GB temporary tables.

### 4. Run

The whole campaign is run one system at a time, exactly as in 2026. On the M3 Pro, the runs took
37.6 hours in total (the sum of `wall_s`, re-runs included).

```sh
export SCRATCH=<directory for server data: crdb-data, crdb-extern, mongo-data, neo4j/>
export XSB_BIN=<path to XSB/bin>
scripts/capture_versions.sh > results/my_run/versions.txt
./virtualenv/bin/pip freeze > results/my_run/pip_freeze.txt
scripts/run_all.sh results/my_run      # phases: duckdb xsb postgres cockroachdb mariadb mariadb_tuned mongodb neo4j singlestore
```

`run_all.sh` also takes a list of phases, say `scripts/run_all.sh results/my_run duckdb xsb`. Any
subset can be run directly as well:

```sh
./virtualenv/bin/python benchmark.py --systems duckdb --graphs cycle path --modes left_recursion right_recursion \
    --sizes 100 200 300 --runs 5 --timeout 600 --out results/my_run/duckdb
```

The output goes to `results/<campaign>/<series>/`, where the series is the system's name, optionally
followed by a suffix such as `_tuned`. With `--campaign results/my_run` in place of `--out`, several
systems are run into one campaign, each into its own series. A campaign can be stopped and started
again: it continues where it stopped, and the run that was interrupted is repeated.

The investigation of MariaDB's wrong results (Section 4.4 of the paper) must run with MariaDB
alone, because it connects through the server's socket:

```sh
scripts/run_investigation.sh results/my_run/mariadb_investigation.jsonl
```

### 5. Analyze and compare

The analysis runs on its own: `benchmark.py` analyzes the campaign when it finishes, and
`scripts/run_all.sh` does so once at the end. It writes `results/my_run/analysis/`:

* `summary.csv` and `verification.json`;
* `table_*.tex`, the rows of the paper's tables;
* every figure twice, with the same content and layout, as `figures/<graph>_{elapsed,cpu}.pdf`
  (matplotlib) and as `figures_tex/<graph>_{elapsed,cpu}.tex`, a standalone pgfplots/TikZ
  document compiled to `figures_tex/<name>.pdf`.

The LaTeX version is a transcription of the matplotlib figure made by `engine/figures_tex.py`. Page
size, axes, limits, ticks, data, markers, dashes, colours and legend are the same, and every text
sits where matplotlib draws it, in the same font (DejaVu Sans). The document is compiled with the
first engine found among tectonic, lualatex, xelatex and pdflatex. Without any of them, the `.tex`
files are still written and can be compiled later; `--no-latex-compile` does the same on purpose.

The analysis can also be run by hand, and the campaign compared with the published one:

```sh
./virtualenv/bin/python analyze_verified.py results/my_run --out results/my_run/analysis   # --runs N if not 5
./virtualenv/bin/python scripts/compare_results.py results/my_run    # against results/verified_2026_v2
```

A LaTeX figure goes into a paper through its compiled PDF
(`\includegraphics[width=\textwidth]{figures_tex/cycle_elapsed.pdf}`), exactly like the matplotlib
PDF. The `.tex` file should not be `\input`, because its preamble sets the figure's own fonts, marker
shapes and colours. It is the editable source: lasting changes belong in the plotting code of
`analyze_verified.py`, and a one-off change can be made in the `.tex` file itself.

`compare_results.py` checks that every completed run returned the same result (count and hash) as
in the published campaign. This must hold on any machine. It also lists the configurations whose
status differs, which can happen near the 600 s limit on a faster or slower machine, and the ratio
of the median query times per system.

## C. What happened during the 2026 campaigns

**The repeat with memory measurements** (`results/verified_2026_v2`, the paper's data) ran from
2026-09-29 02:26Z to 2026-09-30 17:02Z. The harness was the commit in `versions.txt` plus
`code.patch`, and it was started with:

```sh
SCRATCH=<server data> XSB_BIN=<XSB/bin> scripts/capture_versions.sh > results/verified_2026_v2/versions.txt
SCRATCH=<server data> XSB_BIN=<XSB/bin> caffeinate -i -s scripts/run_all.sh results/verified_2026_v2
```

This run had no incidents. Before it, every system was tested on a few configurations, and that is
how the memory probes were chosen ([VERIFICATION.md](VERIFICATION.md)). Its results agree with the
first campaign, as `results/verified_2026_v2/README.md` shows.

**The first campaign** (`results/verified_2026`) ran from 2026-09-26 13:42Z to 2026-09-28 05:16Z,
all times UTC. The full driver output, with the start and end of every phase, is
`results/verified_2026/run_all.log.gz`. It was started with:

```sh
./capture_versions.sh > results/versions.txt; ./venv/bin/pip freeze > results/pip_freeze.txt
./run_all.sh results      # all phases
```

### Incidents and repeated runs

All invalid records were kept, each with a README.

1. **Neo4j, 2026-09-27 22:18Z.** The first Neo4j phase timed the query with `consume()`, which lets
   Neo4j skip computing the rows: the export took 0.04 s where the real work takes 27 s. The phase was stopped and
   the timing fixed, so that every statement finishes inside its timed call and the query returns
   the number of distinct pairs. Neo4j was then run again. The invalid runs are in
   `results/verified_2026/invalid/neo4j_consume_timing/`.
2. **SingleStore, 2026-09-27 23:27Z.** The first query (Cmpl_100) made the virtual machine's
   out-of-memory killer end the leaf node, and every later query failed with "Failed to find a
   master partition". `maximum_memory` was lowered to 6000 MB, and the phase was run again at
   2026-09-28 03:08Z. The invalid runs are in `results/verified_2026/invalid/singlestore_server_crash/`.
3. **CockroachDB.** After the timeout of scale_free/left/20000, the cancelled `CREATE TABLE ... AS`
   kept running as a schema-change job, and the following scale_free/right and Barabási-Albert runs
   failed (`table "tc_result" is being added`). The driver was fixed: it now cancels such jobs one by
   one and waits until the tables can be dropped. The stuck jobs were cancelled by hand
   (`CANCEL JOB 1213804023738073089`, `CANCEL JOB 1214007923699646465`), and the affected
   configurations were run again at 2026-09-27 23:46Z and 2026-09-28 01:09Z. The invalid runs are in
   `results/verified_2026/cockroachdb/runs_invalid_cleanup_bug.jsonl`.

### The 2026 harness, and where its changes live now

The first campaign ran on the original harness (`analyze_dbs.py` and `analyze_logic_systems.py`,
one process per run) at upstream `main` 92ecc775 plus 17 commits. The 15 code commits are kept in
`results/verified_2026/harness/` as patches. The other two added the scale-free and
Barabási-Albert input files; `generate_db.py` and `input/SHA256SUMS` replace them here. Every change
was carried over into the plugin architecture:

| 2026 harness commit(s) | Change | Here |
| --- | --- | --- |
| 085be459 | CockroachDB: concatenate all EXPORT chunks, clean the export directory | `CockroachDBConnector`, `concatenate_chunks` |
| 013f8231, 443e4e72, 6b1dd055 | Neo4j: finish every result inside its timed call; the timed query counts the distinct pairs | `Neo4jConnector`, `systems/neo4j/rules/*.cypher` |
| ffd9cb69 | SingleStore support | `systems/singlestore/`, `SingleStoreConnector` |
| bdc0ab6a | DuckDB double recursion over `recurring.tc` | `systems/duckdb/rules/transitive_doublerecurring_recursion.sql` |
| 6e647478 | per-run driver with time limit, cancellation and verification; `verify.py` | `engine/campaign.py` (with `benchmark.py` as its command line), `engine/run_one.py`, `engine/verify.py`, `cancel_running()` |
| ef67c8ee, 551ac62b | scale-free and Barabási-Albert inputs; XSB facts without duplicates | `generate_db.py` (distinct, sorted `.lp` facts for multigraphs), `input/SHA256SUMS` |
| 15d040e1 | `run_all.sh`, `capture_versions.sh` | `scripts/` |
| ad14a66b, 3fe8df8e | CockroachDB: cancel schema-change jobs one by one after a timeout | `CockroachDBConnector.cancel_running` |
| 1af79aea | Neo4j: its single query once per large graph | `scripts/run_all.sh` |
| 95233a7f, 78b204b8 | investigation of MariaDB's wrong results | `scripts/investigate_mariadb.py`, `scripts/run_investigation.sh` |
| c1d758cb | analysis | `analyze_verified.py` |

For each system and mode, the statements, their order, and what each timing phase covers are the
same as in the 2026 harness, and the rule files differ only in formatting. After the campaigns, the
driver was moved from `benchmark.py` into `engine/campaign.py`, so that the web interface and
`transitive.py` run campaigns with it too; the rules for a run did not change. The DuckDB results of
the current code were checked against the published records and have the same counts and hashes
(`tests/test_verified_pipeline.py::TestBenchmarkDriver`). The records differ in three small ways,
none of which affects a measured phase:

* the timing rows have an extra `MaxRAM_MB` column per phase;
* `command` shows `python -m engine.run_one ...`, because credentials now come from files;
* the timing CSVs are arranged as `timing/<domain>/<system>/<graph>/<mode>_graph_<n>.csv` inside the
  series directory.
