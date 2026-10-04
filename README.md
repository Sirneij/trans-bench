# trans-bench

trans-bench measures how database systems and logic systems evaluate recursive queries. The same
transitive-closure query, `tc(X, Z) :- tc(X, Y), e(Y, Z)` and its SQL, Cypher and Prolog
equivalents, is run on every system over fourteen graph families, and every result is compared with a
closure computed independently in Python. A fast answer that is wrong is therefore reported as wrong.
This repository holds the harness, the measured systems, the published campaigns and a web interface
for all of them.

## Published results

The measurements of *Database System Performance on Recursive Queries* are in
[`results/verified_2026_v2/`](results/verified_2026_v2/README.md). The campaign covers PostgreSQL,
MariaDB, DuckDB, CockroachDB, SingleStore, MongoDB, Neo4j and XSB, with 12,289 runs (five per
configuration, a 600 s limit per run) and the memory used by each query. Completed runs were checked
against the closure computed in Python, and on the largest scale-free graphs, where that closure
was not computed, by agreement between the systems. The first campaign, run with the same protocol but without memory measurements, is in
[`results/verified_2026/`](results/verified_2026/README.md) together with its incident log.

The paper's tables and figures can be derived again from the recorded runs, and the tests compare
them byte for byte with the published ones:

```sh
python analyze_verified.py results/verified_2026_v2 --out /tmp/reanalysis
python -m pytest -q tests
```

[docs/REPRODUCING.md](docs/REPRODUCING.md) explains how to verify the data or repeat the campaign.

## How a campaign runs

There is one execution engine, `engine/campaign.py`. A campaign started from the web interface, from
`transitive.py` or from `benchmark.py` goes through it, so every measurement follows the same rules:

- Each run is a separate process (`python -m engine.run_one`) in its own process group.
- A run that exceeds the time limit is killed with its whole process group, and the query still
  running inside the server is cancelled; the run is recorded as `timeout`.
- A failed run is recorded with the kind of failure (timeout, out of memory, unsupported query,
  iteration limit, killed, error), the remaining runs of that configuration are not started, and all
  larger sizes of the same system, graph and mode are recorded as `skipped`.
- In the transitive-closure domain, every successful result is checked against the independent
  closure (row count and a 64-bit order-independent hash) before the result file is deleted.
- Input graphs that do not exist yet are generated first with `generate_db.py`.
- Every run is appended to `results/<campaign>/<series>/runs.jsonl`, with its full output in `logs/`
  and its timing rows in `timing/`. When the campaign ends, `analyze_verified.py` writes the tables
  and the figures (matplotlib PDF and pgfplots/TikZ) into `results/<campaign>/analysis/`.

A campaign that is stopped can be started again with the same settings: configurations that already
have records are not run twice, and the run that was interrupted is repeated. The format of the
records and the checks are described in [docs/VERIFICATION.md](docs/VERIFICATION.md).

## Getting started

Python 3.12 is used throughout; `requirements.txt` pins the versions of the published campaign.

```sh
git clone https://github.com/Sirneij/trans-bench.git
cd trans-bench
python3.12 -m venv virtualenv
source virtualenv/bin/activate
# mysqlclient is built against the MariaDB client library
PKG_CONFIG_PATH=/opt/homebrew/opt/mariadb/lib/pkgconfig pip install -r requirements.txt
```

DuckDB runs inside the harness and needs no server. Every other system needs a running server and a
`systems/<name>/credentials.yaml`, which is ignored by git. Copy it from the
`credentials.example.yaml` next to it, or enter it on the system's page of the web interface.
[docs/SYSTEMS.md](docs/SYSTEMS.md) describes how each server was installed and configured.

### The web interface

```sh
python transitive.py --ui                 # http://127.0.0.1:5000
python transitive.py --ui --ui-port 5055  # on macOS, AirPlay Receiver often holds port 5000
```

| Page | Address | Contents |
| --- | --- | --- |
| Home | `/` | Animated introduction, driven by the newest campaign: a closure growing iteration by iteration, the topologies, the race at the largest size, and the verification counts |
| Overview | `/overview` | Campaigns and their outcomes, the leaderboard, systems and topologies |
| Campaigns | `/campaigns`, `/campaigns/<name>` | Outcomes per series, scaling and race charts, time and memory matrices, the paper's figures, failures, README and versions |
| Results explorer | `/results` | The timing row of every run, by campaign, series, topology, mode and size; phase-by-phase comparison of systems |
| Systems | `/systems`, `/systems/<name>` | Descriptor, timing phases, rule files and credentials of each system |
| Topologies | `/graphs`, `/graphs/<name>` | Every graph family drawn from its own generator, with the pairs its closure adds |
| New experiment | `/experiment/new` | Systems, topologies, settings (campaign name, sizes, runs, time limit), review; start the campaign or copy the command |
| Live monitor | `/experiment/live` | Progress of the running campaign, every configuration as a tile, the output; Stop ends the run in progress |

Press <kbd>⌘K</kbd> or <kbd>/</kbd> to jump to any page, system, topology or campaign, <kbd>n</kbd> for a
new experiment and <kbd>t</kbd> to change the theme. `--ui-debug` turns on Flask's debugger and
reloader for local work.

### The command line

`transitive.py` takes the sizes as a range; `benchmark.py` takes them as a list, which is how
`scripts/run_all.sh` drove the published campaign. Both run the same engine.

```sh
# DuckDB and PostgreSQL on two graphs, n = 100, 200 and 300, five runs each
python transitive.py --systems duckdb postgres --graphs cycle path --sizes 100 301 100 --campaign results/my_run
python benchmark.py --systems duckdb postgres --graphs cycle path --sizes 100 200 300 --campaign results/my_run

# one series with a variant setting, as in the published campaign
python benchmark.py --systems mariadb --sizes 100 200 --out results/my_run/mariadb_tuned --label tmp_table_size=4G
```

The main options are `--runs` (or `--num-runs`), `--timeout` in seconds (default 600),
`--modes`, `--domain`, `--query-mode` and `--no-analysis`. `python benchmark.py --help` lists all of
them.

## Repository layout

```
trans-bench/
├── engine/                  the harness
│   ├── campaign.py          the execution engine (CampaignSpec, Campaign)
│   ├── run_one.py           one trial in its own process
│   ├── runner.py            one trial: connect, run the rule file, write the timing row
│   ├── connectors/          one connector per protocol (SQL databases, DuckDB, Neo4j, MongoDB, logic systems)
│   ├── loader.py            descriptors of systems, graph types and domains
│   ├── verify.py            the independent closure check (count and hash)
│   ├── memory.py            memory sampling while a query runs
│   ├── failures.py          classification of failed runs
│   └── figures_tex.py       matplotlib figures as standalone pgfplots/TikZ documents
├── systems/<name>/          descriptor.yaml, rules/, credentials.example.yaml
├── graph_types/             one descriptor per graph family
├── generate_db.py           the graph generators; writes input/
├── input/                   generated inputs (the small sizes are tracked), SHA256SUMS, expected_closures.json
├── benchmark.py             command line: sizes as a list
├── transitive.py            command line: sizes as a range, the web interface, scaffolding and rule checks
├── analyze_verified.py      tables, verification report and figures of a campaign
├── results/                 the campaigns
├── ui/                      the Flask web interface
├── scripts/                 campaign scripts, input check, comparison of campaigns, checks for CI
├── templates/               templates for new systems, rules and domains
├── tests/
└── docs/
```

## Extending the suite

A system is described by `systems/<name>/descriptor.yaml`; the engine finds it at start-up. How much
else is needed depends on the protocol:

DuckDB's SQL scripts (`protocol: duckdb`) need only the descriptor and one `.sql` file per mode,
whose statements map one to one to the timing phases. The server databases (`psycopg2`,
`mysqlclient`, `cockroachdb`, `singlestore` and `pymongo`) load Python operation classes instead: a
shared `systems/<name>/__init__.py` holds the steps of a run, and `rules/<domain>_<mode>.py` holds one
class per mode. Hence, copying `systems/postgres` gives a working start for another
PostgreSQL-compatible server. The logic systems and Neo4j read their rule files (`.P`, `.lp`, `.dl`,
`.da`, `.cypher`) directly. A new protocol needs one connector class, registered in
`engine/connectors/__init__.py` or dropped in as `systems/<name>/connector.py`.

A new graph family needs a `generate_<name>_graph` method in `generate_db.py` and a descriptor in
`graph_types/`. Query domains other than the transitive closure are supported through
`domains/<name>/descriptor.yaml`; the repository ships templates for them in `templates/`, but rule
files only for the transitive closure.

```sh
python transitive.py --bootstrap-system my_database --bootstrap-system-template descriptor_sql_database.yaml
python transitive.py --validate-rules my_database
python transitive.py --test-rule systems/duckdb/rules/transitive_left_recursion.sql
```

The step-by-step guide is [docs/EXTENSION_GUIDE.md](docs/EXTENSION_GUIDE.md); rule examples are in
[docs/RULES.md](docs/RULES.md), and worked recipes in [docs/COOKBOOK.md](docs/COOKBOOK.md).

### Descriptor reference

```yaml
name: postgres                  # the directory name
display_name: PostgreSQL
category: db                    # db | logic | hybrid
protocol: psycopg2              # connector (engine/connectors/__init__.py)
timing_phases:                  # the CSV columns, in the order the phases run
  - { id: create_table, label: CreateTable }   # -> CreateTableRealTime, CreateTableCPUTime, CreateTableMaxRAM_MB
  - { id: execute_query, label: ExecuteQuery }
query_phase: execute_query      # the phase that is the query itself (what the analysis reports)
result_file: postgres_results.csv  # the result, checked after every run
input_format: tsv               # tsv | lp | facts | pickle
modes: [right_recursion, left_recursion, double_recursion]
rule_extension: .py
flags:
  requires_credentials: true
  class_prefix: PostgreSQL      # class PostgreSQLLeftRecursion in rules/transitive_left_recursion.py
  module_prefix: postgres_rules # the name the rule modules import the shared operations under
```

### Systems and topologies

| System | Protocol | Modes |
| --- | --- | --- |
| PostgreSQL | psycopg2 | right, left; double recursion is rejected by the server |
| MariaDB | mysqlclient | right, left, double |
| DuckDB | duckdb | right, left, double (incomplete results), doublerecurring |
| CockroachDB | cockroachdb | right, left; double recursion is rejected |
| SingleStore | singlestore | right and left on acyclic graphs only; double recursion is rejected |
| MongoDB | pymongo | one `$graphLookup` pipeline |
| Neo4j | neo4j | one Cypher query |
| XSB | subprocess | right, left, double |
| Clingo | clingo_python | right, left, double |
| Soufflé | souffle_subprocess | right, left, double |
| ALDA | alda_subprocess | right, left, double |

The first eight were measured in the published campaigns; [docs/SYSTEMS.md](docs/SYSTEMS.md)
explains why some modes are rejected or incomplete. The graph families are `complete`,
`max_acyclic`, `cycle`, `cycle_with_shortcuts`, `path`, `multi_path`, `grid`, `binary_tree`,
`reverse_binary_tree`, `x`, `y`, `w`, `star`, `scale_free` and `barabasi_albert`.

## Development

The tests need no database server: the end-to-end tests run DuckDB on the tracked inputs, and the
analysis tests derive the published tables again. The static checks follow the setup of my Django
series on dev.to: black and isort for formatting, prospector (pylint, pycodestyle, pydocstyle,
mccabe) at very high strictness, bandit for security and mypy for types.

```sh
pip install -r requirements_dev.txt
scripts/static_validation.sh
scripts/test.sh            # in parallel, with coverage
```

GitHub Actions runs both scripts on every push and pull request (`.github/workflows/ci.yml`).

## Public deployment

The web interface is deployed read-only on Railway from the `Dockerfile`, as described in
`railway.json`. Its image holds the descriptors and the published campaigns, but no database. In
read-only mode the site refuses every request that would start a campaign or change a file, as well
as the rule checks that contact a server. Credentials are hidden, and a banner says why. The mode is
on when `TRANS_BENCH_READ_ONLY=1` (set in the Dockerfile) or when the app runs on Railway. On Railway, the build and start commands come from
the Dockerfile, so neither needs to be set; only a `SECRET_KEY` variable has to be added.

```sh
docker build -t trans-bench-ui .
docker run --rm -p 10000:10000 trans-bench-ui
```

## Further documentation

| Document | Contents |
| --- | --- |
| [REPRODUCING.md](docs/REPRODUCING.md) | Checking the published data, and repeating the campaign |
| [VERIFICATION.md](docs/VERIFICATION.md) | The correctness check, the run protocol and the record format |
| [SYSTEMS.md](docs/SYSTEMS.md) | Installation, configuration and pitfalls of every measured system |
| [EXTENSION_GUIDE.md](docs/EXTENSION_GUIDE.md) | Adding systems, protocols, graph types and domains |
| [RULES.md](docs/RULES.md) | Rule files for each language |
| [COOKBOOK.md](docs/COOKBOOK.md) | Worked recipes |
| [ui_features.md](docs/ui_features.md) | The web interface in detail |
| [scale_free_findings.md](docs/scale_free_findings.md) | Notes on the scale-free and Barabási-Albert graphs |
| [templates/README.md](templates/README.md) | The templates for new systems, rules and domains |
