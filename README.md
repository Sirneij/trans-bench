# Benchmarking experiments for logic rule and database systems

## What is what and where's what

1. **`alda`:** Contains the source code of the Alda logic system

2. **`alda_rules`:** Contains rule files for the Alda environment. These `.da` files define transitive closure rules in a syntax compatible with Alda, for double recursion, left recursion, and right recursion.

3. **`analysis`:** Houses the folders and files generated from `analyze.py`.

4. **`clingo_rules`:** Stores rule files for the Clingo environment. These `.lp` (logic programming) files define transitive closure rules for double recursion, left recursion, and right recursion in Clingo's syntax.

5. **`cockroachdb_rules`:** Houses the Python files containing the classes that interact with CockroachDB using the `psycopg2` driver.

6. **`duckdb_rules`:** All the SQL files which contain queries for DuckDB driver to execute are here.

7. **`input`:** This directory contains synthetic graph data used as input for the experiments. It is organized by logic programming environment (`clingo`, `souffle`, `xsb`), and within each environment directory, further subdivided by graph type (e.g., `complete`, `cycle`). Each graph type directory contains `.lp`, `.P`, or `.dl` files corresponding to different graph sizes.

8. **`mariadb_rules`:** Same with `cockroachdb_rules` but for MariaDB and `mysqlclient` driver used.

9. **`mongodb_rules`:** Same with `cockroachdb_rules` but for MongoDB and `pymongo` driver used.

10. **`neo4j_rules`:** Same with `cockroachdb_rules` but for Neo4J and `neo4j` driver used.

11. **`output`:** Contains LaTeX files generated by `generate_plot_table.py`, which produce plots and tables for the performance data. Each `.tex` file corresponds to a specific logic programming environment and contains plots and tables for all graph types analyzed.

12. **playground:** The folder that holds playground files for testing some integration before final integration with the entire system.

13. **`postgres_rules`:** Same with `cockroachdb_rules` but for PostgreSQL.

14. **`souffle_rules`:** Contains rule files for the Souffle environment. The `.dl` (Datalog) files within this directory define transitive closure rules for double recursion, left recursion, and right recursion, adhering to Souffle's syntax. It also contains a C++ file, `souffle_export/main.cpp`, that uses [souffle interface][1] to time facts loading, program run, and result writing.

15. **`tests`:** Some automated test files are located here.

16. **`timing`:** Stores the output CSV files from running the experiments. Like the `input` directory, it is organized by logic programming environment and graph type. Each graph type directory contains CSV files for each recursion mode and graph size, detailing the timing results of the experiments. The output of the query is also in this directory. They are in the `timing/<rule_system>/<graph_type>/<tc_variant>/<size>/` directory.

17. **uml:** This directory holds the programs that generated system architectures.

18. **`xsb_rules`:** Houses rule files for the XSB Prolog environment. The `.P` files define transitive closure rules in XSB syntax for double recursion, left recursion, and right recursion. It also contains a file, `xsb_export/extfilequery.P`, which exports `external_file_query` predicate for XSB timing and other processes.

19. **`analyze_alda.da`, `analyze_dbs.py` and `analyze_logic_systems.py`:** Analysis Scripts

- **Role:** Executes the transitive closure rules within specific systems (e.g., Clingo, XSB, Souffle, PostgreSQL, MariaDB, DuckDB, Neo4J, CockroachDB and Alda), collecting execution metrics.
- **Designation:** Specialized tools for environment-specific execution and performance measurement, tailored to either work with logic systems (`analyze_logic_systems.py`), database systems (`analyze_dbs.py`) or leverage built-in mechanisms of a Python extension for logic programming (`analyze_alda.da`).

20. **analyze.ipynb and `analyze.py`:** Some scripts that utilize data analysis libraries to analyze the experiment results.

21. **`common.py`:** Contains common classes for the entire system.

22. **`config.json`:** The system's configurations including setting database credentials and specifying the systems to cover can be found here.

23. **`generate_db.py`:** Graph Database Generator

- **Role:** It generates synthetic graph databases of specified sizes and types, which serve as input for the experiments.
- **Designation:** Provides the necessary input data for evaluating the performance of transitive closure rules across different graph structures.

24. **`generate_plot_table.py`:** Plot Generation Script

- **Role:** Generates LaTeX files for creating plots and tables that visually represent the performance data collected from rule systems.
- **Designation:** Facilitates the visualization of experiment outcomes, allowing for a graphical and tabular comparison of execution times across different rule systems, graph types, and rule modes.

25. **`requirements.txt`:** Contains all the Python packages which should be installed to run the entire system smoothly. There are some development dependencies that are not required.

26. **`transitive.py`:** Central Experiment Orchestrator

- **Role:** Manages the entire lifecycle of transitive closure rule evaluation experiments across various logic programming environments.
- **Designation:** Acts as the main entry point for initiating experiments, orchestrating the data generation, execution of analysis scripts, and the aggregation and visualization of results.

## Running locally

Clone this repository into any directory of choice (it is assumed you have `git` installed) and change directory into `trans-bench`, the repository's name:

```sh
git clone https://github.com/Sirneij/trans-bench.git && cd trans-bench
```

To run locally, you need to ensure the concerned systems are properly installed and set up.

### Step 1: Set up systems

#### Alda

Ensure `XSB` is accessible in your system's `PATH` with `xsb` as its name or alias. Integrate Alda's source files into your development environment by setting the `PYTHONPATH`:

```shell
(virtualenv) ➜  trans-bench git:(main) ✗ export PYTHONPATH=$HOME/path/to/project/alda:${PYTHONPATH}
```

Replace `/path/to/project/` with the actual path to the Alda directory within the project.

#### Souffle

The version of souffle used require C++ compiler (preferably `g++`) that supports `C++17`. Without this, this program won't run for Souffle. You are required to provide the `PATH` where souffle's `include` folder is located.

##### MacOS

On MacOS, if souffle was installed with `Homebrew`, you can get where souffle is installed by:

```sh
(virtualenv) ➜  trans-bench git:(main) ✗ brew info souffle
```

This was an output:

```sh
==> souffle: stable 2.4.1 (bottled)
Logic Defined Static Analysis
https://souffle-lang.github.io
Installed
/opt/homebrew/Cellar/souffle/HEAD-c7ce229 (105 files, 16.2MB) *
  Built from source on 2024-04-29 at 19:31:13
From: https://github.com/Homebrew/homebrew-core/blob/HEAD/Formula/s/souffle.rb
License: UPL-1.0
==> Dependencies
Build: bison ✔, cmake ✔, mcpp ✔, pkg-config ✔
==> Requirements
Required: macOS >= 10.15 (or Linux) ✔
==> Analytics
install: 10 (30 days), 49 (90 days), 199 (365 days)
install-on-request: 10 (30 days), 49 (90 days), 199 (365 days)
build-error: 0 (30 days)
```

which means it was installed in `/opt/homebrew/Cellar/souffle/HEAD-c7ce229`. As a result, the `include` folder would be `/opt/homebrew/Cellar/souffle/HEAD-c7ce229/include`.

#### PostgreSQL

First, install PostgreSQL based on your operating system. Then set up a database and fill in the credentials in `config.json`:

```json
{
    "postgres": {
        "dbURL": "postgres://<db_username>:<db_user_password>@<host>:<port>/<db_name>"
    },
    ...
}
```

Substitute `<db_username>:<db_user_password>@<host>:<port>/<db_name>` with their real values based on your set up.

#### MariaDB

Just like PostgreSQL, fill in MariaDB's database's credentials in `config.json`:

```json
{
    ...
    "mariadb": {
        "host": "<host>",
        "user": "<db_user>",
        "password": "<db_user_password>",
        "database": "<db_name>",
        "port": <port>
    },
    ...
}
```

_NOTE:_ There is an issue with mariadb that prevents programs from writing into any directory of choice even after `secure_file_priv = ""` was set in `~/.my.cnf` or `/etc/mysql/my.cnf` in Ubuntu (`~/.my.ini` works for Windows) but could write into `/tmp/` so the results were temporarily written in `/tmp/mariadb_results.csv` and thereafter move it into the desired directory using `sudo` priviledges. As a result, some lines were added to `analyze_dbs.py::AnalyzeDBs::solve_with_mariadb` method for safe-keeping. You may still be required to insert your user password while the analysis is going on as a result.

#### Neo4J

As with other systems, first [install Neo4J][3] on your machine, set it up and fill in its credentials in `config.json`:

```json
{
    ...
    "neo4j": {
        "uri": " neo4j://localhost:7687",
        "user": "<user>",
        "password": "<user_password>",
        "import_directory": "<neo4j_import_directory>"
    },
    ...
}
```

This project uses [`apoc`][4], a Neo4J plugin which "provides access to user-defined procedures and functions which extend the use of the Cypher query language into areas such as data integration, graph algorithms, and data conversion", to export query results to CSV. Kindly set it up too.

#### MongoDB

Just install MongoDB and fill in the details in `config.json`.

#### CockroachDB

Having [installed CockroachDB][1], you must [run the `cockroach start-single-node` command][2] before starting the benchmarking tool:

```sh
cockroach start-single-node --advertise-addr 'localhost' --insecure
```

### Step 2: Install dependencies

The project uses each system's recommended database connectors to avoid the overhead posed by their CLI in connecting to a database each time a query is to be executed. It's recommended to install these dependencies in a virtual environment:

```sh
➜  trans-bench git:(main) ✗ python3 -m venv virtualenv
➜  trans-bench git:(main) ✗ source virtualenv/bin/activate
(virtualenv) ➜  trans-bench git:(main) ✗ pip install -r requirements.txt
```

### Step 3 (Optional): Data Generation

Though `transitive.py` automates data generation, manual execution is available:

```shell
(virtualenv) ➜  trans-bench git:(main) ✗ python generate_db.py --py
```

For custom graph sizes, use the `--sizes` flag as demonstrated:

```shell
(virtualenv) ➜  trans-bench git:(main) ✗ python generate_db.py --sizes 100 601 100 --py
```

### Step 4: Run command

```sh
(virtualenv) ➜  trans-bench git:(main) ✗ python transitive.py --size-range 50 401 50 --modes left_recursion right_recursion --environments xsb postgres mariadb duckdb cockroachdb neo4j mongodb --num-runs 2
```

### Step 5 (Optional): Run automated tests

Some tests were written to verify the functionalities of the benchmark suite. You can run them via:

```sh
(virtualenv) ➜  trans-bench git:(main) ✗ python -m unittest discover -s tests
```

## Extension

[1]: https://www.cockroachlabs.com/docs/v24.1/install-cockroachdb "Install CockroachDB"
[2]: https://www.cockroachlabs.com/docs/stable/build-a-python-app-with-cockroachdb?filters=local "Step 1. Start CockroachDB"
[3]: https://neo4j.com/docs/operations-manual/current/installation/ "Installing Neo4J"
[4]: https://neo4j.com/docs/apoc/current/installation/ "Installing APOC"
