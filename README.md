# Benchmarking experiments for logic rule and database systems

## Running locally

To run locally, you need to ensure the concerned systems are properly installed and set up.

### Step 1: Set up systems

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

*NOTE:* There is an issue with mariadb that prevents programs from writing into any directory of choice even after `secure_file_priv = ""` was set in `~/.my.cnf` or `/etc/mysql/my.cnf` in Ubuntu (`~/.my.ini` works for Windows) but could write into `/tmp/` so the results were temporarily written in `/tmp/mariadb_results.csv` and thereafter move it into the desired directory using `sudo` priviledges. As a result, some lines were added to `analyze_dbs.py::AnalyzeDBs::solve_with_mariadb` method for safe-keeping. You may still be required to insert your user password while the analysis is going on as a result.

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

### Step 3: Run command

```sh
(virtualenv) ➜  trans-bench git:(main) ✗ python transitive.py --size-range 50 401 50 --modes left_recursion right_recursion --environments xsb postgres mariadb duckdb cockroachdb neo4j mongodb --num-runs 2
```

### Step 4 (Optional): Run automated tests

Some tests were written to verify the functionalities of the benchmark suite. You can run them via:

```sh
(virtualenv) ➜  trans-bench git:(main) ✗ python -m unittest discover -s tests
```

[1]: https://www.cockroachlabs.com/docs/v24.1/install-cockroachdb "Install CockroachDB"
[2]: https://www.cockroachlabs.com/docs/stable/build-a-python-app-with-cockroachdb?filters=local "Step 1. Start CockroachDB"
[3]: https://neo4j.com/docs/operations-manual/current/installation/ "Installing Neo4J"
[4]: https://neo4j.com/docs/apoc/current/installation/ "Installing APOC"