# Benchmarking experiments for logic rule and database systems

### PostgreSQL

### MariaDB

Ensure you fill in database credentials of MariaDB.

*NOTE:* There is an issue with mariadb that prevents programs from writing into any directory of choice even after `secure_file_priv = ""` was set in `~/.my.cnf` or `/etc/mysql/my.cnf` in Ubuntu (`~/.my.ini` works for Windows) but could write into `/tmp/` so the results were temporarily written in `/tmp/mariadb_results.csv` and thereafter move it into the desired directory using `sudo` priviledges. As a result, some lines were added to `analyze_dbs.py::AnalyzeDBs::solve_with_mariadb` method for safe-keeping. You may still be required to insert your user password while the analysis is going on as a result.

### CockroachDB

Having [installed CockroachDB][1], you must [run the `cockroach start-single-node` command][2] before starting the benchmarking tool:

```sh
cockroach start-single-node --advertise-addr 'localhost' --insecure
```

## Run command

```sh
python transitive.py --size-range 50 401 50 --modes left_recursion right_recursion --environments xsb postgres mariadb duckdb cockroachdb neo4j mongodb --num-runs 2
```

```sh
python -m unittest discover -s tests
```

[1]: https://www.cockroachlabs.com/docs/v24.1/install-cockroachdb "Install CockroachDB"
[2]: https://www.cockroachlabs.com/docs/stable/build-a-python-app-with-cockroachdb?filters=local "Step 1. Start CockroachDB"