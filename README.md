# Benchmarking experiments for logic rule and database systems

### PostgreSQL

### MariaDB

Ensure you fill in database credentials of MariaDB.

*NOTE:* There is an issue with mariadb that prevents programs from writing into any directory of choice even after `secure_file_priv = ""` was set in `~/.my.cnf` or `/etc/mysql/my.cnf` in Ubuntu (`~/.my.ini` works for Windows) but could write into `/tmp/` so the results were temporarily written in `/tmp/mariadb_results.csv` and thereafter move it into the desired directory using `sudo` priviledges. As a result, some lines were added to `analyze_dbs.py::AnalyzeDBs::solve_with_mariadb` method for safe-keeping.

### CockroachDB

Having [installed CockroachDB][1], you must [run the `cockroach start-single-node` command][2] before starting the benchmarking tool:

```sh
cockroach start-single-node --advertise-addr 'localhost' --insecure
```

### MemDB or MemSQL (SignleStore)

You need to [install and obtain a free license][3].


[1]: https://www.cockroachlabs.com/docs/v24.1/install-cockroachdb "Install CockroachDB"
[2]: https://www.cockroachlabs.com/docs/stable/build-a-python-app-with-cockroachdb?filters=local "Step 1. Start CockroachDB"
[3]: https://docs.singlestore.com/db/v7.8/deploy/linux/ciab-cli-online-deb/ "Cluster-in-a-Box CLI Online Deployment - Debian Distribution"