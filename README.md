# Benchmarking experiments for logic rule and database systems

### PostgreSQL
`psql -c` was used for interacting with the PostgreSQL server on my machine. And in all its use in this suite, no credentials were passed because when one uses `psql -c` without specifying a database name or password, the behavior depends on the default configuration and environment variables. Here's what will happen:

1. **Environment Variables:**
   - The `psql` command-line utility checks environment variables such as `PGHOST`, `PGPORT`, `PGUSER`, and `PGDATABASE`.
   - If these variables are set, `psql` uses them to connect to the default database.
   - For example:
     - If `PGDATABASE` is set, it connects to that database.
     - If not, it connects to the default database (usually `postgres`).

2. **Default Database:**
   - If no database name is provided explicitly or through environment variables, `psql` connects to the default database.
   - In my case, it executed the queries against the default database.

3. **Authentication:**
   - If no password is provided, `psql` attempts to connect using the operating system user's credentials (peer authentication).
   - If that fails, it falls back to other authentication methods (e.g., password-based authentication).

4. **Local Connection:**
   - If you're running `psql` locally (on the same machine as the PostgreSQL server), it might use local socket connections (Unix domain sockets) instead of network connections.
   - In this case, authentication relies on the operating system user.

**So, if you do not want to modify the code, kindly create database and database user having the same name and password as your machine's username.**


### MariaDB

Ensure you fill in database credentials of MariaDB.

*NOTE:* There is an issue with mariadb that prevents programs from writing into any directory of choice even after `secure_file_priv = ""` was set in `~/.my.cnf` or `/etc/mysql/my.cnf` in Ubuntu (`~/.my.ini` works for Windows) but could write into `/tmp/` so the results were temporarily written in `/tmp/test_mariadb.csv` and thereafter move it into the desired directory using `sudo` priviledges. As a result, these lines:

```py
password = 'Your sudo password'
command = ['sudo', '-S', 'mv', '/tmp/test_mariadb.csv', output_file]
# Run the command and pass the password
subprocess.run(
    command, input=f'{password}\n', text=True, capture_output=True
)
```

were added to the end of `analyze_db.py::AnalyzeDBs::solve_with_mariadb(...)`. Your MariaDB might not be that stubborn.

## Credits

1. The left recursion script was adapted from https://gitlab.informatik.uni-halle.de/brass/rbench/-/blob/master/pg/tcff_2.sql?ref_type=heads, the git repository for:

    *Brass, Stefan, and Mario Wenzel. "Performance Analysis and Comparison of Deductive Systems and SQL Databases." Datalog. 2019.*

    ### Modifications to the original script:
    - Changed table name from `par` to `tc_path`
    - Made the path to the data source dynamic
    - Created table for storing results so that they could be written to file later
    - Used `SELECT *` instead of `SELECT count(*)`
    - Dump the results to a file