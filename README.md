# Benchmarking experiments for logic rule and database systems

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