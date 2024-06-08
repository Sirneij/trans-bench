# Benchmarking experiments for logic rule and database systems

### MariaDB

Ensure you fill in database credentials of MariaDB.

MariaDB has some limitations when working with reading from and writing to a file. Depending on the OS you installed it, you need to modify `secure_file_priv` to be `""` in your installations `my.cnf` or `my.ini`. Kindly locate this file:

```ini
; /path/to/my.ini or /path/to/my.cnf
[mysqld]
secure_file_priv = ""
```

then, restart MariaDB's server. For Ubuntu, I just did:

```sh
sudo systemctl restart mariadb
```

You can check whether or not it's effected via this (Ubuntu):

```sh
mysql -u root -p -e "SHOW VARIABLES LIKE 'secure_file_priv';"
```
