CREATE TEMPORARY TABLE tc_path(x INT NOT NULL, y INT NOT NULL);
LOAD DATA LOCAL INFILE '{data_file}' INTO TABLE tc_path FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' (x, y);
CREATE INDEX tc_path_yx ON tc_path(y, x);
ANALYZE TABLE tc_path;
CREATE TEMPORARY TABLE tc_result AS
WITH RECURSIVE tc AS (
    SELECT tc_path.x, tc_path.y
    FROM tc_path
    UNION
    SELECT tc_path.x, tc.y
    FROM tc_path, tc
    WHERE tc_path.y = tc.x
)
SELECT * FROM tc;
SELECT * FROM tc_result INTO OUTFILE '/tmp/test_mariadb.csv' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';
