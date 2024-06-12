CREATE TEMPORARY TABLE edge(x INT NOT NULL, y INT NOT NULL);
LOAD DATA LOCAL INFILE '{data_file}' INTO TABLE edge FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' (x, y);
CREATE INDEX edge_yx ON edge(y, x);
ANALYZE TABLE edge;
CREATE TEMPORARY TABLE tc_result AS
WITH RECURSIVE tc AS (
    SELECT edge.x, edge.y
    FROM edge
    UNION
    SELECT edge.x, tc.y
    FROM edge, tc
    WHERE edge.y = tc.x
)
SELECT * FROM tc;
SELECT * FROM tc_result INTO OUTFILE '/tmp/test_mariadb.csv' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';
