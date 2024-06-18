CREATE TABLE edge (x INTEGER, y INTEGER);
COPY edge FROM '{data_file}' (DELIMITER '\t');
CREATE INDEX edge_yx ON edge (y, x);
ANALYZE;
CREATE TABLE tc_result AS
WITH RECURSIVE tc AS (
    SELECT x, y FROM edge
    UNION
    SELECT tc1.x, tc2.y FROM tc AS tc1, tc AS tc2 WHERE tc1.y = tc2.x
)
SELECT * FROM tc;
COPY (SELECT * FROM tc_result) TO '{output_file}' WITH (HEADER, DELIMITER ',');