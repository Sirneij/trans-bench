CREATE TABLE edge (x INTEGER, y INTEGER);
COPY edge FROM '{data_file}' (DELIMITER '\t');
CREATE INDEX edge_yx ON edge (y, x);
ANALYZE;
CREATE TABLE tc_result AS
WITH RECURSIVE tc AS (
    SELECT x, y FROM edge
    UNION
    SELECT edge.x, tc.y FROM edge JOIN tc ON edge.y = tc.x
)
SELECT * FROM tc;
COPY (SELECT * FROM tc_result) TO '{output_file}' WITH (HEADER, DELIMITER ',');