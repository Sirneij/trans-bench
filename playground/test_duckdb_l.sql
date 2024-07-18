CREATE TABLE edge (x INTEGER, y INTEGER);
COPY edge FROM '{data_file}' (DELIMITER '\t');
CREATE INDEX edge_yx ON edge (y, x);
CREATE TABLE tc_result AS
WITH RECURSIVE tc AS (
    SELECT x, y FROM edge
    UNION
    SELECT tc.x, edge.y FROM tc JOIN edge ON tc.y = edge.x
)
SELECT * FROM tc;
COPY (SELECT * FROM tc_result) TO '{output_file}' WITH (HEADER, DELIMITER ',');