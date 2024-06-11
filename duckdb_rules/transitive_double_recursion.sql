CREATE TABLE tc_path (x INTEGER, y INTEGER);
COPY tc_path FROM '{data_file}' (DELIMITER '\t');
CREATE INDEX tc_path_yx ON tc_path (y, x);
ANALYZE;
CREATE TABLE tc_result AS
WITH RECURSIVE tc AS (
    SELECT tc_path.x, tc_path.y
    FROM tc_path
    UNION
    SELECT tc1.x, tc2.y
    FROM tc tc1, tc tc2
    WHERE tc1.y = tc2.x
)
SELECT * FROM tc;
COPY (SELECT * FROM tc_result) TO '{output_file}' WITH (HEADER, DELIMITER ',');