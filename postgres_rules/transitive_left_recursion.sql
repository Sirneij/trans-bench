-- The left recursion script was adapted from https://gitlab.informatik.uni-halle.de/brass/rbench/-/blob/master/pg/tcff_2.sql?ref_type=heads, the git repository for:

-- Brass, Stefan, and Mario Wenzel. "Performance Analysis and Comparison of Deductive Systems and SQL Databases." Datalog. 2019.

-- Modifications to the original script:
--    - Changed table name from `par` to `tc_path`
--    - Made the path to the data source dynamic
--    - Created table for storing results so that they could be written to file later
--    - Used `SELECT *` instead of `SELECT count(*)`
--    - Dump the results to a file

CREATE TABLE tc_path(x INTEGER NOT NULL, y INTEGER NOT NULL);

\COPY tc_path FROM '{data_file}';

CREATE INDEX tc_path_yx ON tc_path(y, x);

ANALYZE tc_path;

CREATE TABLE tc_result AS
WITH RECURSIVE tc AS (
    SELECT tc_path.x, tc_path.y
    FROM tc_path
    UNION
    SELECT tc_path.x, tc.y
    FROM tc_path, tc
    WHERE tc_path.y = tc.x
)
SELECT * FROM tc;

\COPY (SELECT * FROM tc_result) TO '{output_file}' CSV HEADER;
