-- The left recursion script was adapted from https://gitlab.informatik.uni-halle.de/brass/rbench/-/blob/master/pg/tcff_2.sql?ref_type=heads, the git repository for:

-- Brass, Stefan, and Mario Wenzel. "Performance Analysis and Comparison of Deductive Systems and SQL Databases." Datalog. 2019.

-- Modifications to the original script:
--    - Changed table name from `par` to `edge`
--    - Made the path to the data source dynamic
--    - Created table for storing results so that they could be written to file later
--    - Used `SELECT *` instead of `SELECT count(*)`
--    - Dump the results to a file

CREATE TABLE edge(x INTEGER NOT NULL, y INTEGER NOT NULL);

\COPY edge FROM '{data_file}';

CREATE INDEX edge_yx ON edge(y, x);

ANALYZE edge;

CREATE TABLE tc_result AS
WITH RECURSIVE tc AS (
    SELECT edge.x, edge.y
    FROM edge
    UNION
    SELECT edge.x, tc.y
    FROM edge, tc
    WHERE edge.y = tc.x
)
SELECT * FROM tc;

\COPY (SELECT * FROM tc_result) TO '{output_file}' CSV HEADER;
