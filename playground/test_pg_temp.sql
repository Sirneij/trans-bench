-- This script was adapted from https://gitlab.informatik.uni-halle.de/brass/rbench/-/blob/master/pg/tcff_2.sql?ref_type=heads, the git repository for:

-- Brass, Stefan, and Mario Wenzel. "Performance Analysis and Comparison of Deductive Systems and SQL Databases." Datalog. 2019.

-- Modifications:
-- 1. Removed dropping table because I now use temporary which drops automatically and has improved load time (see the reference).
-- 2. Changed table name from `par` to `edge`
-- 3. Made the path to the data source dynamic
-- 4. Created temporary table for storing results so that they could be written to file later
-- 5. Used `SELECT *` instead of `SELECT count(*)`
-- 6. Dump the results to a file.

\timing ON

-- Create temporary table
CREATE TEMPORARY TABLE edge(x INTEGER NOT NULL, y INTEGER NOT NULL);

-- Load data from the specified file
\echo 'LOAD DATA'
\COPY edge FROM 'edge.facts';

-- Create index to speed up query execution
CREATE INDEX edge_yx ON edge(y, x);

-- Analyze the table to improve query performance
ANALYZE edge;

-- Execute the recursive query and store results in a temporary table
\echo 'EXECUTE QUERY'
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

-- Measure the time taken to write the results to a file
\echo 'WRITE RESULT'
\COPY (SELECT * FROM tc_result) TO 'test_pg.csv' CSV HEADER;
