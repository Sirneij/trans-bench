-- This script was adapted from https://gitlab.informatik.uni-halle.de/brass/rbench/-/blob/master/pg/tcff_2.sql?ref_type=heads, the git repository for:

-- Brass, Stefan, and Mario Wenzel. "Performance Analysis and Comparison of Deductive Systems and SQL Databases." Datalog. 2019.

-- Modifications:
-- 1. Removed dropping table because I now use temporary which drops automatically and has improved load time (see the reference).
-- 2. Changed table name from `par` to `tc_path`
-- 3. Made the path to the data source dynamic
-- 4. Created temporary table for storing results so that they could be written to file later
-- 5. Used `SELECT *` instead of `SELECT count(*)`
-- 6. Dump the results to a file.

\timing ON

-- Create temporary table
CREATE TEMPORARY TABLE tc_path(x INTEGER NOT NULL, y INTEGER NOT NULL);

-- Load data from the specified file
\echo 'LOAD DATA'
\COPY tc_path FROM 'edge.facts';

-- Create index to speed up query execution
CREATE INDEX tc_path_yx ON tc_path(y, x);

-- Analyze the table to improve query performance
ANALYZE tc_path;

-- Execute the recursive query and store results in a temporary table
\echo 'EXECUTE QUERY'
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

-- Measure the time taken to write the results to a file
\echo 'WRITE RESULT'
\COPY (SELECT * FROM tc_result) TO 'test_pg.csv' CSV HEADER;
