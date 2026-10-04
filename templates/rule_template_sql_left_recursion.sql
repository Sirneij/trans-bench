-- Template: transitive closure with left recursion, in SQL.
--
-- The recursive term extends every known path by one edge at its end, as in the rule
-- path(X, Y) :- path(X, Z), edge(Z, Y). UNION, not UNION ALL, removes the duplicates of every
-- iteration, so the query also ends on graphs with cycles.
--
-- For DuckDB, a rule file is a whole script, one statement per timing phase (systems/duckdb/rules/).
-- For PostgreSQL, MariaDB and CockroachDB, the statement below is the body of run_recursive_query()
-- in a Python class (systems/postgres/rules/). The table edge has the columns x and y.

CREATE TABLE tc_result AS
WITH RECURSIVE tc AS (
    SELECT x, y FROM edge
    UNION
    SELECT tc.x, edge.y FROM tc JOIN edge ON tc.y = edge.x
)
SELECT * FROM tc;
