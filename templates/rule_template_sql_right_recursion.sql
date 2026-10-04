-- Template: transitive closure with right recursion, in SQL.
--
-- The recursive term puts one edge in front of every known path, as in the rule
-- path(X, Y) :- edge(X, Z), path(Z, Y). UNION, not UNION ALL, removes the duplicates of every
-- iteration, so the query also ends on graphs with cycles.
--
-- For DuckDB, a rule file is a whole script, one statement per timing phase (systems/duckdb/rules/).
-- For PostgreSQL, MariaDB and CockroachDB, the statement below is the body of run_recursive_query()
-- in a Python class (systems/postgres/rules/). The table edge has the columns x and y.

CREATE TABLE tc_result AS
WITH RECURSIVE tc AS (
    SELECT x, y FROM edge
    UNION
    SELECT edge.x, tc.y FROM edge JOIN tc ON edge.y = tc.x
)
SELECT * FROM tc;
