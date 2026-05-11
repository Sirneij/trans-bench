-- SQL Template: Left Recursion (bottom-up build)
-- Description: Recursive query that builds results by accumulating predecessors
-- Recursion direction: Z → Y → X (backward chaining)
-- 
-- How to use this template:
-- 1. This pattern is less common in SQL but useful for some analytical queries
-- 2. Ensure your 'edge' table has columns (x, y) with appropriate types
-- 3. This builds the transitive closure by finding all nodes that can reach each target

CREATE TEMP TABLE tc_result AS
WITH RECURSIVE tc AS (
    -- Base case: all direct edges
    SELECT x, y
    FROM edge
    
    UNION ALL
    
    -- Recursive case: find predecessors
    -- For each reachable node, add all nodes that can reach it
    SELECT edge.x, tc.y
    FROM edge
    JOIN tc ON edge.y = tc.x
)
SELECT x, y FROM tc;

-- Validation: Count reachable pairs
SELECT COUNT(*) AS reachable_pairs FROM tc_result;
