-- SQL Template: Right Recursion (top-down build)
-- Description: Recursive query that builds results from base edges outward
-- Recursion direction: X → Y → Z (forward chaining)
-- 
-- How to use this template:
-- 1. Replace 'tc_result' with a meaningful output table name if desired
-- 2. Ensure your 'edge' table has columns (x, y) with appropriate types
-- 3. Adjust the UNION ALL to include additional base/recursive cases if needed
-- 4. Test with: SELECT COUNT(*) FROM tc_result;

CREATE TEMP TABLE tc_result AS
WITH RECURSIVE tc AS (
    -- Base case: all direct edges
    SELECT x, y
    FROM edge
    
    UNION ALL
    
    -- Recursive case: extend paths by one edge
    -- Connect previously found paths to new edges
    SELECT tc.x, edge.y
    FROM tc
    JOIN edge ON tc.y = edge.x
    WHERE tc.x < tc.y  -- Optional: avoid duplicates for symmetric closure
)
SELECT x, y FROM tc;

-- Validation: Count reachable pairs
SELECT COUNT(*) AS reachable_pairs FROM tc_result;
