// Cypher Template: Right Recursion (Neo4j)
// Description: Neo4j pattern matching for transitive closure
// Execution model: Index-aware traversal with variable-length paths
//
// How to use this template:
// 1. Neo4j imports data as nodes and relationships
// 2. Pattern: (source)-[:EDGE*..N]->(target) matches paths of up to N hops
// 3. Adjust ..100 to limit recursion depth (prevents runaway queries)
// 4. Use APOC procedures for additional capabilities if installed

MATCH (start)-[:EDGE*..100]->(end)
WHERE start <> end  // Exclude self-loops if not desired
RETURN DISTINCT start, end
ORDER BY start, end;

// Alternative with aggregation (count reachability stats):
// MATCH (start)-[:EDGE*..100]->(end)
// WHERE start <> end
// RETURN 
//   start,
//   COUNT(DISTINCT end) AS reachable_count,
//   COLLECT(end) AS reachable_nodes
// ORDER BY start;
