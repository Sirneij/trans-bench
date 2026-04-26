# Empirical Findings on Transitive Closure Computation for Scale-Free Graphs

## 1. Execution Time Analysis (PostgreSQL)
We evaluated the query execution times (`ExecuteQueryRealTime`) for computing transitive closure on scale-free graphs using PostgreSQL, testing graph sizes from 10,000 to 60,000 nodes. The findings demonstrate a non-linear, aggressive increase in execution time as graph size scales, establishing a clear computational bottleneck for traditional relational database systems on recursive workloads.

* **Left Recursion:** Execution time grew from **8.96s** (10k nodes) to **1095.05s** (60k nodes).
* **Right Recursion:** Execution time grew from **8.09s** (10k nodes) to **718.51s** (60k nodes).

**Observation:** Right recursion consistently outperforms left recursion in PostgreSQL for scale-free graphs. However, both strategies exhibit severe temporal degradation. By $N=60,000$, query times reach 12–18 minutes, making PostgreSQL computationally intractable for significantly larger scale-free datasets.

## 2. Temporal Efficiency vs. Memory Constraints (DuckDB Comparison)
By contrast, DuckDB exhibits superior temporal efficiency for the same workloads. For instance, at 60,000 nodes, DuckDB executes left recursion in **~25.7s** and right recursion in **~25.3s**, operating orders of magnitude faster than PostgreSQL. 

Despite its execution speed, DuckDB suffers from a fundamental structural limitation: **memory exhaustion**.

When scaling to $N \ge 90,000$ using right recursion, DuckDB encounters a fatal Out-of-Memory (OOM) error:
> `Out of Memory Error: failed to allocate data of size 8.0 GiB (11.4 GiB/14.3 GiB used)`

**Observation:** While PostgreSQL is bottlenecked by CPU time and execution inefficiency, DuckDB's vectorized execution engine aggressively consumes memory to maintain intermediate states during recursive Common Table Expressions (CTEs). Consequently, DuckDB reaches a hard memory wall before it encounters temporal degradation.

## 3. Conclusion
Our benchmarking of recursive queries on scale-free graphs reveals a distinct dichotomy between disk-oriented and in-memory analytical DBMS architectures:
1. **PostgreSQL** suffers from extreme **temporal constraints**, rendering it impractically slow for large-scale recursive tasks ($>60,000$ nodes), yet it avoids catastrophic memory failure by relying on disk spilling.
2. **DuckDB** offers highly optimized **temporal execution** but is strictly bounded by **memory capacity**, failing completely at sizes $N \ge 90,000$ due to the immense intermediate memory overhead required for recursive joins on dense scale-free networks. 

These findings underscore the trade-offs between execution speed and memory resilience in modern SQL engines when handling deep recursion.



### Neo4j

```sh
2026-04-26 14:05:21,144 - INFO: Analyzing neo4j among the DBs
2026-04-26 14:05:21,798 - INFO: Using rule file: /Users/johnidogun/Documents/projects/trans-bench/neo4j_rules/transitive_right_recursion.cypher, input file: input/souffle/scale_free/30000/edge.facts, timing file: timing/neo4j/scale_free/timing_right_recursion_graph_30000.csv
2026-04-26 14:05:21,798 - INFO: Output folder: timing/neo4j/scale_free/right_recursion/30000
2026-04-26 14:05:21,902 - INFO: CPU time start: 0.461885, CPU time end: 0.464053
2026-04-26 14:05:21,902 - INFO: Command: MATCH (n) DETACH DELETE n;. Time: DeleteDataRealTime, DeleteDataCPUTime
2026-04-26 14:05:22,421 - INFO: CPU time start: 0.464241, CPU time end: 0.466155
2026-04-26 14:05:22,421 - INFO: Command: LOAD CSV FROM "file:///edge.facts" AS line FIELDTERMINATOR '\t'
MERGE (a:Node {id: toInteger(line[0])})
MERGE (b:Node {id: toInteger(trim(line[1]))})
CREATE (a)-[:EDGE]->(b);. Time: LoadDataRealTime, LoadDataCPUTime
2026-04-26 14:05:22,562 - INFO: Received notification from DBMS server: <GqlStatusObject gql_status='03N94', status_description="info: eager operator. The query execution plan contains the 'Eager' operator. 'LOAD CSV' in combination with 'Eager' can consume a lot of memory.", position=None, raw_classification='PERFORMANCE', classification=<NotificationClassification.PERFORMANCE: 'PERFORMANCE'>, raw_severity='INFORMATION', severity=<NotificationSeverity.INFORMATION: 'INFORMATION'>, diagnostic_record={'_classification': 'PERFORMANCE', '_severity': 'INFORMATION', 'OPERATION': '', 'OPERATION_CODE': '0', 'CURRENT_SCHEMA': '/'}> for query: 'LOAD CSV FROM "file:///edge.facts" AS line FIELDTERMINATOR \'\\t\'\nMERGE (a:Node {id: toInteger(line[0])})\nMERGE (b:Node {id: toInteger(trim(line[1]))})\nCREATE (a)-[:EDGE]->(b);'
2026-04-26 14:05:22,565 - INFO: CPU time start: 0.46627, CPU time end: 0.467893
2026-04-26 14:05:22,565 - INFO: Command: CREATE INDEX IF NOT EXISTS FOR (n:Node) ON (n.id);. Time: CreateIndexXRealTime, CreateIndexXCPUTime
2026-04-26 14:05:22,565 - INFO: Received notification from DBMS server: <GqlStatusObject gql_status='00NA0', status_description="note: successful completion - index or constraint already exists. The command 'CREATE RANGE INDEX IF NOT EXISTS FOR (e:Node) ON (e.id)' has no effect. The index or constraint specified by 'RANGE INDEX index_243dda65 FOR (e:Node) ON (e.id)' already exists.", position=None, raw_classification='SCHEMA', classification=<NotificationClassification.SCHEMA: 'SCHEMA'>, raw_severity='INFORMATION', severity=<NotificationSeverity.INFORMATION: 'INFORMATION'>, diagnostic_record={'_classification': 'SCHEMA', '_severity': 'INFORMATION', 'OPERATION': '', 'OPERATION_CODE': '0', 'CURRENT_SCHEMA': '/'}> for query: 'CREATE INDEX IF NOT EXISTS FOR (n:Node) ON (n.id);'
2026-04-26 14:05:22,566 - INFO: CPU time start: 0.468046, CPU time end: 0.468857
2026-04-26 14:05:22,566 - INFO: Query executed: MATCH (start:Node)-[:EDGE*1..]->(end:Node) 
RETURN DISTINCT start.id AS x, end.id AS y;
2026-04-26 14:14:07,045 - ERROR: Last Neo4J export error: {neo4j_code: Neo.TransientError.General.MemoryPoolOutOfMemoryError} {message: The allocation of an extra 2.0 MiB would use more than the limit 3.1 GiB. Currently using 3.1 GiB. dbms.memory.transaction.total.max threshold reached} {gql_status: 51N72} {gql_status_description: error: system configuration or operation exception - memory pool out of memory. Failed to allocate memory in a memory pool. See dbms.memory.transaction.total.max in the neo4j.conf file.}, Query: CALL apoc.export.csv.query(
    "MATCH (start:Node)-[:EDGE*1..]->(end:Node) RETURN DISTINCT start.id AS x, end.id AS y",
    "neo4j_export_71719.csv",
    {}
)
YIELD file, nodes, relationships, properties, time, rows, batchSize, batches, done, data
RETURN file, rows;
2026-04-26 14:14:07,048 - INFO: Timing results saved to: timing/neo4j/scale_free/timing_right_recursion_graph_30000.csv
2026-04-26 14:14:07,068 - ERROR: Error copying Neo4j results: Command 'cp /opt/homebrew/Cellar/neo4j/2026.04.0/libexec/import/neo4j_export_71719.csv timing/neo4j/scale_free/right_recursion/30000/neo4j_results.csv' returned non-zero exit status 1.
```