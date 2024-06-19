MATCH (n) DETACH DELETE n;

LOAD CSV FROM "file:///edge.facts" AS line FIELDTERMINATOR '\t'
MERGE (a:Node {x: toInteger(line[0])})
MERGE (b:Node {y: toInteger(trim(line[1]))})
CREATE (a)-[:CONNECTED_TO]->(b);

CREATE INDEX IF NOT EXISTS FOR (n:Node) ON (n.x);
CREATE INDEX IF NOT EXISTS FOR (n:Node) ON (n.y);

MATCH (start:Node)-[:CONNECTED_TO*]->(end:Node)
RETURN start.x AS startX, end.y AS endY;

CALL apoc.export.csv.query(
    "MATCH (start:Node)-[:CONNECTED_TO*]->(end:Node) RETURN start.x AS startX, end.y AS endY",
    "/home/sirneij/temp/trans-bench/timing/neo4j/complete/left_recursion/5/test_neo4j.csv",
    {}
)
YIELD file, nodes, relationships, properties, time, rows, batchSize, batches, done, data
RETURN file, rows;
