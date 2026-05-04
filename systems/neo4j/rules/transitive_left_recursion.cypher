MATCH (n) DETACH DELETE n;

LOAD CSV FROM "file:///{data_file}" AS line FIELDTERMINATOR '\t'
MERGE (a:Node {id: toInteger(line[0])})
MERGE (b:Node {id: toInteger(trim(line[1]))})
CREATE (a)-[:EDGE]->(b);

CREATE INDEX IF NOT EXISTS FOR (n:Node) ON (n.id);

MATCH (start:Node)-[:EDGE*1..]->(end:Node) 
RETURN DISTINCT start.id AS x, end.id AS y;

CALL apoc.export.csv.query(
    "MATCH (start:Node)-[:EDGE*1..]->(end:Node) RETURN DISTINCT start.id AS x, end.id AS y",
    "{output_file}",
    {}
)
YIELD file, nodes, relationships, properties, time, rows, batchSize, batches, done, data
RETURN file, rows;