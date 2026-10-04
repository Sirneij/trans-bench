// Template: transitive closure in Cypher, for Neo4j.
//
// The statements are separated by semicolons, so no comment may contain one. All statements but the
// last two prepare the graph, the second to last is the timed query, and the last exports the
// result. The connector replaces {data_file} and {output_file}. The pattern [:EDGE*1..] has no upper
// bound, since a bound would leave out the pairs joined only by longer paths. Cypher has one
// formulation of the closure, so the files of the three modes are the same.

MATCH (n) DETACH DELETE n;

LOAD CSV FROM "file:///{data_file}" AS line FIELDTERMINATOR '\t'
MERGE (a:Node {id: toInteger(line[0])})
MERGE (b:Node {id: toInteger(trim(line[1]))})
CREATE (a)-[:EDGE]->(b);

CREATE INDEX IF NOT EXISTS FOR (n:Node) ON (n.id);

MATCH (start:Node)-[:EDGE*1..]->(end:Node)
WITH DISTINCT start.id AS x, end.id AS y
RETURN count(*) AS pairs;

CALL apoc.export.csv.query(
    "MATCH (start:Node)-[:EDGE*1..]->(end:Node) RETURN DISTINCT start.id AS x, end.id AS y",
    "{output_file}",
    {}
)
YIELD file, nodes, relationships, properties, time, rows, batchSize, batches, done, data
RETURN file, rows;
