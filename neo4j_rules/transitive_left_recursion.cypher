MATCH (n) DETACH DELETE n;

LOAD CSV FROM "file:///{data_file}" AS line FIELDTERMINATOR '\t'
CREATE (a:Node {x: toInteger(line[0]), y: toInteger(line[1])})
WITH a
MERGE (b:Node {x: a.x})
MERGE (c:Node {y: a.y})
CREATE (b)-[:CONNECTED_TO]->(c);

CREATE INDEX IF NOT EXISTS FOR (n:Node) ON (n.x);
CREATE INDEX IF NOT EXISTS FOR (n:Node) ON (n.y);

MATCH path = (start:Node)-[:CONNECTED_TO*]->(end:Node)
RETURN start.x AS startX, end.y AS endY
ORDER BY startX, endY;
