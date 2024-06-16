MATCH (n) DETACH DELETE n;

LOAD CSV FROM "file:///{data_file}" AS line FIELDTERMINATOR '\t'
MERGE (a:Node {x: toInteger(line[0])})
MERGE (b:Node {y: toInteger(trim(line[1]))})
CREATE (a)-[:CONNECTED_TO]->(b);

CREATE INDEX IF NOT EXISTS FOR (n:Node) ON (n.x);
CREATE INDEX IF NOT EXISTS FOR (n:Node) ON (n.y);

MATCH path = (end:Node)<-[:CONNECTED_TO*]-(start:Node)
RETURN start.x AS startX, end.y AS endY;
