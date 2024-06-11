MATCH (n) DETACH DELETE n;

LOAD CSV FROM "file:///{data_file}" AS line FIELDTERMINATOR '\t'
MERGE (a:Node {n: toInteger(line[0])})
MERGE (b:Node {n: toInteger(trim(line[1]))})
CREATE (a)-[:PAR]->(b);


CREATE INDEX IF NOT EXISTS FOR (n:Node) ON (n.x);
CREATE INDEX IF NOT EXISTS FOR (n:Node) ON (n.y);

MATCH (a)-[*]->(b)
RETURN COUNT(DISTINCT [a,b]);


