"""
Find the pairs missing from MariaDB's transitive closure, and test whether max_recursive_iterations explains them.

The paper discusses the results in Section 4.4; they are in results/verified_2026/mariadb_investigation.jsonl.

Usage, from the repository root, with only MariaDB running:
    python scripts/investigate_mariadb.py <graph> <n> <left|right> [max_recursive_iterations]
Environment: STD_CTE=0|1 (standard_compliant_cte, default 0 as in the benchmark), TMP_BYTES=<bytes>
(session tmp_table_size and max_heap_table_size), MARIADB_SOCKET (default /tmp/mysql.sock),
MARIADB_DATABASE (default benchmark); connects as the current OS user, as Homebrew's MariaDB allows.
"""

import getpass
import json
import os
import sys
import time
from collections import defaultdict, deque

import duckdb
import MySQLdb

graph, n, mode = sys.argv[1], int(sys.argv[2]), sys.argv[3]
max_iter = int(sys.argv[4]) if len(sys.argv) > 4 else None
edge_file = os.path.abspath(f'input/souffle/{graph}/{n}/edge.facts')
rec = {
    'left': 'SELECT tc.x, edge.y FROM tc JOIN edge ON tc.y = edge.x',
    'right': 'SELECT edge.x, tc.y FROM edge JOIN tc ON edge.y = tc.x',
}[mode]
# The SQL below is built from this script's own constants and the graph name and size given on the
# command line by the person running it (nosec B608 on those lines).
# INTO OUTFILE is written by the server, so the file must be in a directory the server can write
OUT_FILE = '/tmp/mariadb_investigate.csv'  # nosec B108
if os.path.exists(OUT_FILE):
    os.remove(OUT_FILE)

c = MySQLdb.connect(
    db=os.environ.get('MARIADB_DATABASE', 'benchmark'),
    user=getpass.getuser(),
    unix_socket=os.environ.get('MARIADB_SOCKET', '/tmp/mysql.sock'),  # nosec B108  # Homebrew's socket
    local_infile=1,
)
c.autocommit(True)
cur = c.cursor()
cur.execute(f"SET @@standard_compliant_cte={os.environ.get('STD_CTE', '0')}")  # harness uses 0
if os.environ.get('TMP_BYTES'):
    cur.execute(f"SET @@tmp_table_size={os.environ['TMP_BYTES']}")
    cur.execute(f"SET @@max_heap_table_size={os.environ['TMP_BYTES']}")
if max_iter:
    cur.execute(f'SET @@max_recursive_iterations={max_iter}')
cur.execute('SELECT @@max_recursive_iterations, @@standard_compliant_cte, @@tmp_table_size')
settings = cur.fetchone()
cur.execute('DROP TABLE IF EXISTS edge, tc_result')
cur.execute('CREATE TABLE edge (x INTEGER NOT NULL, y INTEGER NOT NULL)')
cur.execute(
    f"LOAD DATA LOCAL INFILE '{edge_file}' INTO TABLE edge "
    "FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' (x, y)"
)
cur.execute('CREATE INDEX edge_yx ON edge (y, x)')
cur.execute('ANALYZE TABLE edge')
cur.fetchall()
t = time.perf_counter()
cur.execute(
    f'CREATE TABLE tc_result AS WITH RECURSIVE tc AS (SELECT x, y FROM edge UNION {rec}) SELECT * FROM tc'  # nosec B608
)
elapsed = time.perf_counter() - t
cur.execute('SHOW WARNINGS')
warnings = cur.fetchall()
cur.execute('SELECT COUNT(*) FROM tc_result')
mariadb_count = cur.fetchone()[0]
cur.execute(
    f"SELECT * FROM tc_result INTO OUTFILE '{OUT_FILE}' "  # nosec B608
    "FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'"
)
cur.execute('DROP TABLE IF EXISTS edge, tc_result')
c.close()

# Reference closure (DuckDB, whose results on this input match the Python ground truth and all other
# systems), and the pairs that MariaDB did not return.
d = duckdb.connect()
COLUMNS = "columns={'x':'INTEGER','y':'INTEGER'}"
d.execute(
    f"CREATE TABLE edge AS SELECT * FROM read_csv('{edge_file}', delim='\t', header=false, {COLUMNS})"  # nosec B608
)
d.execute(
    f'CREATE TABLE ref AS WITH RECURSIVE tc AS (SELECT x, y FROM edge UNION {rec}) SELECT * FROM tc'  # nosec B608
)
d.execute(f"CREATE TABLE got AS SELECT * FROM read_csv('{OUT_FILE}', delim=',', header=false, {COLUMNS})")  # nosec B608
ref_count = d.execute('SELECT COUNT(*) FROM ref').fetchone()[0]
missing = d.execute('SELECT x, y FROM ref EXCEPT SELECT x, y FROM got ORDER BY 1, 2').fetchall()
extra = d.execute('SELECT x, y FROM got EXCEPT SELECT x, y FROM ref ORDER BY 1, 2').fetchall()
dups = d.execute('SELECT COUNT(*) - COUNT(DISTINCT (x, y)) FROM got').fetchone()[0]

# Shortest-path distance of each missing pair (= semi-naive iteration in which it is first derived).
adj = defaultdict(list)
for a, b in d.execute('SELECT DISTINCT x, y FROM edge').fetchall():
    adj[a].append(b)


def dist(source, target):
    """Return the length of the shortest path from source to target (breadth-first), or None."""
    seen, q = {source: 0}, deque([source])
    while q:
        v = q.popleft()
        for w in adj[v]:
            if w not in seen:
                seen[w] = seen[v] + 1
                if w == target:
                    return seen[w]
                q.append(w)
    return None


diameter_hint = max((dist(x, y) or 0) for x, y in missing[:20]) if missing else None
print(
    json.dumps(
        {
            'graph': graph,
            'n': n,
            'mode': mode,
            'settings': {
                'max_recursive_iterations': settings[0],
                'standard_compliant_cte': settings[1],
                'tmp_table_size': settings[2],
            },
            'query_s': round(elapsed, 1),
            'warnings': [list(w) for w in warnings][:5],
            'mariadb_rows': mariadb_count,
            'reference_rows': ref_count,
            'missing': [[x, y, dist(x, y)] for x, y in missing[:20]],
            'n_missing': len(missing),
            'n_extra': len(extra),
            'duplicate_rows': dups,
        }
    )
)
