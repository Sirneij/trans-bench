import csv
from typing import Any

import MySQLdb

from common import Base

# SingleStore only accepts `UNION ALL` between the base and the recursive term and rejects
# DISTINCT/UNION/GROUP BY inside the recursive term, so duplicates cannot be removed between
# iterations. The transitive closure is therefore computed as the set of pairs connected by a
# path, `SELECT DISTINCT` over all paths (bag semantics), which terminates only on acyclic
# graphs. The iteration limit (default 32) is raised so that paths of up to 10,000 edges can be
# followed; on cyclic graphs the query then fails with error 2741 (iteration limit exceeded).
MAX_RECURSIVE_CTE_ITERATIONS = 10000


class SingleStoreOperations(Base):
    def __init__(self, config: dict[str, Any], conn: MySQLdb.Connection) -> None:
        self.config = config
        self.conn = conn
        self.conn.autocommit(True)

    def execute_query(self, query: str, params: Any = None) -> None:
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        cursor.fetchall()

    def set_iteration_limit(self) -> None:
        self.execute_query(f'SET SESSION max_recursive_cte_iterations = {MAX_RECURSIVE_CTE_ITERATIONS};')

    def create_tc_path_table(self) -> None:
        self.execute_query('CREATE TABLE edge (x INT NOT NULL, y INT NOT NULL);')

    def import_data_from_file(self, table_name: str, file_path: str, delimiter: str = '\t') -> None:
        self.execute_query(
            f"LOAD DATA LOCAL INFILE '{file_path}' INTO TABLE {table_name} "
            f"FIELDS TERMINATED BY '{delimiter}' LINES TERMINATED BY '\\n' (x, y);"
        )

    def create_tc_path_index(self) -> None:
        self.execute_query('CREATE INDEX edge_yx ON edge (y, x);')

    def analyze_tc_path_table(self) -> None:
        self.execute_query('ANALYZE TABLE edge;')

    def drop_tc_path_tc_result_tables(self) -> None:
        self.execute_query('DROP TABLE IF EXISTS tc_result;')
        self.execute_query('DROP TABLE IF EXISTS edge;')

    def export_data_to_file(self, output_file) -> None:
        # SELECT ... INTO OUTFILE would write inside the server container, so the result is
        # fetched through the client connection and written locally.
        cursor = self.conn.cursor()
        cursor.execute('SELECT x, y FROM tc_result;')
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['x', 'y'])
            writer.writerows(cursor.fetchall())
