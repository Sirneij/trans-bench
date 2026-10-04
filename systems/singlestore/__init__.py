"""The steps of a SingleStore run; the rule files add the recursive query (systems/singlestore/rules/)."""

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
    """Create, load, index, analyze and export; the connector times each of these steps."""

    def __init__(self, config: dict[str, Any], conn: MySQLdb.Connection) -> None:
        """Keep the configuration and the connection, which commits every statement."""
        super().__init__(config)
        self.conn = conn
        self.conn.autocommit(True)

    def execute_query(self, query: str, params: Any = None) -> None:
        """Execute one statement and fetch its result, so that it has finished when this returns."""
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        cursor.fetchall()

    def set_iteration_limit(self) -> None:
        """Raise the session's limit of recursive CTE iterations (see MAX_RECURSIVE_CTE_ITERATIONS)."""
        self.execute_query(f'SET SESSION max_recursive_cte_iterations = {MAX_RECURSIVE_CTE_ITERATIONS};')

    def create_tc_path_table(self) -> None:
        """Create the edge table."""
        self.execute_query('CREATE TABLE edge (x INT NOT NULL, y INT NOT NULL);')

    def import_data_from_file(self, table_name: str, file_path: str, delimiter: str = '\t') -> None:
        """Load a tab-separated edge file from the client side (LOAD DATA LOCAL INFILE)."""
        self.execute_query(
            f"LOAD DATA LOCAL INFILE '{file_path}' INTO TABLE {table_name} "
            f"FIELDS TERMINATED BY '{delimiter}' LINES TERMINATED BY '\\n' (x, y);"
        )

    def create_tc_path_index(self) -> None:
        """Index the edge table on (y, x)."""
        self.execute_query('CREATE INDEX edge_yx ON edge (y, x);')

    def analyze_tc_path_table(self) -> None:
        """Collect the optimizer's statistics of the edge table."""
        self.execute_query('ANALYZE TABLE edge;')

    def drop_tc_path_tc_result_tables(self) -> None:
        """Drop the tables, so that the next run starts on a clean slate."""
        self.execute_query('DROP TABLE IF EXISTS tc_result;')
        self.execute_query('DROP TABLE IF EXISTS edge;')

    def export_data_to_file(self, output_file) -> None:
        """Fetch the closure through the client and write it as CSV (INTO OUTFILE would write inside the server)."""
        cursor = self.conn.cursor()
        cursor.execute('SELECT x, y FROM tc_result;')
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['x', 'y'])
            writer.writerows(cursor.fetchall())
