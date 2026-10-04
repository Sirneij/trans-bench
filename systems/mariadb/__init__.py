"""The steps of a MariaDB run; the rule files add the recursive query (systems/mariadb/rules/)."""

from typing import Any

import MySQLdb

from common import Base


class MariaDBOperations(Base):
    """Create, load, index, analyze and export; the connector times each of these steps."""

    def __init__(self, config: dict[str, Any], conn: MySQLdb.Connection) -> None:
        """Keep the configuration and the connection, which commits every statement."""
        super().__init__(config)
        self.conn = conn
        self.conn.autocommit(True)

    def execute_query(self, query: str, params: Any = None) -> None:
        """Execute one statement."""
        cursor: MySQLdb.cursors.BaseCursor = self.conn.cursor()
        cursor.execute(query, params)

    def import_data_from_file(self, table_name: str, file_path: str, delimiter: str = '\t') -> None:
        """Load a tab-separated edge file from the client side (LOAD DATA LOCAL INFILE)."""
        query = f"""
        LOAD DATA LOCAL INFILE '{file_path}'
        INTO TABLE {table_name}
        FIELDS TERMINATED BY '{delimiter}'
        LINES TERMINATED BY '\n'
        (x, y);
        """
        self.execute_query(query)

    def export_data_to_file(self, delimiter: str = ',') -> None:
        """Write the closure with SELECT ... INTO OUTFILE (on the server's side; the connector moves the file)."""
        outfile_query = (
            "SELECT * FROM tc_result INTO OUTFILE '/tmp/mariadb_results.csv' "  # nosec B108,B608
            f"FIELDS TERMINATED BY '{delimiter}' LINES TERMINATED BY '\n';"
        )
        self.execute_query(outfile_query)

    def create_tc_path_table(self) -> None:
        """Create the edge table."""
        self.execute_query(
            """
        CREATE TABLE edge(
            x INTEGER NOT NULL,
            y INTEGER NOT NULL
        );
        """
        )

    def create_tc_path_index(self) -> None:
        """Index the edge table on (y, x)."""
        self.execute_query(
            """
        CREATE INDEX edge_yx ON edge(y, x);
        """
        )

    def analyze_tc_path_table(self) -> None:
        """Collect the optimizer's statistics of the edge table."""
        self.execute_query('ANALYZE TABLE edge;')

    def drop_tc_path_tc_result_tables(self) -> None:
        """Drop the tables, so that the next run starts on a clean slate."""
        self.execute_query('DROP TABLE IF EXISTS edge, tc_result;')

    def set_standard_cte_to_zero(self) -> None:
        """Turn standard_compliant_cte off for the session, as the benchmark's queries expect."""
        self.execute_query(
            """
        SET @@standard_compliant_cte=0;
        """
        )
