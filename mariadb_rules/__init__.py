from typing import Any

import MySQLdb

from common import Base


class MariaDBOperations(Base):
    def __init__(self, config: dict[str, Any], conn: MySQLdb.Connection) -> None:
        self.config = config
        self.conn = conn
        self.conn.autocommit(True)

    def execute_query(self, query: str, params: Any = None) -> None:
        cursor = self.conn.cursor()
        cursor.execute(query, params)

    def import_data_from_file(
        self, table_name: str, file_path: str, delimiter: str = '\t'
    ) -> None:
        query = f"""
        LOAD DATA LOCAL INFILE '{file_path}'
        INTO TABLE {table_name}
        FIELDS TERMINATED BY '{delimiter}'
        LINES TERMINATED BY '\n'
        (x, y);
        """
        self.execute_query(query)

    def export_data_to_file(self, delimiter: str = ',') -> None:
        outfile_query = f"SELECT * FROM tc_result INTO OUTFILE '/tmp/mariadb_results.csv' FIELDS TERMINATED BY '{delimiter}' LINES TERMINATED BY '\n';"
        self.execute_query(outfile_query)

    def create_tc_path_table(self) -> None:
        self.execute_query(
            """
        CREATE TABLE edge(
            x INTEGER NOT NULL,
            y INTEGER NOT NULL
        );
        """
        )

    def create_tc_path_index(self) -> None:
        self.execute_query(
            """
        CREATE INDEX edge_yx ON edge(y, x);
        """
        )

    def analyze_tc_path_table(self) -> None:
        self.execute_query('ANALYZE TABLE edge;')

    def drop_tc_path_tc_result_tables(self) -> None:
        """
        Drop the created tables so that next iteration will start on a clean slate
        """
        self.execute_query('DROP TABLE IF EXISTS edge, tc_result;')
