from typing import Any

from common import Base


class MariaDBOperations(Base):
    def __init__(self, config: dict[str, Any], conn: Any) -> None:
        self.config = config
        self.conn = conn

    def execute_query(self, query: str, params: Any = None) -> None:
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        self.conn.commit()
        cursor.close()

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
        CREATE TABLE tc_path(
            x INTEGER NOT NULL,
            y INTEGER NOT NULL
        );
        """
        )

    def create_tc_path_index(self) -> None:
        self.execute_query(
            """
        CREATE INDEX tc_path_yx ON tc_path(y, x);
        """
        )

    def analyze_tc_path_table(self) -> None:
        self.execute_query('ANALYZE TABLE tc_path;')

    def drop_tc_path_tc_result_tables(self) -> None:
        """
        Drop the created tables so that next iteration will start on a clean slate
        """
        self.execute_query('DROP TABLE IF EXISTS tc_path, tc_result;')
