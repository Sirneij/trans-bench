from typing import Any

from psycopg2 import extensions

from common import Base


class CockroachDBOperations(Base):
    def __init__(self, config: dict[str, Any], conn: extensions.connection) -> None:
        """
        Initializes the CockroachDBOperations class with the given configuration and database connection.

        Args:
            config (dict[str, Any]): Configuration dictionary.
            conn (extensions.connection): A psycopg2 connection object.
        """
        super().__init__(config)
        self.conn = conn
        self.conn.set_session(autocommit=True)

    def execute_query(self, query: str, params: Any = None) -> None:
        """
        Executes a SQL query.

        Args:
            query (str): The SQL query to execute.
            params (Any, optional): Parameters to pass to the query. Defaults to None.
        """
        cursor = self.conn.cursor()
        cursor.execute(query, params)

    def import_data_from_tsv(self, table_name: str, file_path: str) -> None:
        """
        Imports data from a TSV file into a specified table using CockroachDB's IMPORT command.

        Args:
            table_name (str): The name of the table to import data into.
            file_path (str): The path to the TSV file.
        """
        query = f"""
        IMPORT INTO {table_name} (x, y)
        CSV DATA ('nodelocal://1/{file_path}')
        WITH delimiter = e'\t';
        """
        self.execute_query(query)

    def export_data_to_csv(self, table_name: str, file_path: str) -> None:
        """
        Exports data from a table to a CSV file using CockroachDB's EXPORT command.

        Args:
            table_name (str): The name of the table to export data from.
            file_path (str): The path to the CSV file.
        """
        query = f"""
        EXPORT INTO CSV 'nodelocal://1/tmp'
        FROM TABLE {table_name};
        """
        self.execute_query(query)

    def export_transitive_closure_results(self, output_file: str) -> None:
        """
        Exports the results of the transitive closure query to a CSV file.

        Args:
            output_file (str): The path to the CSV file.
        """
        self.export_data_to_csv("tc_result", output_file)

    def create_tc_path_table(self) -> None:
        """
        Creates a table for storing path data with columns x and y.
        """
        self.execute_query(
            """
        CREATE TABLE edge(
            x INTEGER NOT NULL,
            y INTEGER NOT NULL
        );
        """
        )

    def create_tc_path_index(self) -> None:
        """
        Creates an index on the edge table for columns y and x.
        """
        self.execute_query(
            """
        CREATE INDEX edge_yx ON edge(y, x);
        """
        )

    def analyze_tc_path_table(self) -> None:
        """
        Analyzes the edge table to update statistics for query optimization.
        """
        self.execute_query('ANALYZE edge;')

    def drop_tc_path_tc_result_tables(self) -> None:
        """
        Drop the created tables so that next iteration will start on a clean slate
        """
        self.execute_query('DROP TABLE IF EXISTS edge, tc_result;')
