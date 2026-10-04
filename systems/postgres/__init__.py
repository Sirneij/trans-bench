from typing import Any

from psycopg2 import extensions, sql

from common import Base


class PostgresOperations(Base):
    def __init__(self, config: dict[str, Any], conn: extensions.connection) -> None:
        """
        Initialize the PostgresOperations class with the given configuration and database connection.

        Parameters
        ----------
        config : dict[str, Any]
            Configuration dictionary.
        conn : extensions.connection
            A psycopg2 connection object.

        """
        super().__init__(config)
        self.conn = conn
        self.conn.set_session(autocommit=True)

    def execute_query(self, query: str, params: Any = None) -> None:
        """
        Execute a SQL query.

        Parameters
        ----------
        query : str
            The SQL query to execute.
        params : Any, optional
            Parameters to pass to the query. Defaults to None.

        """
        cursor = self.conn.cursor()
        cursor.execute(query, params)

    def import_data_from_tsv(self, table_name: str, file_path: str) -> None:
        """
        Import data from a TSV file into a specified table.

        Parameters
        ----------
        table_name : str
            The name of the table to import data into.
        file_path : str
            The path to the TSV file.

        """
        with self.conn.cursor() as cursor:
            with open(file_path, 'r', encoding='utf-8') as f:
                cursor.copy_expert(
                    sql.SQL("COPY {} FROM STDIN WITH DELIMITER E'\t'").format(sql.Identifier(table_name)),
                    f,
                )

    def export_data_to_csv(self, query: str, file_path: str) -> None:
        """
        Export data from a SQL query to a CSV file.

        Parameters
        ----------
        query : str
            The SQL query to select data.
        file_path : str
            The path to the CSV file.

        """
        with self.conn.cursor() as cursor:
            with open(file_path, 'w', encoding='utf-8') as f:
                cursor.copy_expert(
                    sql.SQL("COPY ({}) TO STDOUT WITH CSV HEADER").format(sql.SQL(query)),
                    f,
                )

    def export_transitive_closure_results(self, output_file: str) -> None:
        """
        Export the results of the transitive closure query to a CSV file.

        Parameters
        ----------
        output_file : str
            The path to the CSV file.

        """
        self.export_data_to_csv("SELECT * FROM tc_result", output_file)

    def create_tc_path_table(self) -> None:
        """Create a temporary table for storing path data with columns x and y."""
        self.execute_query(
            """
        CREATE TABLE edge(
            x INTEGER NOT NULL,
            y INTEGER NOT NULL
        );
        """
        )

    def create_tc_path_index(self) -> None:
        """Create an index on the edge table for columns y and x."""
        self.execute_query(
            """
        CREATE INDEX edge_yx ON edge(y, x);
        """
        )

    def analyze_tc_path_table(self) -> None:
        """Analyze the edge table to update statistics for query optimization."""
        self.execute_query('ANALYZE edge;')

    def drop_tc_path_tc_result_tables(self) -> None:
        """Drop the tables, so that the next run starts on a clean slate."""
        self.execute_query('DROP TABLE IF EXISTS edge, tc_result;')
