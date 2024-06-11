from postgres_rules import PostgresOperations


class PostgreSQLRightRecursion(PostgresOperations):
    def run_recursive_query(self) -> None:
        """
        Runs the right recursion query for transitive closure.
        """
        self.execute_query(
            """
        CREATE TABLE tc_result AS
        WITH RECURSIVE tc AS (
            SELECT tc_path.x, tc_path.y
            FROM tc_path
            UNION
            SELECT tc_path.x, tc.y
            FROM tc_path, tc
            WHERE tc_path.y = tc.x
        )
        SELECT * FROM tc;
        """
        )
