from mariadb_rules import MariaDBOperations


class MariaDBLeftRecursion(MariaDBOperations):
    def run_recursive_query(self) -> None:
        """
        Runs the left recursion query for transitive closure.
        """
        self.execute_query(
            """
        CREATE TABLE tc_result AS
        WITH RECURSIVE tc AS (
            SELECT tc_path.x, tc_path.y
            FROM tc_path
            UNION
            SELECT tc.x, tc_path.y
            FROM tc, tc_path
            WHERE tc.y = tc_path.x
        )
        SELECT * FROM tc;
        """
        )
