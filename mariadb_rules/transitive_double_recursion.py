from mariadb_rules import MariaDBOperations


class MariaDBDoubleRecursion(MariaDBOperations):
    def run_recursive_query(self) -> None:
        """
        Runs the double recursion query for transitive closure.
        """
        self.execute_query(
            """
        CREATE TABLE tc_result AS
        WITH RECURSIVE tc AS (
            SELECT x, y FROM edge
            UNION
            SELECT a.x, b.y FROM tc AS a, tc AS b WHERE a.y = b.x
        )
        SELECT * FROM tc;
        """
        )
