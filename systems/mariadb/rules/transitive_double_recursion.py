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
            SELECT tc1.x, tc2.y FROM tc AS tc1, tc AS tc2 WHERE tc1.y = tc2.x
        )
        SELECT * FROM tc;
        """
        )
