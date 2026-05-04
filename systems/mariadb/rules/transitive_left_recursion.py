from mariadb_rules import MariaDBOperations


class MariaDBLeftRecursion(MariaDBOperations):
    def run_recursive_query(self) -> None:
        """
        Runs the right recursion query for transitive closure.
        """
        self.execute_query(
            """
        CREATE TABLE tc_result AS
        WITH RECURSIVE tc AS (
            SELECT x, y FROM edge
            UNION
            SELECT tc.x, edge.y FROM tc JOIN edge ON tc.y = edge.x
        )
        SELECT * FROM tc;
        """
        )
