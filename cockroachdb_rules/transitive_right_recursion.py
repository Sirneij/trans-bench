from cockroachdb_rules import CockroachDBOperations


class CockroachDBLeftRecursion(CockroachDBOperations):
    def run_recursive_query(self) -> None:
        """
        Runs the left recursion query for transitive closure.
        """
        self.execute_query(
            """
        CREATE TABLE tc_result AS
        WITH RECURSIVE tc AS (
            SELECT x, y FROM edge
            UNION
            SELECT edge.x, tc.y FROM edge JOIN tc ON edge.y = tc.x
        )
        SELECT * FROM tc;
        """
        )
