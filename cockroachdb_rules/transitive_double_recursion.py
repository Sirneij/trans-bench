from cockroachdb_rules import CockroachDBOperations


class CockroachDBDoubleRecursion(CockroachDBOperations):
    def run_recursive_query(self) -> None:
        """
        Runs the double recursion query for transitive closure.
        """
        self.execute_query(
            """
        CREATE TABLE tc_result AS
        WITH RECURSIVE tc AS (
            SELECT tc_path.x, tc_path.y
            FROM tc_path
            UNION
            SELECT tc1.x, tc2.y
            FROM tc tc1, tc tc2
            WHERE tc1.y = tc2.x
        )
        SELECT * FROM tc;
        """
        )
