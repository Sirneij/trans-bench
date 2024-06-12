from singlestoredb_rules import SingleStoreOperations


class SingleStoreLeftRecursion(SingleStoreOperations):
    def run_recursive_query(self) -> None:
        """
        Runs the left recursion query for transitive closure.
        """
        self.execute_query(
            """
        CREATE TABLE IF NOT EXISTS tc_result AS
        WITH RECURSIVE tc(x, y) AS (
            SELECT tc_path.x, tc_path.y
            FROM tc_path
            UNION ALL
            SELECT tc.x, tc_path.y
            FROM tc, tc_path
            WHERE tc.y = tc_path.x
        )
        SELECT * FROM tc;
        """
        )