# cockroachdb_rules is registered in sys.modules by the connector at run time (engine/connectors/), so
# pylint cannot resolve it statically
# pylint: disable=import-error
from cockroachdb_rules import CockroachDBOperations


class CockroachDBRightRecursion(CockroachDBOperations):
    def run_recursive_query(self) -> None:
        """Run the right recursion query for transitive closure."""
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
