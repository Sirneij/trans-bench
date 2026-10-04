# singlestore_rules is registered in sys.modules by the connector at run time (engine/connectors/), so
# pylint cannot resolve it statically
# pylint: disable=import-error
from singlestore_rules import SingleStoreOperations


class SingleStoreRightRecursion(SingleStoreOperations):
    def run_recursive_query(self) -> None:
        """Transitive closure with right recursion (UNION ALL + outer DISTINCT; see __init__.py)."""
        self.execute_query(
            """
        CREATE TABLE tc_result AS
        WITH RECURSIVE tc AS (
            SELECT x, y FROM edge
            UNION ALL
            SELECT edge.x, tc.y FROM edge JOIN tc ON edge.y = tc.x
        )
        SELECT DISTINCT x, y FROM tc;
        """
        )
