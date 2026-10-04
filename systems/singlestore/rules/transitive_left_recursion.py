# singlestore_rules is registered in sys.modules by the connector at run time (engine/connectors/), so
# pylint cannot resolve it statically
# pylint: disable=import-error
from singlestore_rules import SingleStoreOperations


class SingleStoreLeftRecursion(SingleStoreOperations):
    def run_recursive_query(self) -> None:
        """Transitive closure with left recursion (UNION ALL + outer DISTINCT; see __init__.py)."""
        self.execute_query(
            """
        CREATE TABLE tc_result AS
        WITH RECURSIVE tc AS (
            SELECT x, y FROM edge
            UNION ALL
            SELECT tc.x, edge.y FROM tc JOIN edge ON tc.y = edge.x
        )
        SELECT DISTINCT x, y FROM tc;
        """
        )
