# singlestore_rules is registered in sys.modules by the connector at run time (engine/connectors/), so
# pylint cannot resolve it statically
# pylint: disable=import-error
from singlestore_rules import SingleStoreOperations


class SingleStoreDoubleRecursion(SingleStoreOperations):
    def run_recursive_query(self) -> None:
        """Transitive closure with double recursion (UNION ALL + outer DISTINCT; see __init__.py)."""
        self.execute_query(
            """
        CREATE TABLE tc_result AS
        WITH RECURSIVE tc AS (
            SELECT x, y FROM edge
            UNION ALL
            SELECT tc1.x, tc2.y FROM tc AS tc1, tc AS tc2 WHERE tc1.y = tc2.x
        )
        SELECT DISTINCT x, y FROM tc;
        """
        )
