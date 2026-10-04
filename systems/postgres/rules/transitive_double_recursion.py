# postgres_rules is registered in sys.modules by the connector at run time (engine/connectors/), so
# pylint cannot resolve it statically
# pylint: disable=import-error
from postgres_rules import PostgresOperations


class PostgreSQLDoubleRecursion(PostgresOperations):
    def run_recursive_query(self) -> None:
        """Run the double recursion query for transitive closure."""
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
