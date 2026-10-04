# postgres_rules is registered in sys.modules by the connector at run time (engine/connectors/), so
# pylint cannot resolve it statically
# pylint: disable=import-error
from postgres_rules import PostgresOperations


class PostgreSQLLeftRecursion(PostgresOperations):
    def run_recursive_query(self) -> None:
        """Run the left recursion query for transitive closure."""
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
