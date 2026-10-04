"""
Run Neo4j trials: Cypher scripts executed through the neo4j Python driver.

The Cypher rule files use {data_file} and {output_file} placeholders. A rule file is a sequence of
statements separated by ';': setup statements (delete, load, index), then the timed query, then
the export. One rule of the driver matters for the timing: session.run() is lazy. It returns
before the statement has been executed, and an unconsumed result is executed as part of the next
statement. Every statement is therefore finished inside its own timed call:
  * setup statements with consume() (update statements are executed even when their empty
    result is discarded);
  * the query and the export by fetching all records; consume() on a read query discards the
    stream and lets Neo4j skip computing it (measured: 0.04 s instead of 27 s).
"""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any

from engine.connectors.base import BaseConnector
from engine.memory import sampler_or_none

if TYPE_CHECKING:
    from engine.loader import SystemDescriptor

log = logging.getLogger(__name__)


class Neo4jConnector(BaseConnector):
    """Runs transitive closure experiments on Neo4j using Cypher scripts."""

    def __init__(self):
        """Start without a driver; connect() opens it."""
        super().__init__()
        self._driver: Any = None
        self._credentials: dict[str, Any] = {}

    def connect(self, credentials: dict[str, Any], descriptor: 'SystemDescriptor') -> None:
        """Open a driver to the server."""
        from neo4j import GraphDatabase

        uri = credentials.get('uri', 'neo4j://localhost:7687')
        user = credentials.get('user', 'neo4j')
        password = credentials.get('password', '')
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self._credentials = credentials
        log.info(f'Neo4j connected: {uri}')

    def run_experiment(
        self,
        rule_path: Path,
        input_path: Path,
        output_folder: Path,
        descriptor: 'SystemDescriptor',
        config: dict[str, Any],
        query_bindings: dict[str, Any] | None = None,
    ) -> dict[str, float]:
        """Copy the input into Neo4j's import directory, time every statement, and copy the result back."""
        import_dir = Path(
            self._credentials.get('import_directory', config.get('neo4j', {}).get('import_directory', ''))
        )
        export_filename = f'neo4j_export_{os.getpid()}.csv'
        fact_file_name = input_path.name

        # Neo4j's LOAD CSV reads only from its import directory
        try:
            shutil.copy(input_path.resolve(), import_dir / fact_file_name)
        except OSError as e:
            self._record_error(f'Neo4j import copy error: {e}')

        with open(rule_path, encoding='utf-8') as f:
            cypher_script = f.read()
        cypher_script = cypher_script.replace('{data_file}', fact_file_name)
        cypher_script = cypher_script.replace('{output_file}', export_filename)

        # Apply query binding substitutions
        if query_bindings:
            cypher_script = self._substitute_query_bindings(cypher_script, query_bindings)

        commands = [f'{cmd.strip()};' for cmd in cypher_script.split(';') if cmd.strip()]
        measurements = self._time_statements(commands, len(descriptor.timing_phases))

        # the export was written into the import directory; copy it next to the other results
        export_source = import_dir / export_filename
        try:
            shutil.copy(export_source, self.result_path(output_folder, descriptor, 'neo4j_results.csv'))
        except OSError as e:
            if not self.errors:  # a failed query leaves no export; report the cause, not the copy
                self._record_error(f'Neo4j result copy error: {e}')
        for leftover in (import_dir / fact_file_name, export_source):
            leftover.unlink(missing_ok=True)

        return self.build_timing_row(descriptor.timing_phases, measurements)

    def _time_statements(self, commands: list[str], n_phases: int) -> list[tuple[float, float]]:
        """Time the setup statements, the query (with its memory) and the export, one phase each."""
        measurements: list[tuple[float, float]] = [(0.0, 0.0)] * n_phases
        session = self._driver.session()

        def run_consumed(cypher: str):
            """Run an update statement to the end (its empty result is discarded)."""
            return session.run(cypher).consume()

        def run_fetched(cypher: str):
            """Run a read statement and fetch every record, so that Neo4j computes all of them."""
            return list(session.run(cypher))

        try:
            # setup statements: all but the last two
            for i, command in enumerate(commands[:-2]):
                if i >= n_phases - 2:
                    break
                real, cpu, _ = self.timed(run_consumed, command)
                measurements[i] = (real, cpu)

            # the penultimate statement is the query
            real, cpu, _ = self.timed_query(run_fetched, commands[-2])
            measurements[-2] = (real, cpu)

            # the last statement is the export
            real, cpu, result = self.timed(run_fetched, commands[-1])
            measurements[-1] = (real, cpu)
            for rec in result:
                log.debug(f'Neo4j export record: {rec}')
        except Exception as e:
            self._record_error(f'Neo4j experiment error: {e}')
        finally:
            session.close()
        return measurements

    def memory_sampler(self):
        """
        Sample Neo4j's own accounting of the heap memory used by the running query.

        This is the quantity that dbms.memory.transaction.total.max limits, read from a second
        session while the query runs. The server's resident memory is not usable: the JVM keeps
        its heap after the first queries.
        """

        def make_probe():
            """Open the second session and return the probe that queries it."""
            session = self._driver.session()

            def probe():
                """Return the estimated heap memory of the running closure query, in bytes."""
                rec = session.run(
                    'SHOW TRANSACTIONS YIELD currentQuery, estimatedUsedHeapMemory '
                    "WHERE currentQuery STARTS WITH 'MATCH (start' RETURN sum(estimatedUsedHeapMemory) AS m"
                ).single()
                return float(rec['m'] or 0)

            return probe

        return sampler_or_none(make_probe, 'neo4j estimatedUsedHeapMemory of the query', interval=0.01)

    @classmethod
    def cancel_running(cls, credentials: dict[str, Any], descriptor: 'SystemDescriptor') -> None:
        """Terminate all running transactions except this one."""
        from neo4j import GraphDatabase

        driver = GraphDatabase.driver(
            credentials.get('uri', 'neo4j://localhost:7687'),
            auth=(credentials.get('user', 'neo4j'), credentials.get('password', '')),
        )
        try:
            with driver.session() as s:
                ids = [
                    r['transactionId']
                    for r in s.run(
                        'SHOW TRANSACTIONS YIELD transactionId, currentQuery '
                        "WHERE NOT currentQuery STARTS WITH 'SHOW TRANSACTIONS' RETURN transactionId"
                    )
                ]
                if ids:
                    s.run('TERMINATE TRANSACTIONS $ids', ids=ids).consume()
        finally:
            driver.close()

    def close(self) -> None:
        """Close the driver."""
        if self._driver:
            self._driver.close()
            self._driver = None
