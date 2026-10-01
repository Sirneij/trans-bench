"""
engine/connectors/neo4j_conn.py

Neo4j connector — executes Cypher scripts via the neo4j Python driver.
The Cypher rule files use {data_file} and {output_file} placeholders.

A rule file is a sequence of statements separated by ';': setup statements (delete, load,
index), then the timed query, then the export. Timing rule for the driver (important):
session.run() is *lazy* — it returns before the statement has been executed, and an
unconsumed result is executed as part of the *next* statement. Every statement is therefore
finished inside its own timed call:
  * setup statements with consume() (update statements are executed even when their empty
    result is discarded);
  * the query and the export by fetching all records — consume() on a read query discards the
    stream and lets Neo4j skip computing it (measured: 0.04 s instead of 27 s).
"""

from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Any

from engine.connectors.base import BaseConnector
from engine.memory import sampler_or_none

if TYPE_CHECKING:
    from engine.loader import SystemDescriptor

log = logging.getLogger(__name__)


class Neo4jConnector(BaseConnector):
    """Runs transitive closure experiments on Neo4j using Cypher scripts."""

    def connect(self, credentials: dict[str, Any], descriptor: 'SystemDescriptor') -> None:
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
        import_dir = self._credentials.get('import_directory', config.get('neo4j', {}).get('import_directory', ''))
        export_filename = f'neo4j_export_{os.getpid()}.csv'
        fact_file_name = input_path.name

        # Copy facts file into Neo4j's import directory
        try:
            subprocess.run(f'cp {input_path.resolve()} {import_dir}/', shell=True, check=True, capture_output=True)
        except Exception as e:
            self._record_error(f'Neo4j import copy error: {e}')

        with open(rule_path) as f:
            cypher_script = f.read()
        cypher_script = cypher_script.replace('{data_file}', fact_file_name)
        cypher_script = cypher_script.replace('{output_file}', export_filename)

        # Apply query binding substitutions
        if query_bindings:
            cypher_script = self._substitute_query_bindings(cypher_script, query_bindings)

        commands = [f'{cmd.strip()};' for cmd in cypher_script.split(';') if cmd.strip()]
        phases = descriptor.timing_phases
        measurements: list[tuple[float, float]] = [(0.0, 0.0)] * len(phases)

        session = self._driver.session()

        def run_consumed(cypher: str):
            return session.run(cypher).consume()

        def run_fetched(cypher: str):
            return list(session.run(cypher))

        try:
            # Setup commands (all but last two)
            for i, command in enumerate(commands[:-2]):
                if i >= len(phases) - 2:
                    break
                real, cpu, _ = self.timed(run_consumed, command)
                measurements[i] = (real, cpu)

            # Penultimate = main query
            real, cpu, _ = self.timed_query(run_fetched, commands[-2])
            measurements[-2] = (real, cpu)

            # Last = export command
            real, cpu, result = self.timed(run_fetched, commands[-1])
            measurements[-1] = (real, cpu)
            for rec in result:
                log.debug(f'Neo4j export record: {rec}')
        except Exception as e:
            self._record_error(f'Neo4j experiment error: {e}')
        finally:
            session.close()

        # Copy result from import dir to output folder
        results_path = self.result_path(output_folder, descriptor, 'neo4j_results.csv')
        export_source = f'{import_dir}/{export_filename}'
        try:
            subprocess.run(f'cp {export_source} {results_path}', shell=True, check=True, capture_output=True)
        except Exception as e:
            if not self.errors:  # a failed query leaves no export; report the cause, not the copy
                self._record_error(f'Neo4j result copy error: {e}')

        # Cleanup
        try:
            subprocess.run(f'rm -f {import_dir}/{fact_file_name} {export_source}', shell=True, capture_output=True)
        except Exception:
            pass

        return self.build_timing_row(phases, measurements)

    def memory_sampler(self):
        """Neo4j's own accounting of the heap memory used by the running query (the quantity that
        dbms.memory.transaction.total.max limits), read from a second session while it runs. The
        server's resident memory is not usable: the JVM keeps its heap after the first queries."""

        def make_probe():
            session = self._driver.session()

            def probe():
                rec = session.run(
                    'SHOW TRANSACTIONS YIELD currentQuery, estimatedUsedHeapMemory '
                    "WHERE currentQuery STARTS WITH 'MATCH (start' RETURN sum(estimatedUsedHeapMemory) AS m").single()
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
        if hasattr(self, '_driver') and self._driver:
            self._driver.close()
            self._driver = None
