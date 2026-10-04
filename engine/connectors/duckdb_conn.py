"""
Run DuckDB trials: SQL scripts executed in-process through the DuckDB Python driver.

The SQL rule files use {data_file} and {output_file} placeholders; each statement of a script is
one timing phase of the descriptor, in order.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb

from engine.connectors.base import BaseConnector
from engine.memory import rss_of_self, sampler_or_none

if TYPE_CHECKING:
    from engine.loader import SystemDescriptor

log = logging.getLogger(__name__)


class DuckDBConnector(BaseConnector):
    """Runs transitive closure experiments on DuckDB using raw SQL files."""

    def __init__(self):
        """Start without a database file; run_experiment creates one per run."""
        super().__init__()
        self._db_path: Path | None = None
        self._credentials: dict[str, Any] = {}

    def connect(self, credentials: dict[str, Any], descriptor: 'SystemDescriptor') -> None:
        """Keep the credentials; DuckDB needs no server, and its database is created per run."""
        self._credentials = credentials
        log.info('DuckDB connector ready (connection created per run)')

    def run_experiment(
        self,
        rule_path: Path,
        input_path: Path,
        output_folder: Path,
        descriptor: 'SystemDescriptor',
        config: dict[str, Any],
        query_bindings: dict[str, Any] | None = None,
    ) -> dict[str, float]:
        """Execute the rule file statement by statement, timing each as one phase."""
        # Per-run db file to avoid cross-contamination. A run that was killed leaves the file (and
        # its WAL) behind; the next run would then fail on CREATE TABLE edge, so remove them first.
        self._db_path = rule_path.parent / 'duckdb' / 'duckdb_file.db'
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        for stale in self._db_path.parent.glob(f'{self._db_path.name}*'):
            stale.unlink()

        conn = duckdb.connect(database=str(self._db_path))
        results_path = self.result_path(output_folder, descriptor, 'duckdb_results.csv')

        with open(rule_path, encoding='utf-8') as f:
            sql_script = f.read()

        sql_script = sql_script.replace('{data_file}', str(input_path))
        sql_script = sql_script.replace('{output_file}', str(results_path))

        # Apply query binding substitutions
        if query_bindings:
            sql_script = self._substitute_query_bindings(sql_script, query_bindings)

        sql_commands = [f'{cmd.strip()};' for cmd in sql_script.split(';') if cmd.strip()]
        phases = descriptor.timing_phases
        measurements: list[tuple[float, float]] = [(0.0, 0.0)] * len(phases)

        try:
            for i, command in enumerate(sql_commands):
                if i >= len(phases):
                    break
                try:
                    timer = self.timed_query if phases[i].id == descriptor.query_phase else self.timed
                    real, cpu, _ = timer(conn.execute, command)
                    measurements[i] = (real, cpu)
                except Exception as e:
                    self._record_error(f'DuckDB command {i} error: {e}\nSQL: {command}')
        except Exception as e:
            self._record_error(f'DuckDB experiment error: {e}')
        finally:
            conn.close()
            self._cleanup()

        return self.build_timing_row(phases, measurements)

    def memory_sampler(self):
        """Sample the RSS of this process: DuckDB runs in-process (one process per run)."""
        return sampler_or_none(rss_of_self, 'duckdb process RSS')

    def close(self) -> None:
        """Remove the run's database file."""
        self._cleanup()

    def _cleanup(self) -> None:
        """Delete the database file and its write-ahead log, if they exist."""
        if self._db_path:
            # the database file and its write-ahead log (duckdb_file.db.wal)
            for f in [self._db_path, *self._db_path.parent.glob(f'{self._db_path.name}.*')]:
                if f.exists():
                    try:
                        f.unlink()
                    except Exception as e:
                        log.warning(f'DuckDB cleanup: {e}')
            self._db_path = None
