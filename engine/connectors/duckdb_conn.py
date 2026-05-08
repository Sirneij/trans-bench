"""
engine/connectors/duckdb_conn.py

DuckDB connector — executes SQL scripts directly via the DuckDB Python driver.
The SQL rule files use {data_file} and {output_file} placeholders.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb

from engine.connectors.base import BaseConnector

if TYPE_CHECKING:
    from engine.loader import SystemDescriptor

log = logging.getLogger(__name__)


class DuckDBConnector(BaseConnector):
    """Runs transitive closure experiments on DuckDB using raw SQL files."""

    def __init__(self):
        super().__init__()
        self._db_path: Path | None = None

    def connect(self, credentials: dict[str, Any], descriptor: 'SystemDescriptor') -> None:
        # DuckDB needs no credentials — connection is created per-experiment
        # to ensure isolation.  We store credentials for future use.
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
        # Per-run db file to avoid cross-contamination
        self._db_path = rule_path.parent / 'duckdb' / 'duckdb_file.db'
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

        conn = duckdb.connect(database=str(self._db_path))
        results_path = output_folder / 'duckdb_results.csv'

        with open(rule_path) as f:
            sql_script = f.read()

        sql_script = sql_script.replace('{data_file}', str(input_path))
        sql_script = sql_script.replace('{output_file}', str(results_path))

        sql_commands = [f'{cmd.strip()};' for cmd in sql_script.split(';') if cmd.strip()]
        phases = descriptor.timing_phases
        measurements: list[tuple[float, float]] = [(0.0, 0.0)] * len(phases)

        try:
            for i, command in enumerate(sql_commands):
                if i >= len(phases):
                    break
                try:
                    real, cpu, _ = self.timed(conn.execute, command)
                    measurements[i] = (real, cpu)
                except Exception as e:
                    log.error(f'DuckDB command {i} error: {e}\nSQL: {command}')
        except Exception as e:
            log.error(f'DuckDB experiment error: {e}')
        finally:
            conn.close()
            self._cleanup()

        return self.build_timing_row(phases, measurements)

    def close(self) -> None:
        self._cleanup()

    def _cleanup(self) -> None:
        if self._db_path and self._db_path.exists():
            try:
                self._db_path.unlink()
            except Exception as e:
                log.warning(f'DuckDB cleanup: {e}')
            self._db_path = None
