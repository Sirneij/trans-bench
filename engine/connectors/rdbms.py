"""
engine/connectors/rdbms.py

Connectors for SQL relational databases:
  - PostgreSQLConnector  (protocol: psycopg2)
  - MariaDBConnector     (protocol: mysqlclient)
  - CockroachDBConnector (protocol: cockroachdb)

Each connector dynamically imports the mode-specific operation class from
the system's rules/ directory, preserving the existing query files unchanged.
"""

from __future__ import annotations

import importlib.util
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

from engine.connectors.base import BaseConnector

if TYPE_CHECKING:
    from engine.loader import SystemDescriptor

log = logging.getLogger(__name__)


def _dynamic_import_class(module_path: Path, class_name: str) -> type:
    """Import a class from an arbitrary file path at runtime."""
    spec = importlib.util.spec_from_file_location(module_path.stem, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_path.stem] = module
    spec.loader.exec_module(module)
    return getattr(module, class_name)


# ────────────────────────────────────────────────────────────────────────────
# PostgreSQL
# ────────────────────────────────────────────────────────────────────────────


class PostgreSQLConnector(BaseConnector):
    """Runs transitive closure experiments on PostgreSQL via psycopg2."""

    def connect(self, credentials: dict[str, Any], descriptor: 'SystemDescriptor') -> None:
        import psycopg2

        db_url = credentials.get('dbURL', '')
        self._connection = psycopg2.connect(db_url)
        log.info('PostgreSQL connected')

    def run_experiment(
        self,
        rule_path: Path,
        input_path: Path,
        output_folder: Path,
        descriptor: 'SystemDescriptor',
        config: dict[str, Any],
        query_bindings: dict[str, Any] | None = None,
    ) -> dict[str, float]:
        # Derive class name from file stem: transitive_right_recursion → RightRecursion
        mode_word = rule_path.stem.split('_', 1)[1].split('_')[0].capitalize()
        class_name = f'PostgreSQL{mode_word}Recursion'

        # Load init module to make PostgresOperations available
        init_path = descriptor.system_dir / '__init__.py'
        if not init_path.exists():
            init_path = Path('postgres_rules') / '__init__.py'
        spec = importlib.util.spec_from_file_location('postgres_rules', init_path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules['postgres_rules'] = mod
        spec.loader.exec_module(mod)

        OpClass = _dynamic_import_class(rule_path, class_name)

        # Pass query_bindings to the operations class via config
        config_with_bindings = {**config}
        if query_bindings:
            config_with_bindings['query_bindings'] = query_bindings

        ops = OpClass(config_with_bindings, self._connection)

        results_path = output_folder / 'postgres_results.csv'
        phases = descriptor.timing_phases
        measurements: list[tuple[float, float]] = [(0.0, 0.0)] * len(phases)

        try:
            ops.drop_tc_path_tc_result_tables()
            measurements[0] = self.timed(ops.create_tc_path_table)[:2]
            measurements[1] = self.timed(ops.import_data_from_tsv, 'edge', str(input_path))[:2]
            measurements[2] = self.timed(ops.create_tc_path_index)[:2]
            measurements[3] = self.timed(ops.analyze_tc_path_table)[:2]
            measurements[4] = self.timed(ops.run_recursive_query)[:2]
            measurements[5] = self.timed(ops.export_transitive_closure_results, results_path)[:2]
            ops.drop_tc_path_tc_result_tables()
        except Exception as e:
            log.error(f'PostgreSQL experiment error: {e}')

        return self.build_timing_row(phases, measurements)

    def close(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None


# ────────────────────────────────────────────────────────────────────────────
# MariaDB
# ────────────────────────────────────────────────────────────────────────────


class MariaDBConnector(BaseConnector):
    """Runs transitive closure experiments on MariaDB via mysqlclient."""

    def connect(self, credentials: dict[str, Any], descriptor: 'SystemDescriptor') -> None:
        import MySQLdb

        self._connection = MySQLdb.connect(
            db=credentials.get('database', ''),
            user=credentials.get('user', ''),
            passwd=credentials.get('password', ''),
            host=credentials.get('host', 'localhost'),
            port=int(credentials.get('port', 3306)),
            local_infile=1,
        )
        log.info('MariaDB connected')

    def run_experiment(
        self,
        rule_path: Path,
        input_path: Path,
        output_folder: Path,
        descriptor: 'SystemDescriptor',
        config: dict[str, Any],
        query_bindings: dict[str, Any] | None = None,
    ) -> dict[str, float]:
        mode_word = rule_path.stem.split('_', 1)[1].split('_')[0].capitalize()
        class_name = f'MariaDB{mode_word}Recursion'

        init_path = descriptor.system_dir / '__init__.py'
        if not init_path.exists():
            init_path = Path('mariadb_rules') / '__init__.py'
        spec = importlib.util.spec_from_file_location('mariadb_rules', init_path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules['mariadb_rules'] = mod
        spec.loader.exec_module(mod)

        OpClass = _dynamic_import_class(rule_path, class_name)

        # Pass query_bindings to the operations class via config
        config_with_bindings = {**config}
        if query_bindings:
            config_with_bindings['query_bindings'] = query_bindings

        ops = OpClass(config_with_bindings, self._connection)

        results_path = output_folder / 'mariadb_results.csv'
        phases = descriptor.timing_phases
        measurements: list[tuple[float, float]] = [(0.0, 0.0)] * len(phases)

        try:
            ops.drop_tc_path_tc_result_tables()
            ops.set_standard_cte_to_zero()
            measurements[0] = self.timed(ops.create_tc_path_table)[:2]
            measurements[1] = self.timed(ops.import_data_from_file, 'edge', str(input_path))[:2]
            measurements[2] = self.timed(ops.create_tc_path_index)[:2]
            measurements[3] = self.timed(ops.analyze_tc_path_table)[:2]
            measurements[4] = self.timed(ops.run_recursive_query)[:2]
            measurements[5] = self.timed(ops.export_data_to_file)[:2]
            ops.drop_tc_path_tc_result_tables()
        except Exception as e:
            log.error(f'MariaDB experiment error: {e}')

        # Copy MariaDB's forced /tmp output to desired location
        try:
            shutil.copy('/tmp/mariadb_results.csv', str(results_path))
            os.remove('/tmp/mariadb_results.csv')
        except Exception as e:
            log.warning(f'MariaDB result copy: {e}')

        return self.build_timing_row(phases, measurements)

    def close(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None


# ────────────────────────────────────────────────────────────────────────────
# CockroachDB
# ────────────────────────────────────────────────────────────────────────────


class CockroachDBConnector(BaseConnector):
    """Runs transitive closure experiments on CockroachDB via psycopg2."""

    def connect(self, credentials: dict[str, Any], descriptor: 'SystemDescriptor') -> None:
        import psycopg2

        db_url = credentials.get('dbURL', '')
        self._connection = psycopg2.connect(db_url)
        log.info('CockroachDB connected')

    def run_experiment(
        self,
        rule_path: Path,
        input_path: Path,
        output_folder: Path,
        descriptor: 'SystemDescriptor',
        config: dict[str, Any],
        query_bindings: dict[str, Any] | None = None,
    ) -> dict[str, float]:
        mode_word = rule_path.stem.split('_', 1)[1].split('_')[0].capitalize()
        class_name = f'CockroachDB{mode_word}Recursion'

        init_path = descriptor.system_dir / '__init__.py'
        if not init_path.exists():
            init_path = Path('cockroachdb_rules') / '__init__.py'
        spec = importlib.util.spec_from_file_location('cockroachdb_rules', init_path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules['cockroachdb_rules'] = mod
        spec.loader.exec_module(mod)

        OpClass = _dynamic_import_class(rule_path, class_name)

        # Pass query_bindings to the operations class via config
        config_with_bindings = {**config}
        if query_bindings:
            config_with_bindings['query_bindings'] = query_bindings

        ops = OpClass(config_with_bindings, self._connection)

        results_path = output_folder / 'cockroachdb_results.csv'
        external_dir = descriptor.credentials.get(
            'externalDirectory', config.get('cockroachdb', {}).get('externalDirectory', '')
        )
        phases = descriptor.timing_phases
        measurements: list[tuple[float, float]] = [(0.0, 0.0)] * len(phases)

        try:
            ops.drop_tc_path_tc_result_tables()
            measurements[0] = self.timed(ops.create_tc_path_table)[:2]

            cmd = f'mkdir -p {external_dir} && cp {input_path} {external_dir}'
            subprocess.run(cmd, shell=True, check=True, capture_output=True)
            filename = f'{input_path.stem}{input_path.suffix}'
            measurements[1] = self.timed(ops.import_data_from_tsv, 'edge', filename)[:2]
            subprocess.run(f'rm -r {external_dir}{filename}', shell=True, capture_output=True)

            measurements[2] = self.timed(ops.create_tc_path_index)[:2]
            measurements[3] = self.timed(ops.analyze_tc_path_table)[:2]
            measurements[4] = self.timed(ops.run_recursive_query)[:2]
            measurements[5] = self.timed(ops.export_transitive_closure_results, results_path)[:2]
            ops.drop_tc_path_tc_result_tables()

            # Copy from CockroachDB's nodelocal tmp export
            cp_cmd = f'cp {external_dir}tmp/*.csv {results_path}'
            rm_cmd = f'rm -rf {external_dir}tmp'
            subprocess.run(cp_cmd, shell=True, capture_output=True)
            subprocess.run(rm_cmd, shell=True, capture_output=True)
        except Exception as e:
            log.error(f'CockroachDB experiment error: {e}')

        return self.build_timing_row(phases, measurements)

    def close(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None
