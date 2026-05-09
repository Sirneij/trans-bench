"""
engine/connectors/mongodb_conn.py

MongoDB connector — executes Python-based query modules via dynamic import.
"""

from __future__ import annotations

import importlib.util
import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

from engine.connectors.base import BaseConnector

if TYPE_CHECKING:
    from engine.loader import SystemDescriptor

log = logging.getLogger(__name__)


class MongoDBConnector(BaseConnector):
    """Runs transitive closure experiments on MongoDB via pymongo."""

    def connect(self, credentials: dict[str, Any], descriptor: 'SystemDescriptor') -> None:
        from pymongo import MongoClient

        uri = credentials.get('uri', 'mongodb://127.0.0.1:27017/')
        database = credentials.get('database', 'test')
        self._client = MongoClient(uri)
        self._db = self._client[database]
        log.info(f'MongoDB connected: {uri}, db={database}')

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
        class_name = f'MongoDB{mode_word}Recursion'

        # Load __init__ so MongoDBOperations base class is available
        init_path = descriptor.system_dir / '__init__.py'
        if not init_path.exists():
            init_path = Path('mongodb_rules') / '__init__.py'
        spec = importlib.util.spec_from_file_location('mongodb_rules', init_path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules['mongodb_rules'] = mod
        spec.loader.exec_module(mod)

        rule_spec = importlib.util.spec_from_file_location(rule_path.stem, rule_path)
        rule_mod = importlib.util.module_from_spec(rule_spec)
        rule_spec.loader.exec_module(rule_mod)
        OpClass = getattr(rule_mod, class_name)

        ops = OpClass({}, self._db)
        results_path = output_folder / 'mongodb_results.csv'
        phases = descriptor.timing_phases
        measurements: list[tuple[float, float]] = [(0.0, 0.0)] * len(phases)

        try:
            measurements[0] = self.timed(ops.create_collection, 'edge', 'tc_result')[:2]
            measurements[1] = self.timed(ops.insert_data, 'edge', input_path)[:2]
            measurements[2] = self.timed(ops.create_index, 'edge')[:2]
            measurements[3] = self.timed(ops.recursive_query, 'edge', 'tc_result')[:2]
            measurements[4] = self.timed(ops.export_to_csv, 'tc_result', results_path)[:2]
        except Exception as e:
            log.error(f'MongoDB experiment error: {e}')

        return self.build_timing_row(phases, measurements)

    def close(self) -> None:
        if hasattr(self, '_client') and self._client:
            self._client.close()
            self._client = None
