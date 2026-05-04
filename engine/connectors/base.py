"""
engine/connectors/base.py

Abstract base class for all connectors.  A connector encapsulates
the logic to run one timed experiment for a specific system.

To add a new system with an existing protocol (e.g. another SQL database
over psycopg2), you only need a new descriptor.yaml — no new connector.

To add a genuinely new protocol, implement BaseConnector here and register
it in engine/connectors/__init__.py.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from time import perf_counter, process_time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from engine.loader import SystemDescriptor

log = logging.getLogger(__name__)


class BaseConnector(ABC):
    """
    Interface every connector must satisfy.

    Lifecycle:
        connector = ConcreteConnector()
        connector.connect(credentials, descriptor)
        timing = connector.run_experiment(rule_path, input_path, output_folder, descriptor, config)
        connector.close()
    """

    def __init__(self):
        self._connection = None

    # ------------------------------------------------------------------
    # Must override
    # ------------------------------------------------------------------

    @abstractmethod
    def connect(self, credentials: dict[str, Any], descriptor: 'SystemDescriptor') -> None:
        """Establish a connection to the system."""

    @abstractmethod
    def run_experiment(
        self,
        rule_path: Path,
        input_path: Path,
        output_folder: Path,
        descriptor: 'SystemDescriptor',
        config: dict[str, Any],
    ) -> dict[str, float]:
        """
        Run one complete benchmark trial and return a dict mapping
        CSV header names to their measured float values.

        e.g. {'CreateTableRealTime': 0.012, 'CreateTableCPUTime': 0.003, ...}
        """

    @abstractmethod
    def close(self) -> None:
        """Release all resources."""

    # ------------------------------------------------------------------
    # Shared utilities
    # ------------------------------------------------------------------

    @staticmethod
    def timed(fn, *args, **kwargs) -> tuple[float, float, Any]:
        """
        Call fn(*args, **kwargs), return (real_seconds, cpu_seconds, result).
        """
        t0_cpu = process_time()
        t0 = perf_counter()
        result = fn(*args, **kwargs)
        real = perf_counter() - t0
        cpu = process_time() - t0_cpu
        return real, cpu, result

    @staticmethod
    def build_timing_row(phases: list, measurements: list[tuple[float, float]]) -> dict[str, float]:
        """
        Zip a list of TimingPhase objects with (real, cpu) measurements into
        the flat dict that gets written to CSV.
        """
        row: dict[str, float] = {}
        for phase, (real, cpu) in zip(phases, measurements):
            row[f'{phase.label}RealTime'] = real
            row[f'{phase.label}CPUTime'] = cpu
        return row
