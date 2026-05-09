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
        query_bindings: dict[str, Any] | None = None,
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
    def timed_subprocess(cmd: list[str], **kwargs) -> tuple[float, float, float, Any]:
        """
        Run a subprocess, returning (real_seconds, cpu_seconds, max_rss_mb, CompletedProcess).
        """
        import subprocess
        import threading

        import psutil

        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, **kwargs)

        max_rss = 0.0
        stop_polling = threading.Event()

        def poll_memory():
            nonlocal max_rss
            try:
                ps_proc = psutil.Process(proc.pid)
            except psutil.NoSuchProcess:
                return
            while not stop_polling.is_set():
                try:
                    mem = ps_proc.memory_info().rss
                    for child in ps_proc.children(recursive=True):
                        mem += child.memory_info().rss
                    max_rss = max(max_rss, mem)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    break
                stop_polling.wait(0.01)

        t = threading.Thread(target=poll_memory, daemon=True)
        t.start()

        t0_cpu = process_time()
        t0 = perf_counter()

        stdout, stderr = proc.communicate()

        real = perf_counter() - t0
        cpu = process_time() - t0_cpu

        stop_polling.set()
        t.join(timeout=0.2)

        result = subprocess.CompletedProcess(proc.args, proc.returncode, stdout, stderr)
        return real, cpu, max_rss / (1024 * 1024), result

    @staticmethod
    def build_timing_row(
        phases: list, measurements: list[tuple[float, float]], memory: list[float] | None = None
    ) -> dict[str, float]:
        """
        Zip a list of TimingPhase objects with (real, cpu) measurements into
        the flat dict that gets written to CSV.
        """
        row: dict[str, float] = {}
        for i, (phase, (real, cpu)) in enumerate(zip(phases, measurements)):
            row[f'{phase.label}RealTime'] = real
            row[f'{phase.label}CPUTime'] = cpu
            if memory and i < len(memory):
                row[f'{phase.label}MaxRAM_MB'] = memory[i]
        return row
