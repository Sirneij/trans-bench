"""
Define the interface of a connector and the timing helpers that all connectors share.

A connector holds the logic that runs one timed trial on one kind of system. A new system that
speaks an existing protocol (e.g. another SQL database reached through psycopg2) needs only a
descriptor.yaml. A new protocol needs a subclass of BaseConnector, registered in
engine/connectors/__init__.py.
"""

from __future__ import annotations

import importlib.util
import logging
import sys
from abc import ABC, abstractmethod
from pathlib import Path
from time import perf_counter, process_time
from types import ModuleType
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from engine.loader import SystemDescriptor

log = logging.getLogger(__name__)


def import_file(module_name: str, path: Path, register: bool = True) -> ModuleType:
    """Import the Python file `path` as module `module_name` (also in sys.modules if `register`)."""
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f'cannot import {path} as {module_name}')
    module = importlib.util.module_from_spec(spec)
    if register:
        sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class BaseConnector(ABC):
    """
    Interface every connector must satisfy.

    Lifecycle:
        connector = ConcreteConnector()
        connector.connect(credentials, descriptor)
        timing = connector.run_experiment(rule_path, input_path, output_folder, descriptor, config)
        connector.close()

    Error reporting:
        A failing step must not be hidden behind a timing row of zeros. Connectors catch
        exceptions so that the remaining cleanup still runs, but record every failure with
        ``self._record_error(msg)``. Callers (engine/run_one.py, the Web UI) read
        ``connector.errors`` after ``run_experiment`` and treat a non-empty list as a failed run.
    """

    def __init__(self):
        """Start with no connection, no errors and no memory measurement."""
        self._connection = None
        self.errors: list[str] = []
        # memory used by the query phase (engine/memory.py MemorySampler.result()), or None
        self.memory: dict | None = None

    def _record_error(self, message: str) -> None:
        """Log an error and keep it in ``self.errors`` so the caller can mark the run as failed."""
        logging.getLogger(type(self).__module__).error(message)
        self.errors.append(message)

    def _substitute_query_bindings(self, rule_content: str, query_bindings: dict[str, Any] | None) -> str:
        """
        Replace every ?name placeholder in the rule text with its value from `query_bindings`.

        The bindings come from queries_<n>.csv in demand-driven mode, e.g. {'X': '10'}; without
        bindings the text is returned as it is.
        """
        if not query_bindings:
            return rule_content

        result = rule_content
        for param_name, param_value in query_bindings.items():
            # Substitute ?param_name with the actual value
            placeholder = f'?{param_name}'
            result = result.replace(placeholder, str(param_value))

        return result

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
        Run one complete trial and return the timing row.

        The row maps CSV header names to measured values, e.g.
        {'CreateTableRealTime': 0.012, 'CreateTableCPUTime': 0.003, ...}.
        """

    @abstractmethod
    def close(self) -> None:
        """Release all resources."""

    # ------------------------------------------------------------------
    # May override
    # ------------------------------------------------------------------

    @classmethod
    def cancel_running(cls, credentials: dict[str, Any], descriptor: 'SystemDescriptor') -> None:
        """
        Stop work that is still running inside the server after the client process was killed.

        engine/campaign.py calls this when a run exceeds its time limit or is stopped. Killing the
        client alone is not enough for client/server systems: the server keeps executing the
        statement, and the next run then measures a loaded machine or fails on tables that are
        still in use. The default does nothing, which is right for in-process and subprocess systems.
        """

    # ------------------------------------------------------------------
    # Shared utilities
    # ------------------------------------------------------------------

    @staticmethod
    def result_path(output_folder: Path, descriptor: 'SystemDescriptor', default: str) -> Path:
        """Return where the run's query result goes: descriptor.result_file, or the connector's default name."""
        return Path(output_folder) / (getattr(descriptor, 'result_file', '') or default)

    def memory_sampler(self):
        """
        Return a MemorySampler (engine/memory.py) for whatever executes this system's query.

        The default is None (no probe); connectors override it.
        """
        return None

    def timed_query(self, fn, *args, **kwargs) -> tuple[float, float, Any]:
        """Time the query phase like timed(), and sample the query's memory into self.memory."""
        # the base class returns None; connectors with a memory probe override memory_sampler()
        sampler = self.memory_sampler()  # pylint: disable=assignment-from-none
        if sampler is None:
            return self.timed(fn, *args, **kwargs)
        with sampler:  # pylint: disable=not-context-manager
            result = self.timed(fn, *args, **kwargs)
        self.memory = sampler.result()
        return result

    @staticmethod
    def timed(fn, *args, **kwargs) -> tuple[float, float, Any]:
        """Call fn(*args, **kwargs) and return (real_seconds, cpu_seconds, result)."""
        t0_cpu = process_time()
        t0 = perf_counter()
        result = fn(*args, **kwargs)
        real = perf_counter() - t0
        cpu = process_time() - t0_cpu
        return real, cpu, result

    @staticmethod
    def timed_subprocess(cmd: list[str], samples: list | None = None, **kwargs) -> tuple[float, float, float, Any]:
        """
        Run a subprocess and return (real_seconds, cpu_seconds, max_rss_mb, CompletedProcess).

        The memory of the process and its children is polled every 10 ms. If `samples` is a list,
        the (seconds since start, rss bytes) samples are appended to it.
        """
        import subprocess
        import threading

        import psutil

        # communicate() below waits for the process; a with-block would add nothing to the measurement
        # pylint: disable-next=consider-using-with
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, **kwargs)

        max_rss = 0.0
        stop_polling = threading.Event()

        def poll_memory():
            """Record the largest resident memory of the process and its children until it ends."""
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
                    if samples is not None:
                        samples.append((perf_counter() - t_start, mem))
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    break
                stop_polling.wait(0.01)

        t_start = perf_counter()
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
        """Zip TimingPhase objects with (real, cpu) measurements into the flat row written to CSV."""
        row: dict[str, float] = {}
        for i, (phase, (real, cpu)) in enumerate(zip(phases, measurements)):
            row[f'{phase.label}RealTime'] = real
            row[f'{phase.label}CPUTime'] = cpu
            if memory and i < len(memory):
                row[f'{phase.label}MaxRAM_MB'] = memory[i]
        return row
