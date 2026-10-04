"""
Measure the memory used by a query by sampling it while the query runs.

A *probe* is a zero-argument function that returns the current memory (bytes) of whatever
executes the query: the benchmark process itself (DuckDB), the server backend serving the
connection (PostgreSQL), the server process (MariaDB, CockroachDB, MongoDB), or the server's own
memory accounting (Neo4j, SingleStore). `MemorySampler` calls the probe just before the query,
then every `interval` seconds in a background thread while the query runs, and once after it,
and reports

    before_mb   memory just before the query
    peak_mb     highest sample (including the one after the query)
    used_mb     peak_mb - before_mb: the memory the query added (what the paper reports)

Sampling can miss a peak that lasts less than the interval, and resident memory (RSS) counts
pages the operating system has not reclaimed yet; see docs/VERIFICATION.md for what each
system's probe measures.
"""

from __future__ import annotations

import os
import threading
from typing import Callable, Optional

MB = 1024 * 1024


class MemorySampler:
    """Context manager that samples a memory probe around and during the timed query."""

    def __init__(self, probe: Callable[[], Optional[float]], name: str, interval: float = 0.01):
        """Keep the probe (bytes or None), the name reported with the result, and the sampling interval."""
        self.probe, self.name, self.interval = probe, name, interval
        self.before: Optional[float] = None
        self.peak: Optional[float] = None
        self.samples = 0
        self.errors: list[str] = []
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _sample(self) -> None:
        try:
            v = self.probe()
        except Exception as e:  # e.g. the process ended; never let sampling break the query
            if len(self.errors) < 3:
                self.errors.append(f'{type(e).__name__}: {e}')
            return
        if v is None:
            return
        self.samples += 1
        self.peak = v if self.peak is None else max(self.peak, v)

    def _loop(self) -> None:
        while not self._stop.wait(self.interval):
            self._sample()

    def __enter__(self) -> 'MemorySampler':
        """Take the sample before the query and start the sampling thread."""
        try:
            self.before = self.probe()
        except Exception as e:
            self.errors.append(f'{type(e).__name__}: {e}')
        if self.before is not None:
            self.peak = self.before
            self.samples = 1
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        """Stop the sampling thread and take one last sample after the query."""
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        self._sample()

    def result(self) -> Optional[dict]:
        """Return before_mb, peak_mb and used_mb, or the reason why there is no measurement."""
        if self.before is None or self.peak is None:
            return {'probe': self.name, 'error': '; '.join(self.errors) or 'no samples'}
        return {
            'probe': self.name,
            'before_mb': round(self.before / MB, 3),
            'peak_mb': round(self.peak / MB, 3),
            'used_mb': round((self.peak - self.before) / MB, 3),
            'samples': self.samples,
            'interval_s': self.interval,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Probes
# ─────────────────────────────────────────────────────────────────────────────


def rss_of_pid(pid: int) -> Callable[[], float]:
    """Return a probe of the resident set size of one process."""
    import psutil

    proc = psutil.Process(pid)
    return lambda: float(proc.memory_info().rss)


def rss_of_self() -> Callable[[], float]:
    """Return a probe of the resident set size of this process (for in-process systems such as DuckDB)."""
    return rss_of_pid(os.getpid())


def find_server_pid(name: str, cmdline_contains: str | None = None) -> int:
    """
    Return the PID of the single running process called `name`.

    When several such servers run, `cmdline_contains` (e.g. a port) selects one of them; anything
    other than exactly one match is an error.
    """
    import psutil

    found = []
    for p in psutil.process_iter(['name', 'cmdline']):
        try:
            if p.info['name'] != name:
                continue
            if cmdline_contains and cmdline_contains not in ' '.join(p.info['cmdline'] or []):
                continue
            found.append(p.pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    if len(found) != 1:
        raise RuntimeError(
            f'expected one {name!r} process{" with " + cmdline_contains if cmdline_contains else ""}, '
            f'found {len(found)}'
        )
    return found[0]


def sampler_or_none(
    make_probe: Callable[[], Callable[[], Optional[float]]], name: str, interval: float = 0.01
) -> Optional[MemorySampler]:
    """Create a MemorySampler, or return None (with the reason logged) if the probe cannot be created."""
    import logging

    try:
        return MemorySampler(make_probe(), name, interval)
    except Exception as e:
        logging.getLogger(__name__).warning(f'memory probe {name!r} unavailable: {e}')
        return None
