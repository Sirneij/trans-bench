"""
engine/runner.py

The ExperimentRunner orchestrates the full benchmarking lifecycle:

  1. Load system and graph-type descriptors
  2. Generate missing input data
  3. For each (system × graph_type × size × mode):
       a. Resolve connector from descriptor.protocol
       b. Find the rule file
       c. Connect, run, collect timings, write CSV
  4. Generate LaTeX plots

Adding a new system requires ZERO changes here — only a new descriptor.yaml.
"""
from __future__ import annotations

import csv
import gc
import json
import logging
import shutil
import subprocess
from pathlib import Path
from typing import Any, Callable, Optional

from engine.connectors import get_connector
from engine.loader import DescriptorLoader, GraphTypeDescriptor, SystemDescriptor

log = logging.getLogger(__name__)


class ExperimentRunner:
    """
    Drives the entire experiment lifecycle.

    Parameters
    ----------
    config : dict
        Global configuration (merged from config.yaml + credentials).
    systems : list[SystemDescriptor]
        Systems to benchmark (pre-loaded by DescriptorLoader).
    graph_types : list[GraphTypeDescriptor]
        Graph topologies to test.
    size_range : list[int]
        [start, stop, step] for range().
    num_runs : int
        Number of repetitions per (system, graph, size, mode) combination.
    modes : list[str]
        Recursion modes to benchmark.
    progress_cb : callable, optional
        Called with a progress dict on every step — used by the Web UI for SSE.
    """

    def __init__(
        self,
        config: dict[str, Any],
        systems: list[SystemDescriptor],
        graph_types: list[GraphTypeDescriptor],
        size_range: list[int],
        num_runs: int,
        modes: list[str],
        progress_cb: Optional[Callable[[dict], None]] = None,
    ):
        self.config = config
        self.systems = systems
        self.graph_types = graph_types
        self.size_range = size_range
        self.num_runs = num_runs
        self.modes = modes
        self.progress_cb = progress_cb
        self.timing_dir = Path(config.get('timing_dir', 'timing'))
        self.base_dir = Path(__file__).parent.parent

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self) -> None:
        self._clean_empty_timing_dirs()

        total = len(self.systems) * len(self.graph_types) * len(range(*self.size_range)) * len(self.modes)
        done = 0

        for system in self.systems:
            self._generate_input_data(system)
            for graph in self.graph_types:
                for size in range(*self.size_range):
                    # Skip if all modes already have data
                    if self._all_modes_exist(system, graph, size):
                        log.info(f'Skipping {system.name}/{graph.name}/{size} — data exists')
                        done += len(self.modes)
                        continue

                    for mode in self.modes:
                        if mode not in system.modes:
                            done += 1
                            continue

                        rule_path = self._resolve_rule_path(system, mode)
                        if rule_path is None:
                            log.warning(f'No rule file for {system.name}/{mode} — skipping')
                            done += 1
                            continue

                        input_path = self._resolve_input_path(system, graph, size)
                        output_folder = self._prepare_output_folder(system, graph, size, mode)
                        timing_path = self._timing_path(system, graph, size, mode)

                        log.info(f'Running: {system.name} / {graph.name} / {size} / {mode}')
                        self._emit(done, total, system.name, graph.name, size, mode, 'running')

                        for _ in range(self.num_runs):
                            self._run_single(system, rule_path, input_path, output_folder, timing_path)

                        self._append_average(timing_path)
                        done += 1
                        self._emit(done, total, system.name, graph.name, size, mode, 'done')

        max_x = list(range(*self.size_range))[-1] if self.size_range else 1000
        config_str = json.dumps(self.config)
        subprocess.run(['python', 'generate_plot_table.py', '--config', config_str, '--max-x-axis', str(max_x)])

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _run_single(
        self,
        system: SystemDescriptor,
        rule_path: Path,
        input_path: Path,
        output_folder: Path,
        timing_path: Path,
    ) -> None:
        ConnectorClass = get_connector(system.protocol)
        connector = ConnectorClass()
        try:
            connector.connect(system.credentials, system)
            timing = connector.run_experiment(
                rule_path, input_path, output_folder, system, self.config
            )
            self._write_timing(timing_path, system.csv_headers, timing)
        except Exception as e:
            msg = f'Experiment failed ({system.name}): {e}'
            log.error(msg)
            self._emit_log(msg, level='error')
        finally:
            connector.close()
            gc.collect()

    def _resolve_rule_path(self, system: SystemDescriptor, mode: str) -> Optional[Path]:
        """Find rule file for this system+mode. Checks new systems/ dir, then legacy *_rules/ dir."""
        candidates = [
            system.rules_dir / f'transitive_{mode}{system.rule_extension}',
            self.base_dir / f'{system.name}_rules' / f'transitive_{mode}{system.rule_extension}',
        ]
        for path in candidates:
            if path.exists():
                return path
        log.warning(f'Rule file not found for {system.name}/{mode}. Tried: {candidates}')
        return None

    def _resolve_input_path(self, system: SystemDescriptor, graph: GraphTypeDescriptor, size: int) -> Path:
        input_dir = Path('input')
        fmt = system.input_format

        if fmt == 'tsv':
            return input_dir / 'souffle' / graph.name / str(size) / 'edge.facts'
        elif fmt == 'lp':
            return input_dir / 'clingo_xsb' / graph.name / f'graph_{size}.lp'
        elif fmt == 'facts':
            return input_dir / 'souffle' / graph.name / str(size)
        elif fmt == 'pickle':
            return input_dir / 'alda' / graph.name / f'graph_{size}.pkl'
        else:
            return input_dir / system.name / graph.name / f'graph_{size}'

    def _timing_path(self, system: SystemDescriptor, graph: GraphTypeDescriptor, size: int, mode: str) -> Path:
        d = self.timing_dir / system.name / graph.name
        d.mkdir(parents=True, exist_ok=True)
        return d / f'timing_{mode}_graph_{size}.csv'

    def _prepare_output_folder(
        self,
        system: SystemDescriptor,
        graph: GraphTypeDescriptor,
        size: int,
        mode: str,
    ) -> Path:
        folder = self.timing_dir / system.name / graph.name / mode / str(size)
        folder.mkdir(parents=True, exist_ok=True)
        return folder

    def _all_modes_exist(self, system: SystemDescriptor, graph: GraphTypeDescriptor, size: int) -> bool:
        return all(
            self._timing_path(system, graph, size, mode).exists()
            for mode in self.modes
            if mode in system.modes
        )

    def _write_timing(self, timing_path: Path, headers: list[str], timing: dict[str, float]) -> None:
        is_new = not timing_path.exists()
        with open(timing_path, 'a', newline='') as f:
            writer = csv.writer(f)
            if is_new:
                writer.writerow(headers)
            writer.writerow([timing.get(h, 0.0) for h in headers])

    def _append_average(self, timing_path: Path) -> None:
        if not timing_path.exists():
            return
        with open(timing_path, newline='') as f:
            reader = csv.reader(f)
            headers = next(reader, None)
            if not headers:
                return
            sums = [0.0] * len(headers)
            counts = [0] * len(headers)
            for row in reader:
                if row and row[0] == 'Average':
                    continue
                for i, val in enumerate(row):
                    if i < len(headers):
                        try:
                            sums[i] += float(val)
                            counts[i] += 1
                        except ValueError:
                            pass

        averages = [s / c if c else 0.0 for s, c in zip(sums, counts)]
        with open(timing_path, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Average'] + averages)

    def _generate_input_data(self, system: SystemDescriptor) -> None:
        fmt = system.input_format
        if fmt in ('tsv', 'facts'):
            env_key = 'souffle'
        elif fmt == 'lp':
            env_key = 'clingo'
        elif fmt == 'pickle':
            env_key = 'alda'
        else:
            env_key = system.name

        input_dir = Path('input')
        graph_names = [g.name for g in self.graph_types]
        missing = []

        for graph in self.graph_types:
            graph_dir = input_dir / env_key / graph.name
            if not graph_dir.exists():
                missing.append(graph.name)
            else:
                required = set(range(*self.size_range))
                if fmt in ('tsv', 'facts'):
                    existing = {int(d.name) for d in graph_dir.iterdir() if d.is_dir()}
                else:
                    existing = {int(f.stem.split('_')[-1]) for f in graph_dir.glob('*.*')}
                if required - existing:
                    missing.append(graph.name)

        if missing:
            log.info(f'Generating input for {system.name}: missing graphs {missing}')
            self._emit_log(f'Generating input data for graphs: {missing}', level='info')
            config_str = json.dumps(self.config)
            result = subprocess.run(
                [
                    'python', 'generate_db.py',
                    '--config', config_str,
                    '--sizes', str(self.size_range[0]), str(self.size_range[1]), str(self.size_range[2]),
                    '--graph-types', *missing,
                ],
                capture_output=True,
                text=True,
            )
            # Emit stdout lines so they appear in the Web UI live log
            for line in (result.stdout + result.stderr).splitlines():
                if line.strip():
                    level = 'error' if ('error' in line.lower() or 'traceback' in line.lower()) else 'info'
                    self._emit_log(f'[generate_db] {line}', level=level)
            if result.returncode != 0:
                self._emit_log(f'generate_db.py exited with code {result.returncode}', level='error')

    def _clean_empty_timing_dirs(self) -> None:
        if not self.timing_dir.exists():
            return
        for subdir in self.timing_dir.iterdir():
            if subdir.is_dir() and not list(subdir.glob('**/*.csv')):
                shutil.rmtree(subdir)

    def _emit(self, done: int, total: int, system: str, graph: str, size: int, mode: str, status: str) -> None:
        if self.progress_cb:
            self.progress_cb({
                'type': 'progress',
                'done': done,
                'total': total,
                'pct': round(100 * done / max(total, 1), 1),
                'system': system,
                'graph': graph,
                'size': size,
                'mode': mode,
                'status': status,
            })

    def _emit_log(self, message: str, level: str = 'info') -> None:
        """Emit a plain log line (not a progress update) through the progress callback."""
        if self.progress_cb:
            self.progress_cb({
                'type': 'log',
                'message': message,
                'level': level,
            })
