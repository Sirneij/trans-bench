"""
Run one trial of one configuration and record its timing row.

A trial is one run of one configuration: connect to a system, run its rule file on one input graph, record the
timing row and disconnect. engine/run_one.py calls TrialRunner.run_trial once per process, and
engine/campaign.py starts that process for every run of a campaign, so the CLI (benchmark.py,
transitive.py) and the Web UI measure through this same code.

A system is described by its descriptor.yaml only (engine/loader.py); this module has no knowledge
of any particular system. The connector named by the descriptor's `protocol` does the work.
"""

from __future__ import annotations

import csv
import gc
import logging
from pathlib import Path
from typing import Any, Optional

from engine.connectors import get_connector
from engine.loader import GraphTypeDescriptor, SystemDescriptor

log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_DIR = Path('input')


def input_path(system: SystemDescriptor, graph: str, size: int, input_dir: Path = INPUT_DIR) -> Path:
    """
    Return where generate_db.py writes graph `graph` of size `size` in the input format of `system`.

    The database systems read the tab-separated edge file of Souffle; XSB and Clingo share one
    Prolog fact file; ALDA reads a pickled set of edges.
    """
    fmt = system.input_format
    if fmt == 'tsv':
        return input_dir / 'souffle' / graph / str(size) / 'edge.facts'
    if fmt == 'facts':
        return input_dir / 'souffle' / graph / str(size)
    if fmt == 'lp':
        return input_dir / 'clingo_xsb' / graph / f'graph_{size}.lp'
    if fmt == 'pickle':
        return input_dir / 'alda' / graph / f'graph_{size}.da'
    return input_dir / system.name / graph / f'graph_{size}'


def edge_file(graph: str, size: int, input_dir: Path = INPUT_DIR) -> Path:
    """Return the tab-separated edge file of a graph; engine/verify.py computes the expected closure from it."""
    return input_dir / 'souffle' / graph / str(size) / 'edge.facts'


class TrialRunner:
    """
    Runs single trials and writes their timing rows.

    Parameters
    ----------
    config : dict
        Global configuration (config.yaml), passed to the connectors.
    timing_dir : Path
        Root of the timing files: <timing_dir>/<domain>/<system>/<graph>/<mode>_graph_<n>.csv.
    domain : str
        Benchmark domain; selects the rule files (<domain>_<mode><extension>).
    query_mode : str
        full_materialization (compute the whole closure) or demand_driven (bound query).

    """

    def __init__(
        self,
        config: dict[str, Any],
        timing_dir: Path,
        domain: str = 'transitive',
        query_mode: str = 'full_materialization',
    ):
        """Keep the settings; nothing is opened until run_trial."""
        self.config = config
        self.timing_dir = Path(timing_dir)
        self.domain = domain
        self.query_mode = query_mode
        self.base_dir = BASE_DIR

    def run_trial(self, system: SystemDescriptor, graph: GraphTypeDescriptor, size: int, mode: str) -> dict:
        """
        Run exactly one trial of (system, graph, size, mode).

        Returns {'timing': <row or None>, 'errors': [...], 'memory': ..., 'rule_path', 'input_path',
        'timing_path', 'result_path'}. A missing mode, rule file or input file is reported as an error
        without connecting to the system.
        """
        if mode not in system.modes:
            return {'timing': None, 'errors': [f'{system.name} does not declare mode {mode}']}
        rule_path = self.resolve_rule_path(system, mode)
        if rule_path is None:
            return {'timing': None, 'errors': [f'no rule file for {system.name}/{self.domain}_{mode}']}
        source = input_path(system, graph.name, size)
        if not source.exists():
            return {
                'timing': None,
                'errors': [f'input {source} not found; create it with generate_db.py (see docs/REPRODUCING.md)'],
            }
        output_folder = self.output_folder(system, graph, size, mode)
        timing_path = self.timing_path(system, graph, size, mode)
        outcome = self._run_single(system, rule_path, source, output_folder, timing_path, size)
        outcome.update(
            rule_path=str(rule_path),
            input_path=str(source),
            timing_path=str(timing_path),
            result_path=str(output_folder / system.result_file) if system.result_file else None,
        )
        return outcome

    def _run_single(
        self,
        system: SystemDescriptor,
        rule_path: Path,
        source: Path,
        output_folder: Path,
        timing_path: Path,
        size: int,
    ) -> dict:
        """
        Connect, run one trial, append its timing row, disconnect.

        A trial whose connector recorded errors still gets its timing row (the phases that ran are
        measured; the others are 0), but callers must treat it as failed. If connecting fails, no row
        is written.
        """
        connector = get_connector(system.protocol)()
        outcome: dict = {'timing': None, 'errors': []}
        try:
            connector.connect(system.credentials, system)
            timing = connector.run_experiment(
                rule_path,
                source,
                output_folder,
                system,
                {**self.config, 'query_mode': self.query_mode},
                query_bindings=self._query_bindings(source, size),
            )
            self._write_timing(timing_path, system.csv_headers, timing)
            outcome['timing'] = timing
        except Exception as e:  # pylint: disable=broad-except  # any failure of a system is a result
            msg = f'Experiment failed ({system.name}): {e}'
            log.error(msg)
            outcome['errors'].append(msg)
        finally:
            outcome['errors'] = list(getattr(connector, 'errors', [])) + outcome['errors']
            memory = getattr(connector, 'memory', None)
            outcome['memory'] = memory if isinstance(memory, dict) else None
            try:
                connector.close()
            except Exception as e:  # pylint: disable=broad-except
                log.warning(f'close() failed ({system.name}): {e}')
            gc.collect()
        return outcome

    @staticmethod
    def _query_bindings(source: Path, size: int) -> Optional[dict[str, str]]:
        """Read the bound query of demand-driven mode (queries_<n>.csv next to the input), if there is one."""
        query_file = source.parent / f'queries_{size}.csv'
        if not query_file.exists():
            return None
        with open(query_file, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader, [])
            row = next(reader, [])
        return dict(zip(headers, row)) if headers and row else None

    def resolve_rule_path(self, system: SystemDescriptor, mode: str) -> Optional[Path]:
        """
        Find the rule file of `system` for `mode` in the current domain, or None.

        Tried in order: <domain>_<mode>, then the first word of the domain (transitive_closure ->
        transitive), then <mode> alone; each in the system's rules directory and in the old
        <system>_rules/ directory.
        """
        names = [f'{self.domain}_{mode}']
        prefix = self.domain.split('_')[0]
        if prefix != self.domain:
            names.append(f'{prefix}_{mode}')
        names.append(mode)
        candidates = []
        for name in names:
            candidates += [
                system.rules_dir / f'{name}{system.rule_extension}',
                self.base_dir / f'{system.name}_rules' / f'{name}{system.rule_extension}',
            ]
        for path in candidates:
            if path.exists():
                return path
        log.warning(f'Rule file not found for {system.name}/{self.domain}_{mode}. Tried: {candidates}')
        return None

    def timing_path(self, system: SystemDescriptor, graph: GraphTypeDescriptor, size: int, mode: str) -> Path:
        """Return the CSV file that collects the timing rows of one configuration (its directory is created)."""
        directory = self.timing_dir / self.domain / system.name / graph.name
        directory.mkdir(parents=True, exist_ok=True)
        return directory / f'{mode}_graph_{size}.csv'

    def output_folder(self, system: SystemDescriptor, graph: GraphTypeDescriptor, size: int, mode: str) -> Path:
        """Return the directory where the system writes its query result for one configuration (created)."""
        folder = self.timing_dir / self.domain / system.name / graph.name / mode / str(size)
        folder.mkdir(parents=True, exist_ok=True)
        return folder

    @staticmethod
    def _write_timing(timing_path: Path, headers: list[str], timing: dict[str, float]) -> None:
        """Append one row (and the header, for a new file) in the column order of the descriptor."""
        is_new = not timing_path.exists()
        with open(timing_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if is_new:
                writer.writerow(headers)
            writer.writerow([timing.get(h, 0.0) for h in headers])
