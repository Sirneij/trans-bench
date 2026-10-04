"""
Run benchmark campaigns: the one execution engine of trans-bench.

This is the one execution engine of trans-bench. benchmark.py, transitive.py and the Web UI all describe
what to run as a CampaignSpec and hand it to Campaign.run(); the rules below therefore hold for
every measurement, whichever way it was started.

  * Every run is a separate process (python -m engine.run_one), started in its own process group.
  * A run that exceeds the time limit is killed as a whole process group, and the query still
    running inside the server is cancelled (connector.cancel_running). It is recorded as `timeout`.
  * A run that reports an error is recorded as `error` (with the kind of failure, engine/failures.py).
  * After a failed run, the remaining runs of that configuration are not started, and all larger n
    of the same (system, graph, mode) are recorded as `skipped`.
  * In the transitive-closure domain with full materialization, every successful result file is
    checked against the closure computed independently (engine/verify.py): row count and a 64-bit
    order-independent hash. The result file is then deleted (results of large graphs have several GB).
  * Missing input graphs are generated first with generate_db.py.
  * Everything is appended to <campaign>/<series>/runs.jsonl (schema in docs/VERIFICATION.md), with
    the full output of each run in logs/ and the timing rows in timing/ of the same directory.

A campaign can be stopped and started again with the same spec: configurations that already have
records in runs.jsonl are not run again. A run that is stopped part-way is not recorded, so it is
repeated on the next start.

Progress is reported through the `on_event` callback as plain dicts (see Campaign.run), which the CLI
prints and the Web UI streams to the browser.
"""

from __future__ import annotations

import csv
import json
import os
import shlex
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from engine import verify
from engine.connectors import get_connector
from engine.failures import classify_failure
from engine.loader import DescriptorLoader, SystemDescriptor
from engine.run_one import RESULT_MARKER
from engine.runner import edge_file, input_path

BASE_DIR = Path(__file__).resolve().parent.parent
RESULTS_DIR = BASE_DIR / 'results'
GRAPHS = [
    'complete',
    'max_acyclic',
    'cycle',
    'cycle_with_shortcuts',
    'path',
    'multi_path',
    'grid',
    'binary_tree',
    'reverse_binary_tree',
    'x',
    'y',
    'w',
]
MODES = ['left_recursion', 'right_recursion']
VERIFIED_DOMAINS = ('transitive', 'transitive_closure')
POLL_S = 0.5  # how often a running trial is checked for the time limit, a stop request and new output

Event = dict[str, Any]


class CampaignError(Exception):
    """The campaign cannot start (unknown system, inputs that could not be generated, bad sizes)."""


def now() -> str:
    """Return the current UTC time in ISO 8601, to the second (the format of `start` and `end` in runs.jsonl)."""
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


@dataclass
class CampaignSpec:  # pylint: disable=too-many-instance-attributes  # one field per option of the CLI
    """What to run. Every field has the meaning of the benchmark.py option of the same name."""

    systems: list[str]
    sizes: list[int]
    campaign_dir: Path
    graphs: list[str] = field(default_factory=lambda: list(GRAPHS))
    modes: list[str] = field(default_factory=lambda: list(MODES))
    runs: int = 5
    timeout: float = 600.0
    series_suffix: str = ''
    label: str = ''
    domain: str = 'transitive'
    query_mode: str = 'full_materialization'
    config_file: Path = BASE_DIR / 'config.yaml'
    expected_max_n: int = 20000
    expected_cache: Path = BASE_DIR / verify.DEFAULT_CACHE
    keep_results: bool = False
    souffle_include_dir: Optional[str] = None

    def __post_init__(self) -> None:
        """Make the paths absolute, so the spec does not depend on the working directory."""
        self.campaign_dir = Path(self.campaign_dir).resolve()
        self.config_file = Path(self.config_file).resolve()
        self.expected_cache = Path(self.expected_cache).resolve()
        self.sizes = [int(n) for n in self.sizes]

    def validate(self) -> None:
        """Raise CampaignError for a spec that cannot be run."""
        if not self.systems:
            raise CampaignError('choose at least one system')
        if not self.graphs or not self.modes or not self.sizes:
            raise CampaignError('choose at least one graph, mode and size')
        if self.sizes != sorted(set(self.sizes)) or self.sizes[0] < 1:
            raise CampaignError('sizes must be positive and increasing (after a failure, larger sizes are skipped)')
        if self.runs < 1 or self.timeout <= 0:
            raise CampaignError('runs and the time limit must be positive')

    @property
    def verifies(self) -> bool:
        """Whether result files are checked against the transitive closure."""
        return self.domain in VERIFIED_DOMAINS and self.query_mode == 'full_materialization'

    def series_dir(self, system: str) -> Path:
        """Directory of one series: <campaign>/<system><suffix>, e.g. results/my_run/mariadb_tuned."""
        return self.campaign_dir / f'{system}{self.series_suffix}'

    def command(self) -> str:
        """Return the benchmark.py command that runs this campaign from a terminal."""
        try:
            where = self.campaign_dir.relative_to(BASE_DIR)
        except ValueError:
            where = self.campaign_dir
        args = [
            'python',
            'benchmark.py',
            '--systems',
            *self.systems,
            '--graphs',
            *self.graphs,
            '--modes',
            *self.modes,
            '--sizes',
            *map(str, self.sizes),
            '--runs',
            str(self.runs),
            '--timeout',
            f'{self.timeout:g}',
        ]
        if len(self.systems) == 1 and self.series_suffix:
            args += ['--out', str(where / f'{self.systems[0]}{self.series_suffix}')]
        else:
            args += ['--campaign', str(where)]
        if self.label:
            args += ['--label', self.label]
        if self.domain != 'transitive':
            args += ['--domain', self.domain]
        if self.query_mode != 'full_materialization':
            args += ['--query-mode', self.query_mode]
        return shlex.join(args)


def csv_rows(path: Path) -> list[dict]:
    """Read the rows of a timing CSV file as dicts (an empty list if the file does not exist)."""
    if not path.exists():
        return []
    with open(path, newline='', encoding='utf-8') as f:
        return [r for r in csv.DictReader(f) if r]


def parse_outcome(log_text: str) -> Optional[dict]:
    """Parse the RUN_ONE_RESULT line printed by engine/run_one.py; None if there is none (a killed process)."""
    for line in reversed(log_text.splitlines()):
        if line.startswith(RESULT_MARKER):
            return json.loads(line[len(RESULT_MARKER) :])
    return None


def log_errors(log_text: str) -> list[str]:
    """ERROR lines of a run's log (the format of logging.basicConfig in engine/run_one.py)."""
    return [ln.split(' - ERROR: ', 1)[1][:500] for ln in log_text.splitlines() if ' - ERROR: ' in ln]


def read_done(runs_file: Path) -> set[tuple]:
    """(system, graph, mode, n) of every configuration that already has records in runs.jsonl."""
    if not runs_file.exists():
        return set()
    done = set()
    for line in runs_file.read_text().splitlines():
        if line.strip():
            r = json.loads(line)
            done.add((r['system'], r['graph'], r['mode'], r['n']))
    return done


class Campaign:
    """
    Runs a CampaignSpec.

    Parameters
    ----------
    spec : CampaignSpec
        What to run.
    on_event : callable, optional
        Receives every event as a dict. `type` is one of:
          log      {'message', 'level'}: a line for the reader (level info, warn or error);
          progress {'done', 'total', 'pct', 'system', 'graph', 'size', 'mode', 'status', 'outcome'}:
                   a configuration starts (status running) or ends (status done, outcome ok, a
                   failure kind, skipped or resumed);
          run      the record of one run as written to runs.jsonl, without the command line.
    should_stop : callable, optional
        Polled while the campaign runs; when it returns True, the running trial is killed (and not
        recorded) and the campaign ends.

    """

    def __init__(
        self,
        spec: CampaignSpec,
        on_event: Optional[Callable[[Event], None]] = None,
        should_stop: Optional[Callable[[], bool]] = None,
    ):
        """Keep the spec and the callbacks; the descriptors are loaded by prepare()."""
        self.spec = spec
        self.on_event = on_event or (lambda event: None)
        self.should_stop = should_stop or (lambda: False)
        self.stopped = False
        self.loader = DescriptorLoader(base_dir=BASE_DIR, config_path=spec.config_file, detect_versions=False)
        self.systems: list[SystemDescriptor] = []
        self.modes: dict[str, list[str]] = {}
        self.done = 0
        self.total = 0

    # ── events ───────────────────────────────────────────────────────────────

    def log(self, message: str, level: str = 'info') -> None:
        """Send one log line to the listener."""
        self.on_event({'type': 'log', 'message': message, 'level': level})

    def _progress(self, system: str, graph: str, size: int, mode: str, status: str, outcome: str = '') -> None:
        self.on_event(
            {
                'type': 'progress',
                'done': self.done,
                'total': self.total,
                'pct': round(100 * self.done / max(self.total, 1), 1),
                'system': system,
                'graph': graph,
                'size': size,
                'mode': mode,
                'status': status,
                'outcome': outcome,
            }
        )

    # ── plan ─────────────────────────────────────────────────────────────────

    def prepare(self) -> dict:
        """
        Load the descriptors and work out which modes each system runs; returns the plan.

        The plan is {'systems', 'graphs', 'sizes', 'modes', 'system_modes'}, in the order of the runs.
        A mode is run for a system if the system declares it and, when the domain has a descriptor
        with its own list of modes, the domain declares it too.
        """
        spec = self.spec
        spec.validate()
        self.systems = []
        for name in spec.systems:
            system = self.loader.get_system(name)
            if system is None:
                raise CampaignError(f'unknown system {name!r} (no systems/{name}/descriptor.yaml)')
            self.systems.append(system)
        known = {g.name for g in self.loader.load_graph_types()}
        unknown = [g for g in spec.graphs if g not in known]
        if unknown:
            raise CampaignError(f'unknown graph types {unknown} (no graph_types/<name>.yaml)')
        domain = self.loader.get_domain(spec.domain)
        allowed = set(domain.modes) if domain and domain.modes else None
        self.modes = {
            s.name: [m for m in spec.modes if m in s.modes and (allowed is None or m in allowed)] for s in self.systems
        }
        self.total = sum(len(m) for m in self.modes.values()) * len(spec.graphs) * len(spec.sizes)
        self.done = 0
        return {
            'systems': [s.name for s in self.systems],
            'graphs': list(spec.graphs),
            'sizes': list(spec.sizes),
            'modes': list(spec.modes),
            'system_modes': {k: list(v) for k, v in self.modes.items()},
        }

    # ── inputs ───────────────────────────────────────────────────────────────

    def missing_inputs(self) -> dict[str, list[int]]:
        """Map each graph to the sizes whose input files (for any chosen system, or for verification) do not exist."""
        missing: dict[str, list[int]] = {}
        for graph in self.spec.graphs:
            for n in self.spec.sizes:
                paths = [input_path(s, graph, n, BASE_DIR / 'input') for s in self.systems if self.modes[s.name]]
                if self.spec.verifies:
                    paths.append(edge_file(graph, n, BASE_DIR / 'input'))
                if any(not p.exists() for p in paths):
                    missing.setdefault(graph, []).append(n)
        return missing

    def generate_inputs(self) -> None:
        """Write the missing input graphs with generate_db.py, one process per graph type."""
        for graph, sizes in self.missing_inputs().items():
            if self.should_stop():
                return
            self.log(f'Generating {graph} inputs for n = {", ".join(map(str, sizes))}')
            cmd = [sys.executable, 'generate_db.py', '--graph-types', graph, '--size-list', *map(str, sizes)]
            if self.spec.domain not in VERIFIED_DOMAINS:
                cmd += ['--domain', self.spec.domain]
            with subprocess.Popen(cmd, cwd=BASE_DIR, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as p:
                for line in p.stdout or []:
                    if line.strip():
                        self.log(f'[generate_db] {line.rstrip()}', 'error' if 'ERROR' in line else 'info')
            if p.returncode != 0:
                raise CampaignError(f'generate_db.py failed for {graph} (exit code {p.returncode})')
        still = self.missing_inputs()
        if still:
            raise CampaignError(f'inputs still missing after generation: {still}')

    # ── runs ─────────────────────────────────────────────────────────────────

    def run(self) -> dict:
        """Run the whole campaign; returns counts {'ok', 'failed', 'skipped', 'resumed', 'stopped'}."""
        plan = self.prepare()
        self.log(
            f'Campaign {self.spec.campaign_dir.name}: {self.total} configurations, {self.spec.runs} runs each, '
            f'limit {self.spec.timeout:g} s per run'
        )
        for system in self.systems:
            dropped = [m for m in self.spec.modes if m not in plan['system_modes'][system.name]]
            if dropped:
                self.log(f'{system.name} has no mode {", ".join(dropped)} in this domain; not run', 'warn')
        self.generate_inputs()
        counts = {'ok': 0, 'failed': 0, 'skipped': 0, 'resumed': 0, 'stopped': False}
        for system in self.systems:
            if not self._run_series(system, counts):
                counts['stopped'] = self.stopped = True
                self.log(
                    'Stopped on request. The run in progress was not recorded; starting the same '
                    'campaign again repeats it.',
                    'warn',
                )
                break
        return counts

    def _run_series(self, system: SystemDescriptor, counts: dict) -> bool:
        """Run every configuration of one system into its series directory; False if stopped."""
        spec = self.spec
        out = spec.series_dir(system.name)
        (out / 'logs').mkdir(parents=True, exist_ok=True)
        runs_file = out / 'runs.jsonl'
        done = read_done(runs_file)

        def record(rec: dict) -> None:
            with runs_file.open('a') as f:
                f.write(json.dumps(rec) + '\n')
            self.on_event({'type': 'run', **{k: v for k, v in rec.items() if k != 'command'}})

        for graph in spec.graphs:
            for mode in self.modes[system.name]:
                failed = False
                for n in spec.sizes:
                    if (system.name, graph, mode, n) in done:
                        self.done += 1
                        counts['resumed'] += 1
                        self._progress(system.name, graph, n, mode, 'done', 'resumed')
                        continue
                    base = {
                        'system': system.name,
                        'graph': graph,
                        'mode': mode,
                        'n': n,
                        'label': spec.label,
                        'timeout_s': spec.timeout,
                    }
                    if failed:
                        record({**base, 'run': None, 'status': 'skipped'})
                        self.done += 1
                        counts['skipped'] += 1
                        self._progress(system.name, graph, n, mode, 'done', 'skipped')
                        continue
                    self._progress(system.name, graph, n, mode, 'running')
                    outcome = self._run_configuration(system, out, base, record)
                    if outcome is None:
                        return False
                    self.done += 1
                    counts['ok' if outcome == 'ok' else 'failed'] += 1
                    failed = outcome != 'ok'
                    self._progress(system.name, graph, n, mode, 'done', outcome)
        return True

    def _run_configuration(self, system: SystemDescriptor, out: Path, base: dict, record) -> Optional[str]:
        """All runs of one configuration; returns 'ok', the failure kind, or None if stopped."""
        spec = self.spec
        graph, mode, n = base['graph'], base['mode'], base['n']
        timing_csv = out / 'timing' / spec.domain / system.name / graph / f'{mode}_graph_{n}.csv'
        result_file = (timing_csv.parent / mode / str(n) / system.result_file) if system.result_file else None
        expected = None
        if spec.verifies and result_file and n <= spec.expected_max_n:
            expected = verify.cached_expected(edge_file(graph, n, BASE_DIR / 'input'), spec.expected_cache)
        cmd = [
            sys.executable,
            '-m',
            'engine.run_one',
            '--system',
            system.name,
            '--graph',
            graph,
            '--mode',
            mode,
            '--size',
            str(n),
            '--timing-dir',
            str(out / 'timing'),
            '--domain',
            spec.domain,
            '--query-mode',
            spec.query_mode,
            '--config-file',
            str(spec.config_file),
        ]
        if spec.souffle_include_dir:
            cmd += ['--souffle-include-dir', spec.souffle_include_dir]
        for i in range(1, spec.runs + 1):
            if result_file and result_file.exists():
                result_file.unlink()
            before = len(csv_rows(timing_csv))
            log_file = out / 'logs' / f'{system.name}_{graph}_{mode}_{n}_run{i}.log'
            rec = {**base, 'run': i, 'start': now(), 'command': shlex.join(cmd)}
            t0 = time.perf_counter()
            status, exit_code = self._run_process(cmd, log_file, system)
            if status == 'stopped':
                return None
            rec['status'] = status
            if exit_code is not None:
                rec['exit_code'] = exit_code
            rec['wall_s'] = round(time.perf_counter() - t0, 3)
            rec['end'] = now()
            text = log_file.read_text(errors='replace')
            result = parse_outcome(text)
            errors = (result or {}).get('errors') or log_errors(text)
            rows = csv_rows(timing_csv)
            rec['timing_row'] = rows[-1] if len(rows) > before else None
            if status == 'ok':
                self._check_result(rec, result_file, expected)
                if errors or rec['timing_row'] is None or exit_code != 0:
                    rec['status'] = 'error'
            rec['errors'] = errors
            rec['failure'] = classify_failure(rec['status'], rec.get('exit_code'), errors)
            rec['memory'] = (result or {}).get('memory')
            record(rec)
            self._report_run(rec)
            if rec['status'] != 'ok':
                return rec['failure'] or 'error'
        return 'ok'

    def _check_result(self, rec: dict, result_file: Optional[Path], expected: Optional[dict]) -> None:
        """Summarize the result file of a successful run into rec (and delete the file)."""
        if not self.spec.verifies:
            return
        if result_file and result_file.exists() and result_file.stat().st_size > 0:
            count, digest = verify.summarize_file(result_file)
            rec['result'] = {'count': count, 'hash': f'{digest:016x}'}
            rec['expected'] = expected
            rec['correct'] = (rec['result'] == expected) if expected else None
            if not self.spec.keep_results:
                result_file.unlink()
        else:
            rec['result'] = None
            rec['correct'] = False if result_file else None

    def _report_run(self, rec: dict) -> None:
        """One log line per run: its time, or why it failed."""
        what = f'{rec["system"]} · {rec["graph"]} · n={rec["n"]} · {rec["mode"]} · run {rec["run"]}'
        if rec['status'] == 'ok':
            row = rec.get('timing_row') or {}
            extra = (
                ' · result checked' if rec.get('correct') else ' · RESULT WRONG' if rec.get('correct') is False else ''
            )
            self.log(f'{what}: {rec["wall_s"]:.2f} s{extra}', 'warn' if rec.get('correct') is False else 'info')
            if not row:
                self.log(f'{what}: no timing row', 'warn')
        else:
            first = (rec.get('errors') or [''])[0]
            self.log(f'{what}: {rec["failure"]}{": " + first if first else ""}', 'error')

    def _run_process(self, cmd: list[str], log_file: Path, system: SystemDescriptor) -> tuple[str, Optional[int]]:
        """Start one trial; returns ('ok', exit code), ('timeout', None) or ('stopped', None)."""
        deadline = time.monotonic() + self.spec.timeout
        # start_new_session: the trial and everything it starts form one process group, killed together
        with (
            open(log_file, 'wb') as lf,
            subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT, cwd=BASE_DIR, start_new_session=True) as p,
        ):
            try:
                while True:
                    try:
                        p.wait(timeout=max(0.0, min(POLL_S, deadline - time.monotonic())))
                        return 'ok', p.returncode
                    except subprocess.TimeoutExpired:
                        pass
                    if self.should_stop():
                        self._kill(p, system)
                        return 'stopped', None
                    if time.monotonic() >= deadline:
                        self._kill(p, system)
                        time.sleep(5)  # let the server release what the cancelled query held
                        return 'timeout', None
            except BaseException:  # Ctrl-C in the CLI: never leave a trial running
                self._kill(p, system)
                raise

    def _kill(self, p: subprocess.Popen, system: SystemDescriptor) -> None:
        """Kill the trial's process group and cancel its query inside the server."""
        try:
            os.killpg(p.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        p.wait()
        try:
            get_connector(system.protocol).cancel_running(system.credentials, system)
        except Exception as e:  # pylint: disable=broad-except  # cleanup never stops the campaign
            self.log(f'[cancel {system.name}] {e}', 'warn')


def analyze(
    campaign_dir: Path, runs: int, compile_latex: bool = True, on_line: Optional[Callable[[str], None]] = None
) -> int:
    """
    Run analyze_verified.py on a campaign directory; returns its exit code.

    It writes <campaign>/analysis/: summary.csv, verification.json, the LaTeX table rows, and every
    figure both as matplotlib PDF and as a standalone pgfplots/TikZ document.
    """
    cmd = [
        sys.executable,
        str(BASE_DIR / 'analyze_verified.py'),
        str(campaign_dir),
        '--out',
        str(Path(campaign_dir) / 'analysis'),
        '--runs',
        str(runs),
    ]
    if not compile_latex:
        cmd.append('--no-compile')
    emit = on_line or print
    emit(f'# analysis: {shlex.join(cmd)}')
    with subprocess.Popen(cmd, cwd=BASE_DIR, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as p:
        for line in p.stdout or []:
            emit(line.rstrip())
    return p.returncode
