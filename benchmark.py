"""
benchmark.py — verified benchmark driver (time limit, isolation, per-run correctness check).

This is the driver used for the published measurements (results/verified_2026, see
docs/REPRODUCING.md). transitive.py remains the quick, in-process way to run experiments; use this
one when results must be trustworthy:

  * every run is a separate process (python -m engine.run_one), started in its own process group;
  * a run that exceeds --timeout seconds is killed as a whole process group, and the query still
    running inside the server is cancelled (connector.cancel_running); it is recorded as `timeout`;
  * a run that reports an error is recorded as `error`;
  * after a timeout or error, the remaining runs of that configuration are not started and all
    larger n of the same (system, graph, mode) are recorded as `skipped`;
  * every successful run's result file is checked against the transitive closure computed
    independently (engine/verify.py): row count and a 64-bit order-independent hash; the result
    file is then deleted (results of large graphs have several GB);
  * everything is appended to <out>/runs.jsonl (schema in docs/VERIFICATION.md), with the full
    output of each run in <out>/logs/ and the per-run timing rows in <out>/timing/.

The driver can be interrupted and restarted with the same arguments: configurations that already
have records in runs.jsonl are not run again.

When it finishes, the driver analyzes the whole campaign directory (the parent of --out, which
holds one directory per series) with analyze_verified.py: summary.csv, verification.json, LaTeX
table rows, and every figure both as matplotlib PDF (analysis/figures/) and as a standalone
pgfplots/TikZ document compiled to PDF (analysis/figures_tex/). Use --no-analysis to skip this,
e.g. when a script runs the driver many times and analyzes once at the end.

Example (one system; start its server first, see docs/SYSTEMS.md):

    python benchmark.py --systems duckdb --graphs cycle path --modes left_recursion right_recursion \\
        --sizes 100 200 300 --runs 5 --timeout 600 --out results/my_run/duckdb
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from engine import verify
from engine.failures import classify_failure
from engine.connectors import get_connector
from engine.loader import DescriptorLoader
from engine.run_one import RESULT_MARKER

BASE_DIR = Path(__file__).resolve().parent
GRAPHS = ['complete', 'max_acyclic', 'cycle', 'cycle_with_shortcuts', 'path', 'multi_path',
          'grid', 'binary_tree', 'reverse_binary_tree', 'x', 'y', 'w']


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline='') as f:
        return [r for r in csv.DictReader(f) if r]


def parse_outcome(log_text: str) -> dict | None:
    """The RUN_ONE_RESULT line printed by engine/run_one.py, or None (e.g. the process was killed)."""
    for line in reversed(log_text.splitlines()):
        if line.startswith(RESULT_MARKER):
            return json.loads(line[len(RESULT_MARKER):])
    return None


def log_errors(log_text: str) -> list[str]:
    """ERROR lines of the run's log (format of logging.basicConfig in engine/run_one.py)."""
    return [ln.split(' - ERROR: ', 1)[1][:500] for ln in log_text.splitlines() if ' - ERROR: ' in ln]


def cancel_server_side(system, credentials: dict) -> None:
    try:
        get_connector(system.protocol).cancel_running(credentials, system)
    except Exception as e:  # never let cleanup stop the campaign; it is visible in the driver output
        print(f'[cancel {system.name}] {e}', file=sys.stderr, flush=True)


def paths_for(out: Path, domain: str, system: str, graph: str, mode: str, n: int, result_file: str):
    base = out / 'timing' / domain / system / graph
    return base / f'{mode}_graph_{n}.csv', (base / mode / str(n) / result_file) if result_file else None


def run_campaign(a: argparse.Namespace) -> None:
    out = Path(a.out).resolve()
    (out / 'logs').mkdir(parents=True, exist_ok=True)
    runs_file = out / 'runs.jsonl'
    done = set()
    if runs_file.exists():
        for line in runs_file.read_text().splitlines():
            r = json.loads(line)
            done.add((r['system'], r['graph'], r['mode'], r['n']))

    loader = DescriptorLoader(base_dir=BASE_DIR, config_path=Path(a.config_file), detect_versions=False)

    def record(rec: dict) -> None:
        with runs_file.open('a') as f:
            f.write(json.dumps(rec) + '\n')
        print(json.dumps({k: v for k, v in rec.items() if k not in ('command', 'timing_row')}), flush=True)

    for system_name in a.systems:
        system = loader.get_system(system_name)
        if system is None:
            sys.exit(f'unknown system {system_name!r}')
        for graph in a.graphs:
            for mode in a.modes:
                if mode not in system.modes:
                    print(f'# {system_name} has no mode {mode}; skipped', flush=True)
                    continue
                stop = False
                for n in a.sizes:
                    key = (system_name, graph, mode, n)
                    if key in done:
                        continue
                    base = dict(system=system_name, graph=graph, mode=mode, n=n, label=a.label, timeout_s=a.timeout)
                    if stop:
                        record({**base, 'run': None, 'status': 'skipped'})
                        continue
                    edge_file = Path('input') / 'souffle' / graph / str(n) / 'edge.facts'
                    if not edge_file.exists():
                        sys.exit(f'{edge_file} not found: generate the inputs first (docs/REPRODUCING.md)')
                    timing_csv, result_file = paths_for(out, a.domain, system_name, graph, mode, n,
                                                        system.result_file)
                    exp = None
                    if result_file and n <= a.expected_max_n:
                        exp = verify.cached_expected(edge_file, Path(a.expected_cache))
                    cmd = [sys.executable, '-m', 'engine.run_one', '--system', system_name, '--graph', graph,
                           '--mode', mode, '--size', str(n), '--timing-dir', str(out / 'timing'),
                           '--domain', a.domain, '--config-file', a.config_file]
                    for i in range(1, a.runs + 1):
                        if result_file and result_file.exists():
                            result_file.unlink()
                        before = len(csv_rows(timing_csv))
                        log = out / 'logs' / f'{system_name}_{graph}_{mode}_{n}_run{i}.log'
                        t0, start = time.perf_counter(), now()
                        rec = {**base, 'run': i, 'start': start, 'command': ' '.join(cmd)}
                        with open(log, 'wb') as lf:
                            p = subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT, cwd=BASE_DIR,
                                                 start_new_session=True)
                            try:
                                p.wait(timeout=a.timeout)
                                rec['status'] = 'ok'
                                rec['exit_code'] = p.returncode
                            except subprocess.TimeoutExpired:
                                os.killpg(p.pid, signal.SIGKILL)
                                p.wait()
                                rec['status'] = 'timeout'
                                cancel_server_side(system, system.credentials)
                                time.sleep(5)
                        rec['wall_s'] = round(time.perf_counter() - t0, 3)
                        rec['end'] = now()
                        text = log.read_text(errors='replace')
                        outcome = parse_outcome(text)
                        errors = (outcome or {}).get('errors') or log_errors(text)
                        rows = csv_rows(timing_csv)
                        rec['timing_row'] = rows[-1] if len(rows) > before else None
                        if rec['status'] == 'ok':
                            if result_file and result_file.exists() and result_file.stat().st_size > 0:
                                c, h = verify.summarize_file(result_file)
                                rec['result'] = {'count': c, 'hash': f'{h:016x}'}
                                rec['expected'] = exp
                                rec['correct'] = (rec['result'] == exp) if exp else None
                                if not a.keep_results:
                                    result_file.unlink()
                            else:
                                rec['result'] = None
                                rec['correct'] = False if result_file else None
                            if errors or rec['timing_row'] is None or rec['exit_code'] != 0:
                                rec['status'] = 'error'
                        rec['errors'] = errors
                        rec['failure'] = classify_failure(rec['status'], rec.get('exit_code'), errors)
                        rec['memory'] = (outcome or {}).get('memory')
                        record(rec)
                        if rec['status'] != 'ok':
                            stop = True
                            break


def analyze_campaign(out: Path, runs: int, compile_latex: bool = True) -> int:
    """Run analyze_verified.py on the campaign directory that contains `out`; returns its exit code."""
    campaign = out.parent
    loader = DescriptorLoader(base_dir=BASE_DIR, detect_versions=False)
    names = {s.name for s in loader.load_systems()}
    if not any(out.name == n or out.name.startswith(n + '_') for n in names):
        print(f'# not analyzed: {out.name!r} is not a series name (<system>[_suffix]); run '
              f'analyze_verified.py yourself', flush=True)
        return 0
    cmd = [sys.executable, str(BASE_DIR / 'analyze_verified.py'), str(campaign), '--out', str(campaign / 'analysis')]
    cmd += ['--runs', str(runs)]
    if not compile_latex:
        cmd.append('--no-compile')
    print(f'# analysis: {" ".join(cmd)}', flush=True)
    return subprocess.run(cmd, cwd=BASE_DIR).returncode


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--systems', nargs='+', required=True)
    ap.add_argument('--graphs', nargs='+', default=GRAPHS)
    ap.add_argument('--modes', nargs='+', default=['left_recursion', 'right_recursion'])
    ap.add_argument('--sizes', nargs='+', type=int, default=list(range(100, 1001, 100)),
                    help='graph sizes, in increasing order (after a failure, larger sizes are skipped)')
    ap.add_argument('--runs', type=int, default=5, help='runs per configuration (default 5)')
    ap.add_argument('--timeout', type=float, default=600, help='limit per run in seconds, whole run (default 600)')
    ap.add_argument('--out', required=True, help='output directory: runs.jsonl, logs/, timing/')
    ap.add_argument('--label', default='', help='free-text label stored with every run (e.g. a server setting)')
    ap.add_argument('--config-file', default='config.yaml',
                    help='global config with credentials for systems without systems/<name>/credentials.yaml')
    ap.add_argument('--domain', default='transitive')
    ap.add_argument('--expected-max-n', type=int, default=20000,
                    help='compute the expected closure in Python only up to this n (larger scale-free/BA '
                         'graphs are checked by cross-system agreement in analyze_verified.py)')
    ap.add_argument('--expected-cache', default=str(verify.DEFAULT_CACHE),
                    help='cache of expected closures (default input/expected_closures.json)')
    ap.add_argument('--keep-results', action='store_true', help='do not delete result files after checking')
    ap.add_argument('--no-analysis', action='store_true',
                    help='do not analyze the campaign (tables, matplotlib and LaTeX figures) at the end')
    ap.add_argument('--no-latex-compile', action='store_true',
                    help='write the LaTeX figures but do not compile them to PDF')
    a = ap.parse_args(argv)
    if a.sizes != sorted(a.sizes):
        ap.error('--sizes must be increasing')
    # <out> is one series (<system>[_suffix]); analyze_verified.py reads its runs with that system's columns
    names = {d.name for d in DescriptorLoader(base_dir=BASE_DIR, detect_versions=False).load_systems()}
    series_of = [n for n in names if Path(a.out).name == n or Path(a.out).name.startswith(n + '_')]
    if series_of and set(a.systems) != {max(series_of, key=len)}:
        ap.error(f'--out {a.out} is the series of {max(series_of, key=len)!r}; run each system into its own '
                 f'directory (e.g. results/my_run/<system>)')
    # inputs, rules and the expected-closure cache are addressed relative to the repository
    a.out = str(Path(a.out).resolve())
    a.config_file = str(Path(a.config_file).resolve())
    a.expected_cache = str(Path(a.expected_cache).resolve())
    os.chdir(BASE_DIR)
    run_campaign(a)
    if not a.no_analysis and analyze_campaign(Path(a.out), a.runs, compile_latex=not a.no_latex_compile) != 0:
        sys.exit('analysis failed (see above); the runs themselves are recorded in runs.jsonl')


if __name__ == '__main__':
    main()
