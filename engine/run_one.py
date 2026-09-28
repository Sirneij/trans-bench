"""
engine/run_one.py

Runs ONE trial of (system, graph, mode, n) in the current process and exits. This is the unit that
benchmark.py starts once per run, so that every run gets a fresh process (no memory, caches,
connections or garbage from earlier runs), and so that a run over the time limit can be killed as
a whole process group without affecting the next one.

    python -m engine.run_one --system duckdb --graph cycle --mode left_recursion --size 100 \\
        --timing-dir results/my_run/duckdb/timing

It appends one timing row to   <timing-dir>/<domain>/<system>/<graph>/<mode>_graph_<n>.csv
writes the query result to     <timing-dir>/<domain>/<system>/<graph>/<mode>/<n>/<result_file>
and prints, as its last line:  RUN_ONE_RESULT {"timing": {...}, "errors": [...], ...}

Exit code: 0 = no errors, 1 = the trial recorded errors, 2 = the trial could not be set up.
Credentials come from systems/<name>/credentials.yaml, else from the global config file.
"""

from __future__ import annotations

import argparse
import gc
import json
import logging
import sys
from pathlib import Path

from engine.loader import DescriptorLoader
from engine.runner import ExperimentRunner

RESULT_MARKER = 'RUN_ONE_RESULT '
BASE_DIR = Path(__file__).parent.parent

log = logging.getLogger('engine.run_one')


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--system', required=True)
    ap.add_argument('--graph', required=True)
    ap.add_argument('--mode', required=True)
    ap.add_argument('--size', type=int, required=True)
    ap.add_argument('--timing-dir', default='timing')
    ap.add_argument('--domain', default='transitive')
    ap.add_argument('--query-mode', default='full_materialization')
    ap.add_argument('--config-file', default='config.yaml', help='global config (YAML or legacy JSON)')
    return ap.parse_args(argv)


def run(args: argparse.Namespace) -> dict:
    """Run the trial and return its outcome dict (also used by the tests)."""
    loader = DescriptorLoader(base_dir=BASE_DIR, config_path=Path(args.config_file), detect_versions=False)
    system = loader.get_system(args.system)
    if system is None:
        return {'timing': None, 'errors': [f'unknown system {args.system!r} (no systems/{args.system}/descriptor.yaml)'],
                'setup_failed': True}
    graphs = loader.load_graph_types(names=[args.graph])
    if not graphs:
        return {'timing': None, 'errors': [f'unknown graph type {args.graph!r} (no graph_types/{args.graph}.yaml)'],
                'setup_failed': True}

    config = dict(loader.load_global_config())
    config['timing_dir'] = args.timing_dir
    config.setdefault('queries', '[[query1, path(X, Y)]]')

    runner = ExperimentRunner(
        config=config,
        systems=[system],
        graph_types=graphs,
        size_range=[args.size, args.size + 1, 1],
        num_runs=1,
        modes=[args.mode],
        domain=args.domain,
        query_mode=args.query_mode,
    )
    outcome = runner.run_trial(system, graphs[0], args.size, args.mode)
    if outcome.get('timing') is None and not outcome.get('timing_path'):
        outcome['setup_failed'] = True
    return outcome


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
    outcome = run(parse_args(argv))
    for e in outcome['errors']:
        # same line format as the connectors' own error log lines, so logs can be grepped uniformly
        if outcome.get('setup_failed'):
            log.error(e)
    print(RESULT_MARKER + json.dumps(outcome, default=str), flush=True)
    if outcome.get('setup_failed'):
        return 2
    return 1 if outcome['errors'] else 0


if __name__ == '__main__':
    gc.disable()  # as in the original harness: no GC pauses inside timed sections
    sys.exit(main())
