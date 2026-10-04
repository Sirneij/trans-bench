r"""
Run one trial in a process of its own and print its outcome.

This module runs ONE trial of (system, graph, mode, n) in the current process and exits. engine/campaign.py starts
this module once per run, so that every run gets a fresh process (no memory, caches, connections or
garbage from earlier runs), and so that a run over the time limit can be killed as a whole process
group without affecting the next one.

    python -m engine.run_one --system duckdb --graph cycle --mode left_recursion --size 100 \
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
from engine.runner import TrialRunner

RESULT_MARKER = 'RUN_ONE_RESULT '
BASE_DIR = Path(__file__).resolve().parent.parent

log = logging.getLogger('engine.run_one')


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Command-line options; see the module docstring."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--system', required=True)
    ap.add_argument('--graph', required=True)
    ap.add_argument('--mode', required=True)
    ap.add_argument('--size', type=int, required=True)
    ap.add_argument('--timing-dir', default='timing')
    ap.add_argument('--domain', default='transitive')
    ap.add_argument('--query-mode', default='full_materialization')
    ap.add_argument('--config-file', default='config.yaml', help='global config (YAML or legacy JSON)')
    ap.add_argument('--souffle-include-dir', default=None, help="Souffle's C++ include directory, if not in config")
    return ap.parse_args(argv)


def run(args: argparse.Namespace) -> dict:
    """Run the trial and return its outcome dict (also used by the tests)."""
    loader = DescriptorLoader(base_dir=BASE_DIR, config_path=Path(args.config_file), detect_versions=False)
    system = loader.get_system(args.system)
    if system is None:
        return {
            'timing': None,
            'errors': [f'unknown system {args.system!r} (no systems/{args.system}/descriptor.yaml)'],
            'setup_failed': True,
        }
    graphs = loader.load_graph_types(names=[args.graph])
    if not graphs:
        return {
            'timing': None,
            'errors': [f'unknown graph type {args.graph!r} (no graph_types/{args.graph}.yaml)'],
            'setup_failed': True,
        }

    config = dict(loader.load_global_config())
    config.setdefault('queries', '[[query1, path(X, Y)]]')
    if args.souffle_include_dir:
        config['souffle_include_dir'] = args.souffle_include_dir

    runner = TrialRunner(config, timing_dir=Path(args.timing_dir), domain=args.domain, query_mode=args.query_mode)
    outcome = runner.run_trial(system, graphs[0], args.size, args.mode)
    if outcome.get('timing') is None and not outcome.get('timing_path'):
        outcome['setup_failed'] = True
    return outcome


def main(argv: list[str] | None = None) -> int:
    """Run one trial, print its outcome line and return the exit code."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
    outcome = run(parse_args(argv))
    if outcome.get('setup_failed'):
        # same line format as the connectors' own error lines, so every log can be searched the same way
        for e in outcome['errors']:
            log.error(e)
    print(RESULT_MARKER + json.dumps(outcome, default=str), flush=True)
    if outcome.get('setup_failed'):
        return 2
    return 1 if outcome['errors'] else 0


if __name__ == '__main__':
    gc.disable()  # as in the original harness: no garbage-collection pauses inside timed sections
    sys.exit(main())
