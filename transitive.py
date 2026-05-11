"""
transitive.py  (Phase 3 — plugin-driven CLI)

Thin CLI wrapper around the engine. All system/graph knowledge lives in
descriptor files — this file never needs editing to add a new system.

Usage examples:
  # Run all discovered systems on all graph types
  python transitive.py

  # Run specific systems only
  python transitive.py --systems postgres xsb

  # Custom graph types and sizes
  python transitive.py --graphs cycle path --sizes 100 1001 100

  # Launch the Web UI instead
  python transitive.py --ui
"""

from __future__ import annotations

import argparse
import gc
import json
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent


def load_legacy_config(path: Path) -> dict:
    """Load config.json for backward compatibility."""
    if not path.exists():
        return {}
    with open(path) as f:
        return json.load(f)


def merge_config(global_cfg: dict, args: argparse.Namespace) -> dict:
    """Merge global config with CLI overrides and inject souffle_include_dir."""
    cfg = dict(global_cfg)
    cfg.setdefault('timing_dir', 'timing')
    cfg.setdefault('queries', '[[query1, path(X, Y)]]')
    cfg['souffle_include_dir'] = args.souffle_include_dir
    return cfg


def main() -> None:
    parser = argparse.ArgumentParser(
        description='trans-bench: plugin-driven transitive closure benchmark suite',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python transitive.py --systems postgres xsb clingo --graphs cycle path
  python transitive.py --sizes 100 1001 100 --modes right_recursion left_recursion
  python transitive.py --ui           # launch the Web UI
        """,
    )

    # ── Core arguments ────────────────────────────────────────────────────
    parser.add_argument(
        '--config-file',
        default='config.json',
        help='Path to config file (JSON or YAML). Default: config.json',
    )
    parser.add_argument(
        '--systems',
        nargs='+',
        metavar='SYSTEM',
        help='Systems to benchmark. Omit to run ALL discovered systems.',
    )
    parser.add_argument(
        '--graphs',
        nargs='+',
        metavar='GRAPH',
        help='Graph types to test. Omit to run ALL discovered graph types.',
    )
    parser.add_argument(
        '--modes',
        nargs='+',
        default=['right_recursion', 'left_recursion', 'double_recursion'],
        choices=['right_recursion', 'left_recursion', 'double_recursion'],
        help='Recursion modes. Default: all three.',
    )
    parser.add_argument(
        '--sizes',
        type=int,
        nargs=3,
        metavar=('START', 'STOP', 'STEP'),
        default=[10, 101, 10],
        help='Graph size range. Default: 10 101 10',
    )
    parser.add_argument(
        '--domain',
        default='transitive',
        help='Benchmark domain to run. Default: transitive',
    )
    parser.add_argument(
        '--query-mode',
        default='full_materialization',
        choices=['full_materialization', 'demand_driven'],
        help='Query execution mode: full_materialization or demand_driven. Default: full_materialization',
    )
    parser.add_argument(
        '--num-runs',
        type=int,
        default=10,
        help='Repetitions per experiment cell. Default: 10',
    )
    parser.add_argument(
        '--souffle-include-dir',
        default='/opt/homebrew/Cellar/souffle/HEAD-8abf896/include',
        help="Soufflé C++ include directory. Default: /opt/homebrew/Cellar/souffle/HEAD-8abf896/include",
    )

    # ── Web UI ────────────────────────────────────────────────────────────
    parser.add_argument(
        '--ui',
        action='store_true',
        help='Launch the Web UI instead of running experiments from CLI.',
    )
    parser.add_argument(
        '--ui-host',
        default='127.0.0.1',
        help='Web UI host. Default: 127.0.0.1',
    )
    parser.add_argument(
        '--ui-port',
        type=int,
        default=5000,
        help='Web UI port. Default: 5000',
    )

    # ── Bootstrap & Extension commands ────────────────────────────────────
    parser.add_argument(
        '--bootstrap-system',
        metavar='NAME',
        help='Create a new system directory from a template. Specify system name.',
    )
    parser.add_argument(
        '--bootstrap-system-template',
        default='descriptor_sql_database.yaml',
        help='Template to use for --bootstrap-system. Default: descriptor_sql_database.yaml',
    )
    parser.add_argument(
        '--bootstrap-domain',
        metavar='NAME',
        help='Create a new domain descriptor from a template. Specify domain name.',
    )
    parser.add_argument(
        '--bootstrap-domain-template',
        default='domain_shortest_path.yaml',
        help='Template to use for --bootstrap-domain. Default: domain_shortest_path.yaml',
    )
    parser.add_argument(
        '--bootstrap-graph',
        metavar='NAME',
        help='Create a new graph type descriptor. Specify graph name.',
    )
    parser.add_argument(
        '--bootstrap-graph-generator',
        metavar='PATH',
        help='Python dotted path to graph generator (e.g., engine.data_generator.DataGenerator.generate_my_graph).',
    )
    parser.add_argument(
        '--bootstrap-graph-description',
        default='',
        help='Description for the new graph type.',
    )
    parser.add_argument(
        '--list-templates',
        action='store_true',
        help='List all available bootstrap templates and exit.',
    )
    parser.add_argument(
        '--validate-rules',
        metavar='SYSTEM',
        help='Validate all rule files for a given system.',
    )
    parser.add_argument(
        '--test-rule',
        metavar='RULE_FILE',
        help='Test a single rule file against sample input data.',
    )

    args = parser.parse_args()

    # ── Bootstrap & Extension commands ────────────────────────────────────
    from engine.bootstrap import BootstrapManager

    bootstrap_mgr = BootstrapManager(BASE_DIR)

    if args.list_templates:
        templates = bootstrap_mgr.list_templates()
        print('\n=== Available Bootstrap Templates ===\n')
        print('System Descriptors:')
        for t in templates['system_descriptors']:
            print(f'  {t}')
        print('\nDomain Templates:')
        for t in templates['domain_templates']:
            print(f'  {t}')
        print('\nRule Templates:')
        for t in templates['rule_templates']:
            print(f'  {t}')
        print()
        sys.exit(0)

    if args.bootstrap_system:
        try:
            bootstrap_mgr.bootstrap_system(args.bootstrap_system, args.bootstrap_system_template)
            sys.exit(0)
        except Exception as e:
            log.error(f'Bootstrap failed: {e}')
            sys.exit(1)

    if args.bootstrap_domain:
        try:
            bootstrap_mgr.bootstrap_domain(args.bootstrap_domain, args.bootstrap_domain_template)
            sys.exit(0)
        except Exception as e:
            log.error(f'Bootstrap failed: {e}')
            sys.exit(1)

    if args.bootstrap_graph:
        if not args.bootstrap_graph_generator:
            log.error('--bootstrap-graph requires --bootstrap-graph-generator')
            sys.exit(1)
        try:
            bootstrap_mgr.bootstrap_graph(
                args.bootstrap_graph, args.bootstrap_graph_generator, args.bootstrap_graph_description
            )
            sys.exit(0)
        except Exception as e:
            log.error(f'Bootstrap failed: {e}')
            sys.exit(1)

    if args.validate_rules:
        from engine.validation import RuleValidator

        try:
            validator = RuleValidator(BASE_DIR)
            is_valid = validator.validate_system(args.validate_rules)
            sys.exit(0 if is_valid else 1)
        except Exception as e:
            log.error(f'Validation failed: {e}')
            sys.exit(1)

    if args.test_rule:
        log.info(f'Rule testing not yet implemented: {args.test_rule}')
        sys.exit(0)

    # ── Launch Web UI mode ────────────────────────────────────────────────

    if args.ui:
        from ui.app import create_app

        app = create_app()
        print(f'\n  trans-bench Web UI  →  http://{args.ui_host}:{args.ui_port}\n')
        app.run(host=args.ui_host, port=args.ui_port, debug=True)
        return

    # ── CLI experiment mode ───────────────────────────────────────────────
    from engine.loader import DescriptorLoader
    from engine.runner import ExperimentRunner

    # Load config (YAML preferred, JSON fallback)
    config_path = Path(args.config_file)
    loader = DescriptorLoader(base_dir=BASE_DIR, config_path=config_path)
    global_cfg = loader.load_global_config()
    config = merge_config(global_cfg, args)

    # Discover systems
    systems = loader.load_systems(names=args.systems)
    if not systems:
        log.error('No systems found. Check systems/*/descriptor.yaml or --systems flag.')
        sys.exit(1)
    log.info(f'Systems: {[s.name for s in systems]}')

    # Discover graph types
    graph_types = loader.load_graph_types(names=args.graphs)
    if not graph_types:
        log.error('No graph types found. Check graph_types/*.yaml or --graphs flag.')
        sys.exit(1)
    log.info(f'Graph types: {[g.name for g in graph_types]}')

    runner = ExperimentRunner(
        config=config,
        systems=systems,
        graph_types=graph_types,
        size_range=args.sizes,
        num_runs=args.num_runs,
        modes=args.modes,
        domain=args.domain,
        query_mode=args.query_mode,
    )

    log.info(
        f'Starting experiment | domain={args.domain} | query_mode={args.query_mode} | systems={[s.name for s in systems]} | '
        f'graphs={[g.name for g in graph_types]} | sizes={args.sizes} | '
        f'modes={args.modes} | runs={args.num_runs}'
    )
    runner.run()


if __name__ == '__main__':
    gc.disable()
    main()
