"""
transitive.py: the main command-line entry point of trans-bench.

It does four jobs. It runs benchmark campaigns through engine/campaign.py (the same engine as
benchmark.py and the Web UI), with the graph sizes given as a range. It starts the Web UI (--ui). It
creates new systems, domains and graph types from templates (--bootstrap-*). Finally, it checks rule
files (--validate-*, --test-rule). No system is named in this file; everything comes from the
descriptor files.

Usage examples:
  # Run all discovered systems on all graph types, sizes 10 to 100
  python transitive.py

  # Run specific systems only, into a named campaign
  python transitive.py --systems postgres xsb --campaign results/my_run

  # Custom graph types and sizes
  python transitive.py --graphs cycle path --sizes 100 1001 100

  # Launch the Web UI instead
  python transitive.py --ui
"""

from __future__ import annotations

import argparse
import gc
import logging
import os
import sys
import time
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent


def build_parser() -> argparse.ArgumentParser:
    """Build the parser of the command-line options."""
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
        default='config.yaml',
        help='Path to the global config file (YAML, or legacy JSON). Default: config.yaml',
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
        default=['left_recursion', 'right_recursion', 'double_recursion'],
        metavar='MODE',
        help=(
            'Recursion modes to benchmark. Any string is accepted; a system runs only the modes '
            'listed in its descriptor.yaml. '
            'Standard modes: right_recursion, left_recursion, double_recursion. '
            'Domain-specific modes (e.g. dijkstra_style) are also valid. '
            'Default: left_recursion right_recursion double_recursion'
        ),
    )
    parser.add_argument(
        '--sizes',
        type=int,
        nargs=3,
        metavar=('START', 'STOP', 'STEP'),
        default=[10, 101, 10],
        help='Graph size range, as for range(). Default: 10 101 10',
    )
    parser.add_argument(
        '--campaign',
        default=None,
        help='Campaign directory for the results. Default: results/run-<date>-<time>',
    )
    parser.add_argument(
        '--timeout',
        type=float,
        default=600,
        help='Time limit per run in seconds; a run over it is killed and larger sizes are skipped. Default: 600',
    )
    parser.add_argument(
        '--no-analysis',
        action='store_true',
        help='Do not analyze the campaign (tables and figures) at the end.',
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
        default=5,
        help='Runs per configuration (system, graph, mode, size). Default: 5',
    )
    parser.add_argument(
        '--souffle-include-dir',
        default=None,
        help="Souffle's C++ include directory, if the config file does not give it.",
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
    parser.add_argument(
        '--ui-debug',
        action='store_true',
        help="Run the Web UI with Flask's debugger and reloader (local use only).",
    )

    # ── Bootstrap and extension commands ──────────────────────────────────
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
        '--validate-domain',
        metavar='DOMAIN',
        help=(
            'Validate that every discovered system has rule files for all modes '
            'declared in a domain descriptor (domains/<DOMAIN>/descriptor.yaml).'
        ),
    )
    parser.add_argument(
        '--test-rule',
        metavar='RULE_FILE',
        help=(
            'Run static syntax checks and an optional live dry-run on a single rule file. '
            'Use --system to specify which system to use for the live dry-run.'
        ),
    )
    parser.add_argument(
        '--system',
        metavar='SYSTEM',
        help='System name to use for live dry-run with --test-rule.',
    )

    return parser


def list_templates(manager) -> None:
    """Print the bootstrap templates, grouped by kind."""
    templates = manager.list_templates()
    print('\n=== Available Bootstrap Templates ===\n')
    for title, key in (
        ('System Descriptors', 'system_descriptors'),
        ('Domain Templates', 'domain_templates'),
        ('Rule Templates', 'rule_templates'),
    ):
        print(f'{title}:')
        for t in templates[key]:
            print(f'  {t}')
        print()


def run_bootstrap(args: argparse.Namespace) -> int | None:
    """Run the --list-templates or --bootstrap-* command in `args`; None if there is none."""
    from engine.bootstrap import BootstrapManager

    manager = BootstrapManager(BASE_DIR)
    if args.list_templates:
        list_templates(manager)
        return 0
    if args.bootstrap_graph and not args.bootstrap_graph_generator:
        log.error('--bootstrap-graph requires --bootstrap-graph-generator')
        return 1
    try:
        if args.bootstrap_system:
            manager.bootstrap_system(args.bootstrap_system, args.bootstrap_system_template)
        elif args.bootstrap_domain:
            manager.bootstrap_domain(args.bootstrap_domain, args.bootstrap_domain_template)
        elif args.bootstrap_graph:
            manager.bootstrap_graph(
                args.bootstrap_graph, args.bootstrap_graph_generator, args.bootstrap_graph_description
            )
        else:
            return None
    except Exception as e:
        log.error(f'Bootstrap failed: {e}')
        return 1
    return 0


def run_validation(args: argparse.Namespace) -> int | None:
    """Run the --validate-rules, --validate-domain or --test-rule command in `args`; None if there is none."""
    if not (args.validate_rules or args.validate_domain or args.test_rule):
        return None
    from engine.validation import RuleValidator

    try:
        validator = RuleValidator(BASE_DIR)
        if args.validate_rules:
            ok = validator.validate_system(args.validate_rules)
        elif args.validate_domain:
            ok = validator.validate_domain(args.validate_domain, system_names=args.systems or None)
        else:
            ok = validator.test_rule_file(Path(args.test_rule), system_name=args.system)
    except Exception as e:
        log.error(f'Validation failed: {e}')
        return 1
    return 0 if ok else 1


def main(argv: list[str] | None = None) -> None:
    """Parse the options and run what they ask for: a tool command, the Web UI, or a campaign."""
    args = build_parser().parse_args(argv)
    code = run_bootstrap(args)
    if code is None:
        code = run_validation(args)
    if code is not None:
        sys.exit(code)
    if args.ui:
        from ui.app import create_app

        print(f'\n  trans-bench Web UI: http://{args.ui_host}:{args.ui_port}\n')
        create_app().run(host=args.ui_host, port=args.ui_port, debug=args.ui_debug)
        return
    sys.exit(run_campaign(args))


def run_campaign(args: argparse.Namespace) -> int:
    """Run the options as a campaign through engine/campaign.py; returns the exit code."""
    from engine.campaign import Campaign, CampaignError, CampaignSpec, analyze
    from engine.loader import DescriptorLoader

    loader = DescriptorLoader(base_dir=BASE_DIR, config_path=Path(args.config_file), detect_versions=False)
    systems = args.systems or [s.name for s in loader.load_systems()]
    graphs = args.graphs or [g.name for g in loader.load_graph_types()]
    campaign = args.campaign or f'results/run-{time.strftime("%Y-%m-%d-%H%M")}'
    spec = CampaignSpec(
        systems=systems,
        graphs=graphs,
        modes=args.modes,
        sizes=list(range(*args.sizes)),
        runs=args.num_runs,
        timeout=args.timeout,
        campaign_dir=BASE_DIR / campaign,
        domain=args.domain,
        query_mode=args.query_mode,
        config_file=Path(args.config_file),
        souffle_include_dir=args.souffle_include_dir,
    )
    log.info(f'Campaign {spec.campaign_dir}; the same run with benchmark.py: {spec.command()}')

    def on_event(event: dict) -> None:
        if event['type'] == 'log':
            level = {'error': logging.ERROR, 'warn': logging.WARNING}.get(event['level'], logging.INFO)
            log.log(level, event['message'])

    os.chdir(BASE_DIR)
    try:
        counts = Campaign(spec, on_event=on_event).run()
    except CampaignError as e:
        log.error(str(e))
        return 1
    log.info(f'{counts["ok"]} configurations ok, {counts["failed"]} failed, {counts["skipped"]} skipped')
    if not args.no_analysis and not counts['stopped']:
        return analyze(spec.campaign_dir, spec.runs, on_line=log.info)
    return 0


if __name__ == '__main__':
    gc.disable()
    main()
