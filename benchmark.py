r"""
Run a benchmark campaign from the command line.

This is a thin command-line front end to engine/campaign.py, the engine that transitive.py and the
Web UI also use. Each run is a separate process with a time limit; failures are classified; larger
graphs are skipped after a failure; and in the transitive-closure domain every result is checked
against an independently computed closure. The engine's docstring gives the full rules, and
docs/VERIFICATION.md the format of the records.

A campaign is a directory under results/ with one directory per series (a system, optionally with a
suffix for a variant setting). Either name the campaign (all systems go into it), or name one series
directory with --out, as scripts/run_all.sh does:

    python benchmark.py --systems duckdb postgres --graphs cycle path --sizes 100 200 300 \
        --campaign results/my_run
    python benchmark.py --systems mariadb --sizes 100 200 --out results/my_run/mariadb_tuned \
        --label tmp_table_size=4G

The command can be stopped and started again with the same arguments: configurations that already
have records are not run again. At the end, the campaign is analyzed with analyze_verified.py
(tables, matplotlib figures and LaTeX figures); --no-analysis skips this.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from engine import verify
from engine.campaign import BASE_DIR, GRAPHS, MODES, Campaign, CampaignError, CampaignSpec, analyze
from engine.loader import DescriptorLoader


def build_parser() -> argparse.ArgumentParser:
    """Build the parser of the command-line options."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--systems', nargs='+', required=True)
    ap.add_argument('--graphs', nargs='+', default=GRAPHS)
    ap.add_argument('--modes', nargs='+', default=MODES)
    ap.add_argument(
        '--sizes',
        nargs='+',
        type=int,
        default=list(range(100, 1001, 100)),
        help='graph sizes, in increasing order (after a failure, larger sizes are skipped)',
    )
    ap.add_argument('--runs', type=int, default=5, help='runs per configuration (default 5)')
    ap.add_argument('--timeout', type=float, default=600, help='limit per run in seconds, whole run (default 600)')
    where = ap.add_mutually_exclusive_group(required=True)
    where.add_argument('--campaign', help='campaign directory; each system gets its own series in it')
    where.add_argument('--out', help='one series directory, <campaign>/<system>[_suffix] (one system only)')
    ap.add_argument('--label', default='', help='free-text label stored with every run (e.g. a server setting)')
    ap.add_argument(
        '--config-file',
        default='config.yaml',
        help='global config with credentials for systems without systems/<name>/credentials.yaml',
    )
    ap.add_argument('--domain', default='transitive')
    ap.add_argument('--query-mode', default='full_materialization', choices=['full_materialization', 'demand_driven'])
    ap.add_argument('--souffle-include-dir', default=None, help="Souffle's C++ include directory, if not in config")
    ap.add_argument(
        '--expected-max-n',
        type=int,
        default=20000,
        help='compute the expected closure in Python only up to this n (larger scale-free/BA '
        'graphs are checked by cross-system agreement in analyze_verified.py)',
    )
    ap.add_argument(
        '--expected-cache',
        default=str(verify.DEFAULT_CACHE),
        help='cache of expected closures (default input/expected_closures.json)',
    )
    ap.add_argument('--keep-results', action='store_true', help='do not delete result files after checking')
    ap.add_argument(
        '--no-analysis',
        action='store_true',
        help='do not analyze the campaign (tables, matplotlib and LaTeX figures) at the end',
    )
    ap.add_argument(
        '--no-latex-compile', action='store_true', help='write the LaTeX figures but do not compile them to PDF'
    )
    ap.add_argument('--verbose', action='store_true', help='also print the per-run log lines')
    return ap


def series_of(out: Path) -> str | None:
    """Return the system whose series directory `out` is (mariadb_tuned -> mariadb), or None."""
    names = {d.name for d in DescriptorLoader(base_dir=BASE_DIR, detect_versions=False).load_systems()}
    matches = [n for n in names if out.name == n or out.name.startswith(n + '_')]
    return max(matches, key=len) if matches else None


def spec_from_args(a: argparse.Namespace, ap: argparse.ArgumentParser) -> CampaignSpec:
    """Turn the parsed options into a CampaignSpec (exits with a usage error for a bad combination)."""
    if a.out:
        out = Path(a.out)
        system = series_of(out)
        if system is None or a.systems != [system]:
            ap.error(
                f'--out {a.out} must be the series directory of the one system given with --systems, '
                f'named <system>[_suffix] (e.g. results/my_run/{a.systems[0]}); use --campaign for several systems'
            )
        campaign_dir, suffix = out.parent, out.name[len(system) :]
    else:
        campaign_dir, suffix = Path(a.campaign), ''
    return CampaignSpec(
        systems=a.systems,
        graphs=a.graphs,
        modes=a.modes,
        sizes=a.sizes,
        runs=a.runs,
        timeout=a.timeout,
        campaign_dir=campaign_dir,
        series_suffix=suffix,
        label=a.label,
        domain=a.domain,
        query_mode=a.query_mode,
        config_file=Path(a.config_file),
        expected_max_n=a.expected_max_n,
        expected_cache=Path(a.expected_cache),
        keep_results=a.keep_results,
        souffle_include_dir=a.souffle_include_dir,
    )


def printer(verbose: bool):
    """Event handler for the terminal: one JSON line per run record, and the engine's messages."""

    def on_event(event: dict) -> None:
        kind = event.get('type')
        if kind == 'run':
            print(json.dumps({k: v for k, v in event.items() if k not in ('type', 'timing_row')}), flush=True)
        elif kind == 'log' and (verbose or not _is_run_line(event['message'])):
            stream = sys.stderr if event.get('level') == 'error' else sys.stdout
            print(f'# {event["message"]}', file=stream, flush=True)

    return on_event


def _is_run_line(message: str) -> bool:
    """Lines of the form '<system> · <graph> · n=... · run i: ...'; the JSON record already says this."""
    return ' · run ' in message


def main(argv: list[str] | None = None) -> None:
    """Parse the options, run the campaign, then analyze it."""
    ap = build_parser()
    a = ap.parse_args(argv)
    spec = spec_from_args(a, ap)
    os.chdir(BASE_DIR)  # inputs, rules and the expected-closure cache are addressed relative to the repository
    try:
        counts = Campaign(spec, on_event=printer(a.verbose)).run()
    except CampaignError as e:
        ap.error(str(e))
    print(
        f'# done: {counts["ok"]} configurations ok, {counts["failed"]} failed, {counts["skipped"]} skipped, '
        f'{counts["resumed"]} already recorded',
        flush=True,
    )
    if not a.no_analysis and analyze(spec.campaign_dir, a.runs, compile_latex=not a.no_latex_compile) != 0:
        sys.exit('analysis failed (see above); the runs themselves are recorded in runs.jsonl')


if __name__ == '__main__':
    main()
