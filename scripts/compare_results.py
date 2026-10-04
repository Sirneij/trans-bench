"""
Compare a new campaign with a reference campaign (by default the published one).

    python scripts/compare_results.py results/my_run [--reference results/verified_2026]

For every (series, graph, mode, n) present in both:
  * results: the result (count, hash) of every completed run must equal the reference result;
    this is independent of the machine and is the part of a reproduction that must match exactly;
  * status: ok / timeout / error / skipped, which may legitimately differ on a faster or slower
    machine near the time limit (listed, not counted as failures);
  * time: ratio of the median query times (new / reference), summarized per series.
Exit code 1 if any result differs.
"""

import argparse
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import analyze_verified  # noqa: E402  # pylint: disable=wrong-import-position


def load(results: Path) -> dict:
    """Group the run records of a campaign by (series, graph, mode, n)."""
    groups = defaultdict(list)
    for f in sorted(results.glob('*/runs.jsonl')):
        for line in f.read_text(encoding='utf-8').splitlines():
            r = json.loads(line)
            groups[(f.parent.name, r['graph'], r['mode'], r['n'])].append(r)
    return groups


def status(runs: list) -> str:
    """Return the status of a configuration from its runs: skipped, timeout, error or ok."""
    for s in ('skipped', 'timeout', 'error'):
        if any(r.get('status') == s for r in runs):
            return s
    return 'ok'


def main() -> int:
    """Compare the campaigns; return 1 if any result differs."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('results')
    ap.add_argument('--reference', default=str(Path(__file__).resolve().parent.parent / 'results' / 'verified_2026_v2'))
    a = ap.parse_args()
    new, ref = load(Path(a.results)), load(Path(a.reference))
    common = sorted(set(new) & set(ref))
    if not common:
        print('no configurations in common')
        return 1

    mismatches, status_diff, ratios = [], [], defaultdict(list)
    for key in common:
        ref_results = {
            json.dumps(r['result'], sort_keys=True) for r in ref[key] if r.get('status') == 'ok' and r.get('result')
        }
        for r in new[key]:
            if r.get('status') == 'ok' and r.get('result') and ref_results:
                if json.dumps(r['result'], sort_keys=True) not in ref_results:
                    mismatches.append((key, r['run'], r['result'], sorted(ref_results)))
        if status(new[key]) != status(ref[key]):
            status_diff.append((key, status(new[key]), status(ref[key])))
        qreal, _ = analyze_verified.query_columns(key[0])
        t_new = [float(r['timing_row'][qreal]) for r in new[key] if r.get('status') == 'ok' and r.get('timing_row')]
        t_ref = [float(r['timing_row'][qreal]) for r in ref[key] if r.get('status') == 'ok' and r.get('timing_row')]
        if t_new and t_ref and statistics.median(t_ref) > 0:
            ratios[key[0]].append(statistics.median(t_new) / statistics.median(t_ref))

    print(f'{len(common)} configurations in common')
    print(f'results: {len(mismatches)} runs differ from the reference')
    for key, run, got, want in mismatches[:50]:
        print(f'  DIFFERENT {"/".join(map(str, key))} run {run}: {got} vs {want}')
    print(f'status: {len(status_diff)} configurations differ (new vs reference)')
    for key, s_new, s_ref in status_diff[:50]:
        print(f'  {"/".join(map(str, key))}: {s_new} vs {s_ref}')
    print('median query time, new / reference (geometric mean over configurations):')
    for series, rs in sorted(ratios.items()):
        gm = statistics.geometric_mean([max(x, 1e-9) for x in rs])
        print(f'  {series:15s} {gm:6.2f}  ({len(rs)} configurations)')
    return 1 if mismatches else 0


if __name__ == '__main__':
    sys.exit(main())
