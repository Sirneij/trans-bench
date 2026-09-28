"""
analyze_verified.py — analysis of a verified campaign (runs recorded by benchmark.py).

    python analyze_verified.py results/verified_2026 --out results/verified_2026/analysis

Reads every <results>/<series>/runs.jsonl, where <series> is a system name optionally followed by
a suffix for a variant setting (e.g. mariadb_tuned = MariaDB with 4 GB in-memory temporary
tables), and writes summary.csv, verification.json, figures/*.pdf and the LaTeX table rows
(table_*.tex) that are inlined in the paper.

Reported time = the system's query phase (descriptor.yaml `query_phase`) of one run:
ExecuteQueryRealTime for the SQL systems and MongoDB (the statement that computes and stores the
closure), QueryRealTime for XSB (query without writing) and for Neo4j (the count of the distinct
pairs, fetched). CPU time uses the corresponding *CPUTime column and is shown only for XSB and
DuckDB, whose query runs inside the measured process. A configuration's value is the mean over its
runs (5 in the campaign, --runs), reported only if all of them completed; medians, standard deviations, minima and maxima are
in summary.csv. Every figure is written twice: figures/<name>.pdf (matplotlib) and
figures_tex/<name>.tex (the same figure as a standalone pgfplots/TikZ document, compiled to
figures_tex/<name>.pdf if a LaTeX engine is installed; see engine/figures_tex.py). Figures and
tables are laid out for the paper (graph families, system order and
LaTeX macros such as \\gname are those of the paper).
"""
import argparse
import csv
import json
import statistics
import sys
import tarfile
from collections import defaultdict
from functools import lru_cache
from pathlib import Path

import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker  # noqa: E402

from engine.figures_tex import compile_tex, figure_to_tex, find_engine  # noqa: E402
from engine.plot_style import legend_order, style  # noqa: E402

GRAPHS = ['complete', 'max_acyclic', 'cycle', 'cycle_with_shortcuts', 'path', 'multi_path',
          'grid', 'binary_tree', 'reverse_binary_tree', 'x', 'y', 'w']
GNAME = {'complete': r'\gname{Cmpl}{n}', 'max_acyclic': r'\gname{MaxAcyc}{n}', 'cycle': r'\gname{Cyc}{n}',
         'cycle_with_shortcuts': r'\gname{CycExtra}{n,k}', 'path': r'\gname{Path}{n}',
         'multi_path': r'\gname{PathDisj}{n,k}', 'grid': r'\gname{Grid}{n}', 'binary_tree': r'\gname{BinTree}{n}',
         'reverse_binary_tree': r'\gname{BinTreeRev}{n}', 'x': r'\gname{X}{n,k}', 'y': r'\gname{Y}{n,k}',
         'w': r'\gname{W}{n,k}', 'scale_free': 'scale-free', 'barabasi_albert': 'Barab\\\'asi-Albert'}
TITLE = {'complete': 'Cmpl', 'max_acyclic': 'MaxAcyc', 'cycle': 'Cyc', 'cycle_with_shortcuts': 'CycExtra',
         'path': 'Path', 'multi_path': 'PathDisj', 'grid': 'Grid', 'binary_tree': 'BinTree',
         'reverse_binary_tree': 'BinTreeRev', 'x': 'X', 'y': 'Y', 'w': 'W', 'scale_free': 'Scale-free',
         'barabasi_albert': 'Barabási-Albert'}
RESULTS = Path('.')
# time limit of a run in seconds, for records written before benchmark.py stored it (the 2026 campaign)
DEFAULT_LIMIT_S = 600
RUNS = 5  # runs per configuration; a mean is reported only if all of them completed (--runs)
BASE_DIR = Path(__file__).resolve().parent


@lru_cache(maxsize=None)
def descriptors() -> dict:
    from engine.loader import DescriptorLoader

    return {d.name: d for d in DescriptorLoader(base_dir=BASE_DIR, detect_versions=False).load_systems()}


def base_system(series: str) -> str:
    """mariadb_tuned -> mariadb: the longest system name that the series name starts with."""
    names = [n for n in descriptors() if series == n or series.startswith(n + '_')]
    if not names:
        raise ValueError(f'series {series!r} does not start with a system name ({sorted(descriptors())})')
    return max(names, key=len)


def query_columns(series: str) -> tuple[str, str]:
    """(real, cpu) timing columns of the system's query phase, from its descriptor.yaml."""
    return descriptors()[base_system(series)].query_columns


def read_log(series: str, name: str) -> str | None:
    """A run's log from <results>/<series>/logs/, or from logs.tar.gz as shipped in the repository."""
    path = RESULTS / series / 'logs' / name
    if path.exists():
        return path.read_text(errors='replace')
    archive = RESULTS / series / 'logs.tar.gz'
    if archive.exists():
        with tarfile.open(archive) as tf:
            try:
                return tf.extractfile(f'logs/{name}').read().decode(errors='replace')
            except KeyError:
                return None
    return None


def load(results: Path):
    runs = []
    for f in sorted(results.glob('*/runs.jsonl')):
        sysdir = f.parent.name
        for line in f.read_text().splitlines():
            r = json.loads(line)
            r['series'] = sysdir  # e.g. mariadb_tuned
            runs.append(r)
    return runs


def summarize(runs):
    groups = defaultdict(list)
    for r in runs:
        groups[(r['series'], r['graph'], r['mode'], r['n'])].append(r)
    rows = {}
    for key, rs in groups.items():
        series = key[0]
        qreal, qcpu = query_columns(series)
        ok = [r for r in rs if r.get('status') == 'ok']
        status = 'ok'
        if any(r.get('status') == 'skipped' for r in rs):
            status = 'skipped'
        elif any(r.get('status') == 'timeout' for r in rs):
            status = 'timeout'
        elif any(r.get('status') == 'error' for r in rs):
            status = 'error'
        t = [float(r['timing_row'][qreal]) for r in ok if r.get('timing_row')]
        c = [float(r['timing_row'][qcpu]) for r in ok if r.get('timing_row')]
        correct = [r.get('correct') for r in ok]
        err = next((r['errors'][0] for r in rs if r.get('errors')), '')
        bad = [r for r in rs if r.get('status') == 'error']
        if bad:  # full error text (may span several lines) from the run's log file
            text = read_log(series, f"{bad[0]['system']}_{key[1]}_{key[2]}_{key[3]}_run{bad[0]['run']}.log")
            if text is not None:
                i = text.find(' - ERROR: ')
                if i >= 0:
                    err = ' '.join(text[i + 10:].split())
        limit = max((r.get('timeout_s') or DEFAULT_LIMIT_S) for r in rs)
        rows[key] = dict(series=series, graph=key[1], mode=key[2], n=key[3], status=status, runs=len(ok), limit=limit,
                         mean=statistics.mean(t) if len(t) == RUNS and status == 'ok' else None,
                         median=statistics.median(t) if t else None,
                         sd=statistics.stdev(t) if len(t) > 1 else None, min=min(t) if t else None,
                         max=max(t) if t else None,
                         cpu_mean=statistics.mean(c) if len(c) == RUNS and status == 'ok' else None,
                         all_correct=(None if all(x is None for x in correct) else all(x is True for x in correct)) if correct else None,
                         any_incorrect=any(x is False for x in correct),
                         unverified=sum(1 for x in correct if x is None),
                         result=(ok[0].get('result') if ok else None), error=err[:600])
    return rows


def apply_agreement(rows):
    """Large scale-free/BA instances have no Python ground truth; a result there counts as correct if
    it equals the result on which all other systems (at least two) agree, and as incorrect otherwise."""
    groups = defaultdict(dict)
    for k, r in rows.items():
        if r['graph'] in ('scale_free', 'barabasi_albert') and r['result'] and r['unverified']:
            groups[(r['graph'], r['n'])][k] = (r['result']['count'], r['result']['hash'])
    for (g, n), d in groups.items():
        for k, v in d.items():
            others = {x for kk, x in d.items() if kk[0] != k[0]}
            if len(others) == 1 and len({kk[0] for kk in d if kk[0] != k[0]}) >= 2:
                rows[k]['all_correct'] = v in others
                rows[k]['any_incorrect'] = v not in others
                rows[k]['checked_by'] = 'agreement'


def warn_near_limit(rows, fraction: float = 0.5) -> list:
    """Reported times above `fraction` of the time limit. A time limit should be clearly larger than
    every reported time, otherwise "TO" and the slowest reported times are hard to tell apart; the
    remedy is a campaign with a larger benchmark.py --timeout."""
    near = sorted(((r['mean'], r['limit'], k) for k, r in rows.items()
                   if r['mean'] is not None and r['mean'] > fraction * r['limit']), reverse=True)
    if near:
        print(f'WARNING: {len(near)} reported times exceed {fraction:.0%} of the time limit, e.g. '
              + '; '.join(f'{"/".join(map(str, k))}: {m:.0f} s of {lim:.0f} s' for m, lim, k in near[:3]))
    return near


def write_summary(rows, out: Path):
    keys = ['series', 'graph', 'mode', 'n', 'status', 'runs', 'mean', 'median', 'sd', 'min', 'max', 'cpu_mean',
            'all_correct', 'any_incorrect', 'unverified', 'checked_by', 'error']
    with open(out / 'summary.csv', 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=keys, extrasaction='ignore')
        w.writeheader()
        for k in sorted(rows, key=lambda k: (k[0], k[1], k[2], k[3])):
            w.writerow(rows[k])


def verification(runs, rows, out: Path):
    v = defaultdict(int)
    incorrect = defaultdict(set)
    for r in runs:
        v[f"status_{r.get('status')}"] += 1
        if r.get('status') == 'ok':
            v['runs_ok_checked' if r.get('correct') is not None else 'runs_ok_unchecked'] += 1
            if r.get('correct') is False:
                incorrect[f"{r['series']}/{r['mode']}"].add(r['graph'])
    # large scale-free/BA instances have no Python ground truth: check that systems agree
    consensus = {}
    by = defaultdict(dict)
    for k, row in rows.items():
        if row['graph'] in ('scale_free', 'barabasi_albert') and row['result'] and row['series'] != 'mariadb_tuned':
            by[(row['graph'], row['n'])][f"{row['series']}/{row['mode']}"] = (row['result']['count'], row['result']['hash'])
    for (g, n), d in sorted(by.items()):
        consensus[f'{g}/{n}'] = {'systems': len(d), 'agree': len(set(d.values())) == 1,
                                 'count': sorted(set(x[0] for x in d.values()))}
    res = {'counts': dict(v), 'incorrect_results': {k: sorted(s) for k, s in incorrect.items()},
           'scale_free_ba_cross_system_agreement': consensus}
    (out / 'verification.json').write_text(json.dumps(res, indent=1))
    return res


def val(rows, s, g, m, n):
    r = rows.get((s, g, m, n))
    return r['mean'] if r and r['status'] == 'ok' and r['all_correct'] is not False else None


def plot_graph(rows, g, sizes, out: Path, cpu=False, formats=('pdf', 'tex')):
    """One figure: (a) left and (b) right recursion side by side. Every system is drawn in its own
    style from engine/plot_style.py (same marker in every figure), and each panel has its own
    legend, listing the systems in the order of their last data points. Written as
    out/figures/<name>.pdf (matplotlib) and/or out/figures_tex/<name>.tex (the same figure
    transcribed to pgfplots by engine/figures_tex.py); returns the .tex path, if written."""
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.9), sharey=True)
    drawn_any = False
    for i, (ax, m) in enumerate(zip(axes, ['left_recursion', 'right_recursion'])):
        series = ['xsb', 'duckdb'] if cpu else ['xsb', 'postgres', 'mariadb', 'duckdb', 'cockroachdb', 'neo4j', 'mongodb']
        handles, last = {}, {}
        for s in series:
            mm = 'left_recursion' if s in ('neo4j', 'mongodb') else m
            xs, ys, to_x, limit = [], [], None, DEFAULT_LIMIT_S
            for n in sizes:
                r = rows.get((s, g, mm, n))
                if r is None:
                    continue
                if r['status'] == 'ok' and r['all_correct'] is not False:
                    y = r['cpu_mean'] if cpu else r['mean']
                    if y is not None:
                        xs.append(n)
                        ys.append(max(y, 1e-5))
                elif r['status'] in ('timeout', 'error') and to_x is None:
                    to_x, limit = n, r['limit']
            if not xs and to_x is None:
                continue
            label, col, mk, ls = style(s)
            handles[s] = ax.plot(xs, ys, color=col, marker=mk, linestyle=ls, markersize=3.5, linewidth=1.1,
                                 label=label)[0]
            if xs:
                last[s] = (xs[-1], ys[-1])
            if to_x is not None:  # first size that exceeded the time limit or failed: hollow marker at the limit
                ax.plot([to_x], [limit], color=col, marker=mk, markersize=6, markerfacecolor='none', linestyle='')
                last[s] = (to_x, limit)
        if handles:
            drawn_any = True
            order = legend_order(last)
            ax.legend([handles[s] for s in order], [handles[s].get_label() for s in order], loc='center left',
                      bbox_to_anchor=(1.0, 0.5), fontsize=6.5, frameon=False, handlelength=2.0, borderaxespad=0.4)
        ax.set_yscale('log')
        ax.set_title(f"({'ab'[i]}) {TITLE[g]}: {'left' if m.startswith('left') else 'right'} recursion", fontsize=9)
        ax.set_xlabel('n' if g not in ('scale_free', 'barabasi_albert') else 'number of nodes', fontsize=8)
        ax.tick_params(labelsize=7)
        if g in ('scale_free', 'barabasi_albert'):
            ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f'{int(v / 1000)}k'))
        ax.grid(True, which='major', linewidth=0.3)
    axes[0].set_ylabel(('CPU' if cpu else 'Elapsed') + ' time (s, log scale)', fontsize=8)
    if not drawn_any:
        plt.close(fig)
        return None
    fig.tight_layout(w_pad=0.6)
    name = f"{g}_{'cpu' if cpu else 'elapsed'}"
    tex = None
    if 'pdf' in formats:
        (out / 'figures').mkdir(parents=True, exist_ok=True)
        fig.savefig(out / 'figures' / f'{name}.pdf')
    if 'tex' in formats:
        (out / 'figures_tex').mkdir(parents=True, exist_ok=True)
        tex = out / 'figures_tex' / f'{name}.tex'
        tex.write_text(figure_to_tex(fig))
    plt.close(fig)
    return tex


def fmt(v):
    if v is None:
        return '--'
    if v >= 100:
        return f'{v:.1f}'
    if v >= 10:
        return f'{v:.2f}'
    if v >= 1:
        return f'{v:.3f}'
    if v >= 0.001:
        return f'{v:.4f}'
    return f'{v:.5f}'


def cell(rows, s, g, m, n):
    r = rows.get((s, g, m, n))
    if r is None:
        return '--'
    if r['status'] == 'timeout':
        return 'TO'
    if r['status'] == 'skipped':
        return 'TO$^{s}$'
    if r['status'] == 'error':
        e = r['error'].lower()
        if 'out of memory' in e or 'outofmemory' in e or 'memory' in e:
            return 'OOM'
        if 'more than once' in e or 'restrictions imposed' in e:
            return 'n/s'
        if 'max number of iterations' in e:
            return 'IL'
        return 'ERR'
    if r['all_correct'] is False:
        return fmt(r['mean']) + '$^{\\dagger}$'
    return fmt(r['mean'])


def table_linear(rows, n, out: Path):
    systems = ['xsb', 'postgres', 'mariadb', 'duckdb', 'cockroachdb']
    lines = []
    for g in GRAPHS:
        cells = []
        for m in ['left_recursion', 'right_recursion']:
            vals = {s: val(rows, s, g, m, n) for s in systems}
            best = min((v for v in vals.values() if v is not None), default=None)
            for s in systems:
                c = cell(rows, s, g, m, n)
                cells.append(r'\textbf{' + c + '}' if vals[s] is not None and vals[s] == best else c)
        single = [cell(rows, s, g, 'left_recursion', n) for s in ('neo4j', 'mongodb')]
        lines.append(GNAME[g] + ' & ' + ' & '.join(cells + single) + r' \\')
    (out / f'table_linear_n{n}.tex').write_text('\n'.join(lines) + '\n')


def table_double(rows, n, out: Path):
    lines = []
    for g in GRAPHS:
        cells = [cell(rows, s, g, 'double_recursion', n) for s in ('xsb', 'mariadb', 'duckdb')]
        cells.append(cell(rows, 'duckdb', g, 'doublerecurring_recursion', n))
        cells.append(cell(rows, 'xsb', g, 'left_recursion', n))
        lines.append(GNAME[g] + ' & ' + ' & '.join(cells) + r' \\')
    (out / f'table_double_n{n}.tex').write_text('\n'.join(lines) + '\n')


def table_large(rows, g, sizes, mode, out: Path):
    systems = ['cockroachdb', 'neo4j', 'mariadb', 'postgres', 'duckdb', 'xsb', 'mongodb', 'singlestore']
    lines = []
    for n in sizes:
        cs = [cell(rows, s, g, 'left_recursion' if s in ('neo4j', 'mongodb') else mode, n) for s in systems]
        lines.append(f'{n:,} & ' + ' & '.join(cs) + r' \\')
    (out / f'table_{g}_{mode}.tex').write_text('\n'.join(lines) + '\n')


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('results')
    ap.add_argument('--out', default=None, help='output directory (default: <results>/analysis)')
    ap.add_argument('--figures', nargs='*', choices=['pdf', 'tex'], default=['pdf', 'tex'],
                    help='figure formats: pdf = matplotlib (figures/), tex = pgfplots/TikZ (figures_tex/); '
                         'default both; none: --figures with no value')
    ap.add_argument('--runs', type=int, default=5,
                    help='runs per configuration of the campaign (default 5); a configuration is reported only '
                         'if all of them completed')
    ap.add_argument('--no-compile', action='store_true',
                    help='write the LaTeX figures without compiling them to PDF')
    ap.add_argument('--latex-engine', choices=['tectonic', 'lualatex', 'xelatex', 'pdflatex'],
                    help='default: the first one found in this order')
    a = ap.parse_args(argv)
    global RESULTS, RUNS
    RUNS = a.runs
    results = Path(a.results)
    RESULTS = results
    out = Path(a.out) if a.out else results / 'analysis'
    out.mkdir(parents=True, exist_ok=True)
    runs = load(results)
    rows = summarize(runs)
    apply_agreement(rows)
    write_summary(rows, out)
    v = verification(runs, rows, out)
    print(json.dumps(v['counts']), json.dumps(v['incorrect_results']))
    warn_near_limit(rows)
    texs = []
    for g in GRAPHS:
        texs.append(plot_graph(rows, g, list(range(100, 1001, 100)), out, formats=a.figures))
        texs.append(plot_graph(rows, g, list(range(100, 1001, 100)), out, cpu=True, formats=a.figures))
    for g, sizes in (('scale_free', range(10000, 90001, 10000)), ('barabasi_albert', range(10000, 100001, 10000))):
        texs.append(plot_graph(rows, g, list(sizes), out, formats=a.figures))
        texs.append(plot_graph(rows, g, list(sizes), out, cpu=True, formats=a.figures))
        for m in ('left_recursion', 'right_recursion'):
            table_large(rows, g, list(sizes), m, out)
    for n in (500, 1000):
        table_linear(rows, n, out)
        table_double(rows, n, out)
    texs = [t for t in texs if t]
    if texs and not a.no_compile:
        engine = a.latex_engine or find_engine()
        if engine is None:
            print('no LaTeX engine found (tectonic, lualatex, xelatex, pdflatex): '
                  f'{len(texs)} LaTeX figures written to {out / "figures_tex"} but not compiled')
        else:
            failed = {t: e for t, e in compile_tex(texs, engine).items() if e}
            print(f'LaTeX figures: {len(texs) - len(failed)} of {len(texs)} compiled with {engine} in {out / "figures_tex"}')
            for t, e in failed.items():
                print(f'  FAILED {t.name}: {e}')
            if failed:
                sys.exit(1)


if __name__ == '__main__':
    main()
