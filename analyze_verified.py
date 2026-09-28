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
5 runs, reported only if all 5 runs completed; medians, standard deviations, minima and maxima are
in summary.csv. Figures and tables are laid out for the paper (graph families, system order and
LaTeX macros such as \\gname are those of the paper).
"""
import argparse
import csv
import json
import statistics
import tarfile
from collections import defaultdict
from functools import lru_cache
from pathlib import Path

import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker  # noqa: E402

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
SYS = ['xsb', 'postgres', 'mariadb', 'duckdb', 'cockroachdb', 'mongodb', 'neo4j', 'singlestore', 'mariadb_tuned']
SNAME = {'xsb': 'XSB', 'postgres': 'PostgreSQL', 'mariadb': 'MariaDB', 'duckdb': 'DuckDB',
         'cockroachdb': 'CockroachDB', 'mongodb': 'MongoDB', 'neo4j': 'Neo4j', 'singlestore': 'SingleStore',
         'mariadb_tuned': 'MariaDB (4 GB tmp)'}
STYLE = {'xsb': ('k', 'x', '-'), 'postgres': ('tab:blue', 's', '-'), 'mariadb': ('tab:red', 'v', '-'),
         'duckdb': ('goldenrod', '^', '-'), 'cockroachdb': ('tab:purple', 'D', '-'),
         'mongodb': ('tab:green', 'o', '--'), 'neo4j': ('tab:cyan', 'P', '--'), 'singlestore': ('tab:brown', '*', ':'),
         'mariadb_tuned': ('salmon', 'v', ':')}
RESULTS = Path('.')
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
        rows[key] = dict(series=series, graph=key[1], mode=key[2], n=key[3], status=status, runs=len(ok),
                         mean=statistics.mean(t) if len(t) == 5 and status == 'ok' else None,
                         median=statistics.median(t) if t else None,
                         sd=statistics.stdev(t) if len(t) > 1 else None, min=min(t) if t else None,
                         max=max(t) if t else None,
                         cpu_mean=statistics.mean(c) if len(c) == 5 and status == 'ok' else None,
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


def plot_graph(rows, g, sizes, out: Path, cpu=False):
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.9), sharey=True)
    for ax, m in zip(axes, ['left_recursion', 'right_recursion']):
        series = ['xsb', 'duckdb'] if cpu else ['xsb', 'postgres', 'mariadb', 'duckdb', 'cockroachdb', 'neo4j', 'mongodb']
        for s in series:
            mm = 'left_recursion' if s in ('neo4j', 'mongodb') else m
            xs, ys, to_x = [], [], None
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
                    to_x = n
            if not xs and to_x is None:
                continue
            col, mk, ls = STYLE[s]
            label = SNAME[s] + (' (single query)' if s in ('neo4j', 'mongodb') else '')
            ax.plot(xs, ys, color=col, marker=mk, linestyle=ls, markersize=3.5, linewidth=1.1, label=label)
            if to_x is not None:  # first size that exceeded the limit / failed
                ax.plot([to_x], [600], color=col, marker=mk, markersize=6, markerfacecolor='none', linestyle='')
        ax.set_yscale('log')
        ax.set_title(f"{TITLE[g]}: {'left' if m.startswith('left') else 'right'} recursion", fontsize=9)
        ax.set_xlabel('n' if g not in ('scale_free', 'barabasi_albert') else 'number of nodes', fontsize=8)
        ax.tick_params(labelsize=7)
        if g in ('scale_free', 'barabasi_albert'):
            ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f'{int(v / 1000)}k'))
        ax.grid(True, which='major', linewidth=0.3)
    axes[0].set_ylabel(('CPU' if cpu else 'Elapsed') + ' time (s, log scale)', fontsize=8)
    h, lab = axes[0].get_legend_handles_labels()
    h2, lab2 = axes[1].get_legend_handles_labels()
    for hh, ll in zip(h2, lab2):
        if ll not in lab:
            h.append(hh)
            lab.append(ll)
    if not lab:
        plt.close(fig)
        return
    fig.legend(h, lab, loc='lower center', ncol=4 if len(lab) > 4 else len(lab), fontsize=7, frameon=False,
               bbox_to_anchor=(0.5, -0.02))
    fig.tight_layout(rect=(0, 0.1 if len(lab) > 4 else 0.06, 1, 1))
    fig.savefig(out / f"{g}_{'cpu' if cpu else 'elapsed'}.pdf")
    plt.close(fig)


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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('results')
    ap.add_argument('--out', default=None)
    a = ap.parse_args()
    global RESULTS
    results = Path(a.results)
    RESULTS = results
    out = Path(a.out) if a.out else results / 'analysis'
    (out / 'figures').mkdir(parents=True, exist_ok=True)
    runs = load(results)
    rows = summarize(runs)
    apply_agreement(rows)
    write_summary(rows, out)
    v = verification(runs, rows, out)
    print(json.dumps(v['counts']), json.dumps(v['incorrect_results']))
    for g in GRAPHS:
        plot_graph(rows, g, list(range(100, 1001, 100)), out / 'figures')
        plot_graph(rows, g, list(range(100, 1001, 100)), out / 'figures', cpu=True)
    for g, sizes in (('scale_free', range(10000, 90001, 10000)), ('barabasi_albert', range(10000, 100001, 10000))):
        plot_graph(rows, g, list(sizes), out / 'figures')
        plot_graph(rows, g, list(sizes), out / 'figures', cpu=True)
        for m in ('left_recursion', 'right_recursion'):
            table_large(rows, g, list(sizes), m, out)
    for n in (500, 1000):
        table_linear(rows, n, out)
        table_double(rows, n, out)


if __name__ == '__main__':
    main()
