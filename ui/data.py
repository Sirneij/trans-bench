"""
Read what the Web UI shows besides the descriptors: campaigns, run timings, previews, versions.

* Verified campaigns: results/<campaign>/ (one directory per series with runs.jsonl, and the
  analysis written by analyze_verified.py), summarised for the campaign pages.
* Topology previews: a small instance of every graph type, laid out for a diagram.
* System versions: detected once, in the background, instead of on every page load.

Everything here is read-only and cached by file modification time.
"""

from __future__ import annotations

import csv
import json
import math
import re
import threading
from collections import Counter, defaultdict
from functools import lru_cache
from pathlib import Path
from typing import Any

from engine.failures import classify_failure

# ─────────────────────────────────────────────────────────────────────────────
# Verified campaigns
# ─────────────────────────────────────────────────────────────────────────────

LINEAR_GRAPHS = [
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
LARGE_GRAPHS = ['scale_free', 'barabasi_albert']
FAILURE_ORDER = ['timeout', 'oom', 'unsupported', 'iteration_limit', 'killed', 'error']
_cache: dict[tuple, tuple[float, Any]] = {}
_cache_lock = threading.Lock()


def _cached(key: tuple, path: Path, compute):
    """compute() once per modification time of `path`."""
    mtime = path.stat().st_mtime if path.exists() else 0.0
    with _cache_lock:
        hit = _cache.get(key)
        if hit and hit[0] == mtime:
            return hit[1]
    value = compute()
    with _cache_lock:
        _cache[key] = (mtime, value)
    return value


def campaign_dirs(base: Path) -> list[Path]:
    """Return every campaign directory under results/ (one with at least one */runs.jsonl), newest name first."""
    root = base / 'results'
    if not root.is_dir():
        return []
    return sorted(
        (d for d in root.iterdir() if d.is_dir() and any(d.glob('*/runs.jsonl'))), key=lambda d: d.name, reverse=True
    )


def campaign_dir(base: Path, name: str) -> Path | None:
    """Return the directory of a campaign, only if `name` is one of the discovered campaigns."""
    return next((d for d in campaign_dirs(base) if d.name == name), None)


def _read_runs(directory: Path) -> dict:
    per_series: dict[str, Counter] = {}
    total: Counter = Counter()
    first = last = None
    memory_runs = 0
    for f in sorted(directory.glob('*/runs.jsonl')):
        c: Counter = Counter()
        for line in f.read_text(encoding='utf-8').splitlines():
            if not line.strip():
                continue
            r = json.loads(line)
            status = r.get('status')
            if status in ('ok', 'skipped'):
                kind = status
            else:  # records before the `failure` field: same rules as the analysis
                kind = r.get('failure') or classify_failure(status, r.get('exit_code'), r.get('errors') or [])
            c[kind] += 1
            if isinstance(r.get('memory'), dict) and 'used_mb' in r['memory']:
                memory_runs += 1
            for key in ('start', 'end'):
                if r.get(key):
                    first = min(first or r[key], r[key])
                    last = max(last or r[key], r[key])
        per_series[f.parent.name] = c
        total.update(c)
    return {'series': per_series, 'total': total, 'first': first, 'last': last, 'memory_runs': memory_runs}


def campaign_info(directory: Path) -> dict:
    """Headline facts of a campaign (cached by the modification time of its first runs.jsonl)."""
    marker = next(iter(sorted(directory.glob('*/runs.jsonl'))), directory)

    def compute():
        runs = _read_runs(directory)
        analysis = directory / 'analysis'
        verification = {}
        if (analysis / 'verification.json').exists():
            verification = json.loads((analysis / 'verification.json').read_text())
        readme = directory / 'README.md'
        title = directory.name
        intro = ''
        if readme.exists():
            lines = readme.read_text(encoding='utf-8').splitlines()
            title = next((line[2:].strip() for line in lines if line.startswith('# ')), title)
            # first paragraph after the title, as plain text
            para: list[str] = []
            for line in lines:
                if line.startswith('#'):
                    if para:
                        break
                    continue
                if not line.strip():
                    if para:
                        break
                    continue
                para.append(line.strip())
            intro = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', re.sub(r'[*`]', '', ' '.join(para)))
            if intro.endswith(':') and '. ' in intro:  # drop a sentence that introduces a list
                intro = intro[: intro.rindex('. ') + 1]
        versions = {}
        if (directory / 'versions.txt').exists():
            for line in (directory / 'versions.txt').read_text().splitlines():
                k, _, v = line.partition(':')
                if v:
                    versions[k.strip()] = v.strip()
        total = runs['total']
        executed = sum(v for k, v in total.items() if k != 'skipped')
        return {
            'name': directory.name,
            'title': title,
            'intro': intro,
            'versions': versions,
            'series': {k: dict(v) for k, v in sorted(runs['series'].items())},
            'total': dict(total),
            'executed': executed,
            'skipped': total.get('skipped', 0),
            'ok': total.get('ok', 0),
            'failures': {k: total.get(k, 0) for k in FAILURE_ORDER if total.get(k)},
            'first': runs['first'],
            'last': runs['last'],
            'memory_runs': runs['memory_runs'],
            'incorrect': verification.get('incorrect_results', {}),
            'agreement': verification.get('scale_free_ba_cross_system_agreement', {}),
            'has_analysis': (analysis / 'summary.csv').exists(),
        }

    return _cached(('info', str(directory)), marker, compute)


def summary_rows(directory: Path) -> list[dict]:
    """Return the rows of analysis/summary.csv with numbers parsed (cached by modification time)."""
    path = directory / 'analysis' / 'summary.csv'
    if not path.exists():
        return []

    def compute():
        rows = []
        with open(path, newline='', encoding='utf-8') as f:
            for r in csv.DictReader(f):
                r['n'] = int(r['n'])
                for k in (
                    'mean',
                    'median',
                    'sd',
                    'min',
                    'max',
                    'cpu_mean',
                    'mem_used_mb',
                    'mem_used_sd',
                    'mem_peak_mb',
                ):
                    r[k] = float(r[k]) if r.get(k) not in (None, '') else None
                r['runs'] = int(r['runs']) if r.get('runs') else 0
                rows.append(r)
        return rows

    return _cached(('summary', str(directory)), path, compute)


def failure_rows(directory: Path) -> list[dict]:
    """Return the rows of analysis/failures.csv (cached by modification time)."""
    path = directory / 'analysis' / 'failures.csv'
    if not path.exists():
        return []

    def compute():
        with open(path, newline='', encoding='utf-8') as f:
            return list(csv.DictReader(f))

    return _cached(('failures', str(directory)), path, compute)


def campaign_figures(directory: Path) -> list[dict]:
    """Return the figures of a campaign's analysis: PDF, LaTeX source and compiled LaTeX, per graph and metric."""
    figs = directory / 'analysis' / 'figures'
    tex = directory / 'analysis' / 'figures_tex'
    out = []
    for pdf in sorted(figs.glob('*.pdf')) if figs.is_dir() else []:
        graph, _, metric = pdf.stem.rpartition('_')
        out.append(
            {
                'name': pdf.stem,
                'graph': graph,
                'metric': metric,
                'pdf': f'figures/{pdf.name}',
                'tex': f'figures_tex/{pdf.stem}.tex' if (tex / f'{pdf.stem}.tex').exists() else None,
                'tex_pdf': f'figures_tex/{pdf.name}' if (tex / pdf.name).exists() else None,
            }
        )
    return out


def matrix(rows: list[dict], graphs: list[str], n: int, mode: str, metric: str) -> dict:
    """Cells of a graph x series table at one size: the reported value (time or memory) or the failure."""
    by_key = {(r['series'], r['graph'], r['mode'], r['n']): r for r in rows}
    series = sorted({r['series'] for r in rows})
    single = {'neo4j', 'mongodb'}  # one formulation, recorded as left recursion
    cells: dict[str, dict] = defaultdict(dict)
    values = []
    for g in graphs:
        for s in series:
            md = 'left_recursion' if s in single and mode in ('left_recursion', 'right_recursion') else mode
            r = by_key.get((s, g, md, n))
            if r is None:
                continue
            v = r['mean'] if metric == 'time' else r['mem_used_mb']
            cell = {
                'status': r['status'],
                'failure': r.get('failure') or None,
                'value': v,
                'median': r['median'],
                'sd': r['sd'],
                'runs': r['runs'],
                'correct': r.get('all_correct'),
                'mem_peak': r['mem_peak_mb'],
                'mode': md,
            }
            if v is not None and r['status'] == 'ok':
                values.append(v)
            cells[g][s] = cell
    present = [s for s in series if any(s in cells[g] for g in graphs)]
    return {
        'graphs': [g for g in graphs if cells.get(g)],
        'series': present,
        'cells': cells,
        'min': min(values) if values else None,
        'max': max(values) if values else None,
    }


def series_points(rows: list[dict], graph: str, mode: str, metric: str) -> dict[str, list[dict]]:
    """Map each series to [{n, value, sd, status, failure}] in increasing n, for one graph and mode."""
    single = {'neo4j', 'mongodb'}
    out: dict[str, list[dict]] = defaultdict(list)
    for r in sorted(rows, key=lambda r: r['n']):
        if r['graph'] != graph:
            continue
        want = 'left_recursion' if r['series'] in single and mode in ('left_recursion', 'right_recursion') else mode
        if r['mode'] != want:
            continue
        v = r['mean'] if metric == 'time' else r['mem_used_mb']
        out[r['series']].append(
            {
                'n': r['n'],
                'value': v,
                'sd': r['sd'] if metric == 'time' else r['mem_used_sd'],
                'status': r['status'],
                'failure': r.get('failure') or None,
            }
        )
    return dict(sorted(out.items()))


def leaderboard(rows: list[dict]) -> list[dict]:
    """
    Who is fastest on the structured topologies at the largest n, per series.

    A contest is one (topology, left or right recursion) at the largest n measured for it; the fastest completed
    series wins it. `trend` is the geometric mean of the mean times over the topologies the series completed at
    each n (left recursion), for a sparkline.
    """

    def compute():
        single = {'neo4j', 'mongodb'}
        by_key = {(r['series'], r['graph'], r['mode'], r['n']): r for r in rows if r['graph'] in LINEAR_GRAPHS}
        series = sorted({r['series'] for r in rows})
        stats = {s: {'series': s, 'wins': 0, 'podiums': 0, 'contests': 0, 'completed': 0, 'ranks': []} for s in series}
        for g in LINEAR_GRAPHS:
            sizes = sorted({r['n'] for r in rows if r['graph'] == g})
            if not sizes:
                continue
            n = sizes[-1]
            for mode in ('left_recursion', 'right_recursion'):
                times = []
                for s in series:
                    md = 'left_recursion' if s in single else mode
                    r = by_key.get((s, g, md, n))
                    if r is None:
                        continue
                    stats[s]['contests'] += 1
                    if r['status'] == 'ok' and r['mean'] is not None:
                        stats[s]['completed'] += 1
                        times.append((r['mean'], s))
                for rank, (_, s) in enumerate(sorted(times), start=1):
                    stats[s]['ranks'].append(rank)
                    stats[s]['wins'] += rank == 1
                    stats[s]['podiums'] += rank <= 3
        out = []
        for s, st in stats.items():
            if not st['contests']:
                continue
            trend = []
            for n in sorted({r['n'] for r in rows if r['graph'] in LINEAR_GRAPHS}):
                vals = [
                    by_key[(s, g, 'left_recursion', n)]['mean']
                    for g in LINEAR_GRAPHS
                    if (s, g, 'left_recursion', n) in by_key
                    and by_key[(s, g, 'left_recursion', n)]['status'] == 'ok'
                    and by_key[(s, g, 'left_recursion', n)]['mean']
                ]
                if vals:
                    trend.append({'n': n, 'value': math.exp(sum(math.log(v) for v in vals) / len(vals))})
            ranks = st.pop('ranks')
            st['mean_rank'] = sum(ranks) / len(ranks) if ranks else None
            st['trend'] = trend
            out.append(st)
        out.sort(key=lambda x: (-x['wins'], -x['podiums'], x['mean_rank'] or 99))
        return out

    return compute()


def sizes_and_modes(rows: list[dict], graphs: list[str]) -> tuple[list[int], list[str]]:
    """Return the sizes and the modes that occur in the rows of the given graphs."""
    sel = [r for r in rows if r['graph'] in graphs]
    return sorted({r['n'] for r in sel}), sorted({r['mode'] for r in sel})


# ─────────────────────────────────────────────────────────────────────────────
# Timing rows of every run (the results explorer)
# ─────────────────────────────────────────────────────────────────────────────


def series_runs(runs_file: Path) -> list[dict]:
    """Return the records of one series' runs.jsonl that carry a timing row (cached by modification time)."""

    def compute():
        out = []
        for line in runs_file.read_text(encoding='utf-8').splitlines():
            if line.strip():
                r = json.loads(line)
                if r.get('timing_row'):
                    out.append(r)
        return out

    return _cached(('series_runs', str(runs_file)), runs_file, compute)


def timing_tree(base: Path) -> dict:
    """Map campaign -> series -> graph -> mode -> sorted sizes, for every configuration with timing rows."""
    order = {g: i for i, g in enumerate(LINEAR_GRAPHS + LARGE_GRAPHS)}
    tree: dict = {}
    for directory in campaign_dirs(base):
        for runs_file in sorted(directory.glob('*/runs.jsonl')):
            node: dict = {}
            for r in series_runs(runs_file):
                node.setdefault(r['graph'], {}).setdefault(r['mode'], set()).add(int(r['n']))
            if node:
                tree.setdefault(directory.name, {})[runs_file.parent.name] = {
                    g: {m: sorted(ns) for m, ns in sorted(modes.items())}
                    for g, modes in sorted(node.items(), key=lambda kv: (order.get(kv[0], 99), kv[0]))
                }
    return tree


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def timing_table(directory: Path, series: str, graph: str, mode: str, n: int) -> dict:
    """Every run of one configuration: its timing row, wall time and status, then the mean of the ok runs."""
    runs = [
        r
        for r in series_runs(directory / series / 'runs.jsonl')
        if r['graph'] == graph and r['mode'] == mode and int(r['n']) == n
    ]
    if not runs:
        return {'columns': [], 'rows': []}
    phases = list(runs[0]['timing_row'])
    rows = [
        {
            'Run': str(r.get('run')),
            **{c: r['timing_row'].get(c, '') for c in phases},
            'Wall s': r.get('wall_s', ''),
            'Status': r.get('failure') or r.get('status', ''),
        }
        for r in runs
    ]
    ok = [r['timing_row'] for r in runs if r.get('status') == 'ok']
    if ok:
        means = {}
        for c in phases:
            values = [v for v in (_float(t.get(c)) for t in ok) if v is not None]
            means[c] = sum(values) / len(values) if values else ''
        rows.append({'Run': 'Average', **means, 'Wall s': '', 'Status': f'{len(ok)} ok'})
    return {'columns': ['Run', *phases, 'Wall s', 'Status'], 'rows': rows}


def phase_means(directory: Path, series: list[str], graph: str, modes: list[str]) -> list[dict]:
    """[{'size': n, 'modes': {mode: {series: {column: mean over ok runs}}}}], sorted by n."""
    by_size: dict[int, dict] = {}
    for name in series:
        runs_file = directory / name / 'runs.jsonl'
        if not runs_file.is_file():
            continue
        groups: dict[tuple[str, int], list[dict]] = defaultdict(list)
        for r in series_runs(runs_file):
            if r['graph'] == graph and r['mode'] in modes and r.get('status') == 'ok':
                groups[(r['mode'], int(r['n']))].append(r['timing_row'])
        for (mode, n), rows in groups.items():
            cols: dict[str, float] = {}
            for c in rows[0]:
                values = [v for v in (_float(t.get(c)) for t in rows) if v is not None]
                if values:
                    cols[c] = sum(values) / len(values)
            by_size.setdefault(n, {}).setdefault(mode, {})[name] = cols
    return [{'size': n, 'modes': by_size[n]} for n in sorted(by_size)]


# ─────────────────────────────────────────────────────────────────────────────
# Topology previews
# ─────────────────────────────────────────────────────────────────────────────

PREVIEW_N = {
    'complete': 6,
    'max_acyclic': 6,
    'cycle': 10,
    'cycle_with_shortcuts': 12,
    'path': 7,
    'multi_path': 4,
    'grid': 16,
    'binary_tree': 16,
    'reverse_binary_tree': 16,
    'x': 5,
    'y': 5,
    'w': 5,
    'star': 9,
    'barabasi_albert': 22,
    'scale_free': 22,
}


def _hub_layout(name: str, nodes: list[int], n: int) -> dict[int, tuple[float, float]]:
    """Lay out the graphs with one hub: star (around it), X and Y (sources left, the rest right)."""
    hub = n + 1 if name in ('x', 'y') else 1
    pos: dict[int, tuple[float, float]] = {hub: (0.42 if name != 'star' else 0.5, 0.5)}
    if name == 'star':
        others = [v for v in nodes if v != hub]
        for i, v in enumerate(others):
            a = 2 * math.pi * i / len(others)
            pos[v] = (0.5 + 0.45 * math.cos(a), 0.5 + 0.45 * math.sin(a))
        return pos
    srcs = [v for v in nodes if v <= n]
    outs = [v for v in nodes if v > n + 1]
    for i, v in enumerate(srcs):
        pos[v] = (0.0, i / max(len(srcs) - 1, 1))
    for i, v in enumerate(outs):
        if name == 'x':
            pos[v] = (1.0, i / max(len(outs) - 1, 1))
        else:  # y: a path leaving the hub
            pos[v] = (0.42 + 0.58 * (i + 1) / len(outs), 0.5 + (0.14 if i % 2 else -0.14))
    return pos


def _force_layout(nodes: list[int], edges: list[tuple[int, int]]) -> dict[int, tuple[float, float]]:
    """Lay out any other graph force-directed (deterministic seed), scaled to [0, 1]^2."""
    import networkx as nx

    g = nx.Graph()
    g.add_nodes_from(nodes)
    g.add_edges_from((a, b) for a, b in edges if a != b)
    raw = nx.spring_layout(g, seed=7, iterations=200)
    xs = [p[0] for p in raw.values()]
    ys = [p[1] for p in raw.values()]
    return {
        v: ((x - min(xs)) / ((max(xs) - min(xs)) or 1), (y - min(ys)) / ((max(ys) - min(ys)) or 1))
        for v, (x, y) in raw.items()
    }


def _layout(name: str, nodes: list[int], edges: list[tuple[int, int]], n: int) -> dict[int, tuple[float, float]]:
    """Return a position in [0, 1]^2 for every node, in the layout that shows the topology best."""
    if name in ('x', 'y', 'star'):
        return _hub_layout(name, nodes, n)
    pos: dict[int, tuple[float, float]] = {}
    if name in ('complete', 'max_acyclic', 'cycle', 'cycle_with_shortcuts'):
        k = len(nodes)
        for i, v in enumerate(sorted(nodes)):
            a = -math.pi / 2 + 2 * math.pi * i / k
            pos[v] = (0.5 + 0.45 * math.cos(a), 0.5 + 0.45 * math.sin(a))
    elif name == 'grid':
        side = max(1, int(math.isqrt(max(nodes))))
        for v in nodes:
            r, c = divmod(v - 1, side)
            pos[v] = (c / max(side - 1, 1), r / max(side - 1, 1))
    elif name in ('binary_tree', 'reverse_binary_tree'):
        depth = {v: int(math.log2(v)) for v in nodes}
        levels = max(depth.values())
        for v in nodes:
            d = depth[v]
            slot = v - 2**d
            pos[v] = ((slot + 0.5) / 2**d, d / max(levels, 1))
    elif name == 'path':
        for v in nodes:
            pos[v] = ((v - 1) / max(len(nodes) - 1, 1), 0.5 + 0.18 * math.sin(v * 1.3))
    elif name == 'multi_path':
        k = 10
        rows = sorted({(v - 1) % k for v in nodes})
        cols = max((v - 1) // k for v in nodes)
        for v in nodes:
            pos[v] = (((v - 1) // k) / max(cols, 1), rows.index((v - 1) % k) / max(len(rows) - 1, 1))
    elif name == 'w':
        for v in nodes:
            left = v <= n
            idx = (v - 1) if left else (v - n - 1)
            pos[v] = (0.0 if left else 1.0, idx / max(n - 1, 1))
    else:  # scale-free, Barabási-Albert and anything new
        return _force_layout(nodes, edges)
    return pos


def preview_range(name: str) -> tuple[int, int, int]:
    """(min, default, max) of the size parameter n for interactive previews."""
    d = PREVIEW_N.get(name, 12)
    return max(2, d // 2), d, d * 2


@lru_cache(maxsize=256)
def graph_preview(base: str, name: str, n: int | None = None) -> dict | None:
    """
    Return a small instance of graph type `name`, laid out for drawing.

    The result has nodes with positions in [0,1]^2, the distinct edges, and the pairs that the
    transitive closure adds (`closure`); None if the generator yields nothing for this n.
    """
    import io
    import logging
    import sys
    from contextlib import redirect_stdout

    if base not in sys.path:
        sys.path.insert(0, base)
    try:
        from generate_db import DataGenerator

        gen = getattr(DataGenerator(), f'generate_{name}_graph', None)
        if gen is None:
            return None
        n = n or PREVIEW_N.get(name, 12)
        level = logging.getLogger().level
        logging.getLogger().setLevel(logging.WARNING)
        try:
            with redirect_stdout(io.StringIO()):
                edges = sorted({(int(a), int(b)) for a, b, *_ in gen(n)})
        finally:
            logging.getLogger().setLevel(level)
    except Exception:
        return None
    nodes = sorted({v for e in edges for v in e})
    if not nodes or len(edges) > 400:
        return None
    pos = _layout(name, nodes, edges, n)
    succ: dict[int, list[int]] = defaultdict(list)
    for a, b in edges:
        succ[a].append(b)
    reach: set[tuple[int, int]] = set()
    for v in nodes:  # depth-first search from every node
        stack, seen = list(succ[v]), set()
        while stack:
            w = stack.pop()
            if w in seen:
                continue
            seen.add(w)
            stack.extend(succ[w])
        reach.update((v, w) for w in seen)
    added = sorted(reach - set(edges))
    return {
        'name': name,
        'n': n,
        'nodes': [{'id': v, 'x': round(pos[v][0], 4), 'y': round(pos[v][1], 4)} for v in nodes],
        'edges': [{'a': a, 'b': b, 'self': a == b} for a, b in edges],
        'closure': [{'a': a, 'b': b} for a, b in added if a != b][:800],
        'node_count': len(nodes),
        'edge_count': len(edges),
        'closure_count': len(reach),
    }


# ─────────────────────────────────────────────────────────────────────────────
# System versions, detected once in the background
# ─────────────────────────────────────────────────────────────────────────────


class VersionCache:
    """get_system_version() runs a command per system (up to 2 s each); run it once, off the request path."""

    def __init__(self):
        """Start with no versions; start() begins the detection."""
        self.versions: dict[str, str] = {}
        self._started = False
        self._lock = threading.Lock()

    def start(self, names: list[str]) -> None:
        """Detect the versions of `names` in a background thread, once per process."""
        with self._lock:
            if self._started:
                return
            self._started = True

        def work():
            """Detect one version after the other; a failure gives 'Unknown'."""
            from engine.loader import get_system_version

            for name in names:
                try:
                    self.versions[name] = get_system_version(name)
                except Exception:
                    self.versions[name] = 'Unknown'

        threading.Thread(target=work, daemon=True).start()

    def get(self, name: str) -> str | None:
        """Return the detected version, or None while it is not known yet."""
        return self.versions.get(name)


# ─────────────────────────────────────────────────────────────────────────────
# Landing page
# ─────────────────────────────────────────────────────────────────────────────

RACE_GRAPHS = ('complete', 'cycle', 'path', 'grid')
SINGLE_FORMULATION = {'neo4j', 'mongodb'}  # one query, recorded as left recursion


def race(
    rows: list[dict],
    failures: list[dict] | None = None,
    graphs: tuple[str, ...] = RACE_GRAPHS,
    mode: str = 'left_recursion',
) -> dict:
    """Return, per graph, every series' mean time (or failure, and the n where it first failed) at its largest n."""
    first_failed = {(f['series'], f['graph'], f['mode']): int(f['first_failed_n']) for f in failures or []}
    out = {}
    for g in graphs:
        sel = [r for r in rows if r['graph'] == g]
        if not sel:
            continue
        n = max(r['n'] for r in sel)
        entries = []
        for r in sel:
            if r['n'] != n or r['mode'] != ('left_recursion' if r['series'] in SINGLE_FORMULATION else mode):
                continue
            ok = r['status'] == 'ok' and r['mean'] is not None and r.get('all_correct') not in ('False', False)
            failed_at = first_failed.get((r['series'], g, r['mode']))
            entries.append(
                {
                    'series': r['series'],
                    'value': r['mean'] if ok else None,
                    'failure': None if ok else (r.get('failure') or r['status']),
                    'status': r['status'],
                    'failed_at': None if ok else failed_at,
                }
            )
        entries.sort(key=lambda e: (e['value'] is None, e['value'] or 0, e['series']))
        out[g] = {'n': n, 'entries': entries}
    return out


def landing(base: Path) -> dict | None:
    """Return the facts of the newest analyzed campaign that the landing page animates, or None."""
    directory = next((d for d in campaign_dirs(base) if (d / 'analysis' / 'summary.csv').exists()), None)
    if directory is None:
        return None
    info = campaign_info(directory)
    verification = {}
    if (directory / 'analysis' / 'verification.json').exists():
        verification = json.loads((directory / 'analysis' / 'verification.json').read_text(encoding='utf-8'))
    counts = verification.get('counts', {})
    rows = summary_rows(directory)
    failures = failure_rows(directory)
    limits = [float(f['limit_s']) for f in failures if f.get('limit_s')]
    return {
        'campaign': directory.name,
        'executed': info['executed'],
        'checked': counts.get('runs_ok_checked', 0),
        'failures': info['failures'],
        'incorrect': info['incorrect'],
        'series': list(info['series']),
        # mariadb_tuned is MariaDB with another setting, not another system
        'systems': sorted({name.split('_')[0] for name in info['series']}),
        'first': info['first'],
        'last': info['last'],
        'limit_s': max(limits) if limits else 600.0,
        'race': race(rows, failures),
    }
