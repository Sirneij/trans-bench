"""
ui/data.py — data the Web UI reads besides the descriptors.

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

LINEAR_GRAPHS = ['complete', 'max_acyclic', 'cycle', 'cycle_with_shortcuts', 'path', 'multi_path', 'grid',
                 'binary_tree', 'reverse_binary_tree', 'x', 'y', 'w']
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
    root = base / 'results'
    if not root.is_dir():
        return []
    return sorted((d for d in root.iterdir() if d.is_dir() and any(d.glob('*/runs.jsonl'))),
                  key=lambda d: d.name, reverse=True)


def campaign_dir(base: Path, name: str) -> Path | None:
    """The directory of a campaign, only if `name` is one of the discovered campaigns."""
    return next((d for d in campaign_dirs(base) if d.name == name), None)


def _read_runs(directory: Path) -> dict:
    per_series: dict[str, Counter] = {}
    total = Counter()
    first = last = None
    memory_runs = 0
    for f in sorted(directory.glob('*/runs.jsonl')):
        c = Counter()
        for line in f.read_text().splitlines():
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
            lines = readme.read_text().splitlines()
            title = next((l[2:].strip() for l in lines if l.startswith('# ')), title)
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
                intro = intro[:intro.rindex('. ') + 1]
        versions = {}
        if (directory / 'versions.txt').exists():
            for line in (directory / 'versions.txt').read_text().splitlines():
                k, _, v = line.partition(':')
                if v:
                    versions[k.strip()] = v.strip()
        total = runs['total']
        executed = sum(v for k, v in total.items() if k != 'skipped')
        return {
            'name': directory.name, 'title': title, 'intro': intro, 'versions': versions,
            'series': {k: dict(v) for k, v in sorted(runs['series'].items())},
            'total': dict(total), 'executed': executed, 'skipped': total.get('skipped', 0),
            'ok': total.get('ok', 0), 'failures': {k: total.get(k, 0) for k in FAILURE_ORDER if total.get(k)},
            'first': runs['first'], 'last': runs['last'], 'memory_runs': runs['memory_runs'],
            'incorrect': verification.get('incorrect_results', {}),
            'agreement': verification.get('scale_free_ba_cross_system_agreement', {}),
            'has_analysis': (analysis / 'summary.csv').exists(),
        }

    return _cached(('info', str(directory)), marker, compute)


def summary_rows(directory: Path) -> list[dict]:
    path = directory / 'analysis' / 'summary.csv'
    if not path.exists():
        return []

    def compute():
        rows = []
        with open(path, newline='') as f:
            for r in csv.DictReader(f):
                r['n'] = int(r['n'])
                for k in ('mean', 'median', 'sd', 'min', 'max', 'cpu_mean', 'mem_used_mb', 'mem_used_sd', 'mem_peak_mb'):
                    r[k] = float(r[k]) if r.get(k) not in (None, '') else None
                r['runs'] = int(r['runs']) if r.get('runs') else 0
                rows.append(r)
        return rows

    return _cached(('summary', str(directory)), path, compute)


def failure_rows(directory: Path) -> list[dict]:
    path = directory / 'analysis' / 'failures.csv'
    if not path.exists():
        return []
    return _cached(('failures', str(directory)), path, lambda: list(csv.DictReader(open(path, newline=''))))


def campaign_figures(directory: Path) -> list[dict]:
    figs = directory / 'analysis' / 'figures'
    tex = directory / 'analysis' / 'figures_tex'
    out = []
    for pdf in sorted(figs.glob('*.pdf')) if figs.is_dir() else []:
        graph, _, metric = pdf.stem.rpartition('_')
        out.append({'name': pdf.stem, 'graph': graph, 'metric': metric, 'pdf': f'figures/{pdf.name}',
                    'tex': f'figures_tex/{pdf.stem}.tex' if (tex / f'{pdf.stem}.tex').exists() else None,
                    'tex_pdf': f'figures_tex/{pdf.name}' if (tex / pdf.name).exists() else None})
    return out


def matrix(rows: list[dict], graphs: list[str], n: int, mode: str, metric: str) -> dict:
    """Cells of a graph x series table at one size: the reported value (time or memory) or the failure."""
    by_key = {(r['series'], r['graph'], r['mode'], r['n']): r for r in rows}
    series = sorted({r['series'] for r in rows})
    single = {'neo4j', 'mongodb'}  # one formulation, recorded as left recursion
    cells = defaultdict(dict)
    values = []
    for g in graphs:
        for s in series:
            md = 'left_recursion' if s in single and mode in ('left_recursion', 'right_recursion') else mode
            r = by_key.get((s, g, md, n))
            if r is None:
                continue
            v = r['mean'] if metric == 'time' else r['mem_used_mb']
            cell = {'status': r['status'], 'failure': r.get('failure') or None, 'value': v,
                    'median': r['median'], 'sd': r['sd'], 'runs': r['runs'],
                    'correct': r.get('all_correct'), 'mem_peak': r['mem_peak_mb'], 'mode': md}
            if v is not None and r['status'] == 'ok':
                values.append(v)
            cells[g][s] = cell
    present = [s for s in series if any(s in cells[g] for g in graphs)]
    return {'graphs': [g for g in graphs if cells.get(g)], 'series': present, 'cells': cells,
            'min': min(values) if values else None, 'max': max(values) if values else None}


def series_points(rows: list[dict], graph: str, mode: str, metric: str) -> dict[str, list[dict]]:
    """series -> [{n, value, sd, status, failure}] in increasing n, for one graph and mode."""
    single = {'neo4j', 'mongodb'}
    out: dict[str, list[dict]] = defaultdict(list)
    for r in sorted(rows, key=lambda r: r['n']):
        if r['graph'] != graph:
            continue
        want = 'left_recursion' if r['series'] in single and mode in ('left_recursion', 'right_recursion') else mode
        if r['mode'] != want:
            continue
        v = r['mean'] if metric == 'time' else r['mem_used_mb']
        out[r['series']].append({'n': r['n'], 'value': v, 'sd': r['sd'] if metric == 'time' else r['mem_used_sd'],
                                 'status': r['status'], 'failure': r.get('failure') or None})
    return dict(sorted(out.items()))


def sizes_and_modes(rows: list[dict], graphs: list[str]) -> tuple[list[int], list[str]]:
    sel = [r for r in rows if r['graph'] in graphs]
    return sorted({r['n'] for r in sel}), sorted({r['mode'] for r in sel})


# ─────────────────────────────────────────────────────────────────────────────
# Topology previews
# ─────────────────────────────────────────────────────────────────────────────

PREVIEW_N = {'complete': 6, 'max_acyclic': 6, 'cycle': 10, 'cycle_with_shortcuts': 12, 'path': 7, 'multi_path': 4,
             'grid': 16, 'binary_tree': 16, 'reverse_binary_tree': 16, 'x': 5, 'y': 5, 'w': 5, 'star': 9,
             'barabasi_albert': 22, 'scale_free': 22}


def _layout(name: str, nodes: list[int], edges: list[tuple[int, int]], n: int) -> dict[int, tuple[float, float]]:
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
            slot = v - 2 ** d
            pos[v] = ((slot + 0.5) / 2 ** d, d / max(levels, 1))
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
    elif name in ('x', 'y', 'star'):
        hub = n + 1 if name in ('x', 'y') else 1
        pos[hub] = (0.42 if name != 'star' else 0.5, 0.5)
        if name == 'star':
            others = [v for v in nodes if v != hub]
            for i, v in enumerate(others):
                a = 2 * math.pi * i / len(others)
                pos[v] = (0.5 + 0.45 * math.cos(a), 0.5 + 0.45 * math.sin(a))
        else:
            srcs = [v for v in nodes if v <= n]
            outs = [v for v in nodes if v > n + 1]
            for i, v in enumerate(srcs):
                pos[v] = (0.0, i / max(len(srcs) - 1, 1))
            for i, v in enumerate(outs):
                if name == 'x':
                    pos[v] = (1.0, i / max(len(outs) - 1, 1))
                else:  # y: a path leaving the hub
                    pos[v] = (0.42 + 0.58 * (i + 1) / len(outs), 0.5 + (0.14 if i % 2 else -0.14))
    else:  # scale-free, Barabási-Albert and anything new: force-directed, deterministic
        import networkx as nx

        g = nx.Graph()
        g.add_nodes_from(nodes)
        g.add_edges_from((a, b) for a, b in edges if a != b)
        raw = nx.spring_layout(g, seed=7, iterations=200)
        xs = [p[0] for p in raw.values()]
        ys = [p[1] for p in raw.values()]
        for v, (x, y) in raw.items():
            pos[v] = ((x - min(xs)) / ((max(xs) - min(xs)) or 1), (y - min(ys)) / ((max(ys) - min(ys)) or 1))
    return pos


def preview_range(name: str) -> tuple[int, int, int]:
    """(min, default, max) of the size parameter n for interactive previews."""
    d = PREVIEW_N.get(name, 12)
    return max(2, d // 2), d, d * 2


@lru_cache(maxsize=256)
def graph_preview(base: str, name: str, n: int | None = None) -> dict | None:
    """A small instance of graph type `name` (nodes with positions in [0,1]^2, distinct edges), plus the
    pairs its transitive closure adds (`closure`), for drawing."""
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
    return {'name': name, 'n': n, 'nodes': [{'id': v, 'x': round(pos[v][0], 4), 'y': round(pos[v][1], 4)} for v in nodes],
            'edges': [{'a': a, 'b': b, 'self': a == b} for a, b in edges],
            'closure': [{'a': a, 'b': b} for a, b in added if a != b][:800],
            'node_count': len(nodes), 'edge_count': len(edges), 'closure_count': len(reach)}


# ─────────────────────────────────────────────────────────────────────────────
# System versions, detected once in the background
# ─────────────────────────────────────────────────────────────────────────────


class VersionCache:
    """get_system_version() runs a command per system (up to 2 s each); run it once, off the request path."""

    def __init__(self):
        self.versions: dict[str, str] = {}
        self._started = False
        self._lock = threading.Lock()

    def start(self, names: list[str]) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True

        def work():
            from engine.loader import get_system_version

            for name in names:
                try:
                    self.versions[name] = get_system_version(name)
                except Exception:
                    self.versions[name] = 'Unknown'

        threading.Thread(target=work, daemon=True).start()

    def get(self, name: str) -> str | None:
        return self.versions.get(name)
