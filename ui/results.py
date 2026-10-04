"""
Serve the results: campaign pages (with their analysis) and the explorer of every run's timing row.

A campaign is a directory results/<name>/ with one <series>/runs.jsonl per system, written by
engine/campaign.py, and the analysis/ written by analyze_verified.py. All views here only read.
"""

from __future__ import annotations

from flask import abort, jsonify, render_template, request, send_from_directory

from ui import data as uidata
from ui.site import plot_styles, site

CAMPAIGN_TEXT_FILES = ('README.md', 'versions.txt', 'pip_freeze.txt', 'code.patch')


def _campaign_or_404(name: str):
    """Return the directory of a discovered campaign, or end the request with 404."""
    directory = uidata.campaign_dir(site().base_dir, name)
    if directory is None:
        abort(404)
    return directory


def campaigns_list():
    """Show every campaign under results/ with its outcome counts."""
    campaigns = [uidata.campaign_info(d) for d in uidata.campaign_dirs(site().base_dir)]
    return render_template('campaigns.html', campaigns=campaigns)


def campaign_detail(name: str):
    """Show one campaign: outcomes, race and matrix charts, figures, failures and files."""
    directory = _campaign_or_404(name)
    rows = uidata.summary_rows(directory)
    linear_sizes, linear_modes = uidata.sizes_and_modes(rows, uidata.LINEAR_GRAPHS)
    large_sizes, large_modes = uidata.sizes_and_modes(rows, uidata.LARGE_GRAPHS)
    failures = uidata.failure_rows(directory)
    present = {r['graph'] for r in rows}
    limits = {float(f['limit_s']) for f in failures if f.get('limit_s')}
    return render_template(
        'campaign_detail.html',
        info=uidata.campaign_info(directory),
        figures=uidata.campaign_figures(directory),
        failures=failures,
        linear_sizes=linear_sizes,
        linear_modes=linear_modes,
        large_sizes=large_sizes,
        large_modes=large_modes,
        files=[f for f in CAMPAIGN_TEXT_FILES if (directory / f).exists()],
        tables=sorted(p.name for p in (directory / 'analysis').glob('table_*.tex')),
        plot_styles=plot_styles(),
        graphs=[g for g in uidata.LINEAR_GRAPHS + uidata.LARGE_GRAPHS if g in present],
        large_graphs=[g for g in uidata.LARGE_GRAPHS if g in present],
        graph_labels={g.name: g.display_name for g in site().graph_types()},
        limit_s=max(limits) if limits else None,
    )


def campaign_file(name: str, filename: str):
    """Send a file of a campaign: its README or versions, or anything under analysis/ (figures, tables)."""
    directory = _campaign_or_404(name)
    if filename in CAMPAIGN_TEXT_FILES:
        return send_from_directory(directory, filename, mimetype='text/plain')
    mimetype = 'text/plain' if filename.endswith(('.tex', '.csv', '.json')) else None
    # send_from_directory refuses paths that leave the directory
    return send_from_directory(directory / 'analysis', filename, mimetype=mimetype)


def _large_matrix(rows: list[dict], graph: str, sizes: list[int], mode: str, metric: str) -> dict:
    """Return the matrix of one large graph: one row per size, one cell per series."""
    out: dict = {'family': 'large', 'graph': graph, 'mode': mode, 'metric': metric, 'sizes': sizes, 'rows': []}
    for size in sizes:
        m = uidata.matrix(rows, [graph], size, mode, metric)
        out['rows'].append({'n': size, 'cells': m['cells'].get(graph, {})})
    out['series'] = sorted({s for r in out['rows'] for s in r['cells']})
    vals = [
        c['value'] for r in out['rows'] for c in r['cells'].values() if c['value'] is not None and c['status'] == 'ok'
    ]
    out['min'], out['max'] = (min(vals), max(vals)) if vals else (None, None)
    return out


def api_campaign_matrix(name: str):
    """Return the system-by-topology matrix of one size and mode (or, for the large graphs, size by system)."""
    directory = uidata.campaign_dir(site().base_dir, name)
    if directory is None:
        return jsonify({'error': 'Unknown campaign'}), 404
    family = request.args.get('family', 'linear')
    graphs = uidata.LARGE_GRAPHS if family == 'large' else uidata.LINEAR_GRAPHS
    rows = uidata.summary_rows(directory)
    sizes, modes = uidata.sizes_and_modes(rows, graphs)
    try:
        n = int(request.args.get('n') or (sizes[-1] if sizes else 0))
    except ValueError:
        return jsonify({'error': 'n must be an integer'}), 400
    mode = request.args.get('mode') or 'left_recursion'
    metric = 'memory' if request.args.get('metric') == 'memory' else 'time'
    if family == 'large':
        return jsonify(_large_matrix(rows, request.args.get('graph') or uidata.LARGE_GRAPHS[0], sizes, mode, metric))
    m = uidata.matrix(rows, graphs, n, mode, metric)
    return jsonify(
        {
            'family': 'linear',
            'n': n,
            'mode': mode,
            'metric': metric,
            'sizes': sizes,
            'modes': modes,
            'graphs': m['graphs'],
            'series': m['series'],
            'cells': m['cells'],
            'min': m['min'],
            'max': m['max'],
        }
    )


def api_campaign_series(name: str):
    """Return the points of every series for one (graph, mode): mean time or memory per n, or the failure."""
    directory = uidata.campaign_dir(site().base_dir, name)
    if directory is None:
        return jsonify({'error': 'Unknown campaign'}), 404
    graph = request.args.get('graph', 'complete')
    mode = request.args.get('mode', 'left_recursion')
    metric = 'memory' if request.args.get('metric') == 'memory' else 'time'
    return jsonify(
        {
            'graph': graph,
            'mode': mode,
            'metric': metric,
            'series': uidata.series_points(uidata.summary_rows(directory), graph, mode, metric),
        }
    )


def results():
    """Show every run's timing row, by campaign, series, topology, mode and size (from runs.jsonl)."""
    tree = uidata.timing_tree(site().base_dir)
    series = sorted({sr for c in tree.values() for sr in c})
    graphs = {g for c in tree.values() for sr in c.values() for g in sr}
    modes = {m for c in tree.values() for sr in c.values() for g in sr.values() for m in g}
    order = {g: i for i, g in enumerate(uidata.LINEAR_GRAPHS + uidata.LARGE_GRAPHS)}
    styles = plot_styles()
    categories = {s.name: s.category for s in site().loader.load_systems()}
    return render_template(
        'results.html',
        tree=tree,
        config_count=sum(len(ns) for c in tree.values() for sr in c.values() for g in sr.values() for ns in g.values()),
        all_campaigns=list(tree),
        all_series=sorted(series, key=lambda x: (list(styles).index(x) if x in styles else 99, x)),
        all_graphs=sorted(graphs, key=lambda g: (order.get(g, 99), g)),
        all_modes=sorted(modes, key=lambda m: (m != 'left_recursion', m)),
        plot_styles=styles,
        categories={sr: categories.get(sr.split('_')[0], '') for sr in series},
        graph_labels={g.name: g.display_name for g in site().graph_types()},
    )


def api_result_runs(campaign: str, series: str, graph: str, mode: str, n: int):
    """Return every run of one configuration with its phases, and the mean of the successful runs."""
    directory = uidata.campaign_dir(site().base_dir, campaign)
    if directory is None or not (directory / series / 'runs.jsonl').is_file():
        return jsonify({'error': 'Not found'}), 404
    return jsonify(uidata.timing_table(directory, series, graph, mode, n))


def api_compare_trends():
    """Return the mean of every timing column per size, mode and series, for one campaign and topology."""
    req = request.get_json(silent=True) or {}
    directory = uidata.campaign_dir(site().base_dir, str(req.get('campaign', '')))
    graph = req.get('graph_type')
    modes = req.get('modes') or ([req['mode']] if req.get('mode') else [])
    series = req.get('systems') or []
    if directory is None or not graph or not modes or not series:
        return jsonify({'error': 'Choose a campaign, a topology, a mode and a system'}), 400
    return jsonify(uidata.phase_means(directory, series, graph, modes))


# (rule, view, methods), registered by create_app() under the view's name as endpoint
ROUTES = [
    ('/campaigns', campaigns_list, ['GET']),
    ('/campaigns/<name>', campaign_detail, ['GET']),
    ('/campaigns/<name>/file/<path:filename>', campaign_file, ['GET']),
    ('/api/campaigns/<name>/matrix', api_campaign_matrix, ['GET']),
    ('/api/campaigns/<name>/series', api_campaign_series, ['GET']),
    ('/results', results, ['GET']),
    ('/api/results/<campaign>/<series>/<graph>/<mode>/<int:n>', api_result_runs, ['GET']),
    ('/api/compare/trends', api_compare_trends, ['POST']),  # reads only; POST carries the selection
]
# POST views that only read, which the read-only deployment therefore allows
READ_ONLY_POSTS = {'api_compare_trends'}
