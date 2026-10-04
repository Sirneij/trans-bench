"""
Serve the pages that show the suite: dashboard, topologies, systems, and the descriptors as JSON.

Every view here only reads; ui/editing.py has the views that change files.
"""

from __future__ import annotations

import inspect
import os
import platform

from flask import jsonify, redirect, render_template, request, url_for

from ui import data as uidata
from ui.site import plot_styles, read_only, site

# The edge set of every topology, as shown on its page (LaTeX, rendered by KaTeX in the browser).
GRAPH_MATH = {
    'complete': {'symbol': 'K_n', 'definition': r'\{(i,j) \mid i \in 1..n, j \in 1..n\}'},
    'max_acyclic': {'symbol': 'T_n', 'definition': r'\{(i,j) \mid i \in 1..n-1, j \in i+1..n\}'},
    'cycle': {'symbol': 'C_n', 'definition': r'\{(i,i+1) \mid i \in 1..n-1\} \cup \{(n,1)\}'},
    'cycle_with_shortcuts': {
        'symbol': 'S_{n,k}',
        'definition': r'\{(i, (i-1 + t \cdot n/(k+1)) \bmod n + 1) \mid i \in 1..n, t \in 1..k\} \cup C_n',
    },
    'path': {'symbol': 'P_n', 'definition': r'\{(i,i+1) \mid i \in 1..n-1\}'},
    'multi_path': {'symbol': 'M_{n,k}', 'definition': r'\{(i,i+k) \mid i \in 1..(n-1) \cdot k\}'},
    'grid': {
        'symbol': 'G_{n \times n}',
        'definition': r'\{(j, j+1) \mid i \in 1..n, j \in (i-1)n+1..in-1\}'
        r' \cup \{(j, j+n) \mid i \in 1..n-1, j \in (i-1)n+1..in\}',
    },
    'binary_tree': {
        'symbol': 'B_h',
        'definition': r'\{(i, 2i) \mid i \in 1..2^{h-1}\} \cup \{(i, 2i+1) \mid i \in 1..2^{h-1}\}',
    },
    'reverse_binary_tree': {
        'symbol': 'V_h',
        'definition': r'\{(2i, i) \mid i \in 1..2^{h-1}\} \cup \{(2i+1, i) \mid i \in 1..2^{h-1}\}',
    },
    'x': {'symbol': 'X_{n,k}', 'definition': r'\{(i, n+1) \mid i \in 1..n\} \cup \{(n+1, n+1+j) \mid j \in 1..k\}'},
    'y': {'symbol': 'Y_{n,k}', 'definition': r'\{(i, n+1) \mid i \in 1..n\} \cup \{(i, i+1) \mid i \in n+1..n+k-1\}'},
    'w': {'symbol': 'W_{n,k}', 'definition': r'\{(i, n+1 + (i+j-1) \bmod n) \mid i \in 1..n, j \in 1..k\}'},
    'barabasi_albert': {
        'symbol': 'BA_{n,m}',
        'definition': r'\text{Scale-free network generated using preferential attachment with } m \text{ edges.}',
    },
    'scale_free': {'symbol': 'SF_n', 'definition': r'\text{Directed scale-free graph.}'},
    'star': {'symbol': 'S_n', 'definition': r'\{(i, 1) \mid i \in 2..n\}'},
}
NO_MATH = {'symbol': 'G_n', 'definition': r'\text{No formal definition available.}'}


def landing():
    """Show the landing page: the motion graphics that explain the suite, driven by the newest campaign."""
    s = site()
    graph_types = s.graph_types()
    facts = uidata.landing(s.base_dir)
    return render_template(
        'landing.html',
        facts=facts,
        plot_styles=plot_styles(),
        graph_types=graph_types,
        graph_labels={g.name: g.display_name for g in graph_types},
        previews=s.previews(graph_types),
        system_count=len(s.loader.load_systems()),
    )


def dashboard():
    """Show the overview: campaigns, the leaderboard of the newest analyzed one, systems and topologies."""
    s = site()
    systems = s.systems()
    graph_types = s.graph_types()
    analyzed = s.analyzed_campaign()
    return render_template(
        'dashboard.html',
        leaders=uidata.leaderboard(uidata.summary_rows(analyzed)) if analyzed else [],
        leaders_campaign=analyzed.name if analyzed else None,
        plot_styles=plot_styles(),
        systems=systems,
        graph_types=graph_types,
        machine_info={
            'os': f'{platform.system()} {platform.release()}',
            'arch': platform.machine(),
            'cpu_count': os.cpu_count(),
            'python_version': platform.python_version(),
        },
        campaigns=[uidata.campaign_info(d) for d in uidata.campaign_dirs(s.base_dir)],
        system_ui=s.system_ui(systems),
        previews=s.previews(graph_types),
    )


def graphs_list():
    """Show every topology with a diagram of a small instance."""
    graph_types = site().graph_types()
    return render_template(
        'graphs.html',
        graph_types=graph_types,
        previews=site().previews(graph_types),
        math={g.name: GRAPH_MATH.get(g.name, NO_MATH) for g in graph_types},
    )


def graph_detail(name: str):
    """Show one topology: its definition, its generator's source code and an interactive instance."""
    graph = next((g for g in site().graph_types() if g.name == name), None)
    if not graph:
        return redirect(url_for('dashboard'))
    try:
        from engine.data_generator import DataGenerator  # generate_db.py, importable from anywhere

        method = getattr(DataGenerator(), f'generate_{name}_graph', None)
        code_impl = inspect.getsource(method) if method else '# No Python implementation found.'
    except Exception as e:
        code_impl = f'# Error loading implementation: {e}'
    return render_template(
        'graph_detail.html',
        graph=graph,
        code_impl=code_impl,
        math_info=GRAPH_MATH.get(name, NO_MATH),
        preview=uidata.graph_preview(str(site().base_dir), name),
        preview_range=uidata.preview_range(name),
    )


def api_graph_preview(name: str):
    """Return a drawable instance of a topology for size n (bounded, see ui/data.py preview_range)."""
    if name not in {g.name for g in site().graph_types()}:
        return jsonify({'error': 'Unknown graph type'}), 404
    lo, default, hi = uidata.preview_range(name)
    try:
        n = min(hi, max(lo, int(request.args.get('n', default))))
    except ValueError:
        return jsonify({'error': 'n must be an integer'}), 400
    preview = uidata.graph_preview(str(site().base_dir), name, n)
    if preview is None:
        return jsonify({'error': 'No preview for this size'}), 422
    return jsonify(preview)


def systems_list():
    """Show every system with its place in the leaderboard of the newest analyzed campaign."""
    s = site()
    systems = s.systems()
    analyzed = s.analyzed_campaign()
    leaders = {x['series']: x for x in uidata.leaderboard(uidata.summary_rows(analyzed))} if analyzed else {}
    return render_template(
        'systems.html',
        systems=systems,
        system_ui=s.system_ui(systems),
        leaders=leaders,
        leaders_campaign=analyzed.name if analyzed else None,
    )


def system_detail(name: str):
    """Show one system: descriptor, timing phases, rule files, and (locally) the credentials editor."""
    system = site().system(name)
    if system is None:
        return redirect(url_for('systems_list'))
    descriptor_yaml = system.descriptor_path.read_text(encoding='utf-8')
    cred_path = system.system_dir / 'credentials.yaml'
    # the public read-only deployment never shows credentials, even if a file were present
    cred_yaml = cred_path.read_text(encoding='utf-8') if cred_path.exists() and not read_only() else ''
    rule_files = sorted(system.rules_dir.glob(f'*{system.rule_extension}')) if system.rules_dir.exists() else []
    return render_template(
        'system_detail.html',
        system=system,
        descriptor_yaml=descriptor_yaml,
        cred_yaml=cred_yaml,
        rule_files=[r.name for r in rule_files],
        system_ui=site().system_ui([system]),
        has_example=(system.system_dir / 'credentials.example.yaml').exists(),
    )


def api_systems():
    """Return every system descriptor as JSON."""
    return jsonify([s.to_dict() for s in site().systems()])


def api_graph_types():
    """Return every graph type descriptor as JSON."""
    return jsonify([g.to_dict() for g in site().graph_types()])


def api_nav():
    """Return the names for the command palette (no version detection, no I/O beyond the descriptors)."""
    s = site()
    return jsonify(
        {
            'systems': [{'name': x.name, 'label': x.display_name} for x in s.loader.load_systems()],
            'graphs': [{'name': g.name, 'label': g.display_name} for g in s.graph_types()],
            'campaigns': [d.name for d in uidata.campaign_dirs(s.base_dir)],
        }
    )


def api_domains():
    """Return every domain descriptor (name, display name, modes, description)."""
    return jsonify(
        [
            {'name': d.name, 'display_name': d.display_name, 'modes': d.modes, 'description': d.description}
            for d in site().loader.load_domains()
        ]
    )


def api_domain_modes(domain_name: str):
    """Return the modes of a domain; a domain without a descriptor gets the transitive-closure modes."""
    domain = site().loader.get_domain(domain_name)
    if domain:
        return jsonify({'domain': domain_name, 'modes': domain.modes})
    return jsonify({'domain': domain_name, 'modes': site().transitive_modes()})


# (rule, view, methods), registered by create_app() under the view's name as endpoint
ROUTES = [
    ('/', landing, ['GET']),
    ('/overview', dashboard, ['GET']),
    ('/graphs', graphs_list, ['GET']),
    ('/graphs/<name>', graph_detail, ['GET']),
    ('/api/graphs/<name>/preview', api_graph_preview, ['GET']),
    ('/systems', systems_list, ['GET']),
    ('/systems/<name>', system_detail, ['GET']),
    ('/api/systems', api_systems, ['GET']),
    ('/api/graph-types', api_graph_types, ['GET']),
    ('/api/nav', api_nav, ['GET']),
    ('/api/domains', api_domains, ['GET']),
    ('/api/domains/<domain_name>/modes', api_domain_modes, ['GET']),
]
