"""
ui/app.py

Flask Web UI for trans-bench.
Provides a modern dashboard for managing systems, configuring and running
experiments, monitoring live progress, and browsing results — no CLI needed.

Routes
------
GET  /                       Dashboard
GET  /systems                Manage systems
GET  /systems/<name>         System detail + credential editor
POST /systems/<name>/save    Save descriptor changes
POST /systems/<name>/creds   Save credentials
GET  /experiment/new         Configure new experiment
POST /experiment/start       Start experiment (background thread)
GET  /experiment/stream      SSE stream of live progress
GET  /experiment/status      JSON status of current experiment
POST /experiment/stop        Stop running experiment
GET  /results                Browse timing results
GET  /campaigns              Verified campaigns (results/<campaign>/)
GET  /campaigns/<name>       One campaign: outcomes, matrix, figures, failures, files
GET  /graphs                 Graph topologies with diagrams of small instances
GET  /api/systems            JSON: all system descriptors
GET  /api/graph-types        JSON: all graph type descriptors
"""

from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Any, Optional

import yaml
from flask import (
    Flask,
    Response,
    abort,
    jsonify,
    redirect,
    render_template,
    request,
    send_from_directory,
    stream_with_context,
    url_for,
)

from ui import data as uidata

BASE_DIR = Path(__file__).parent.parent
log = logging.getLogger(__name__)

# ── Shared experiment state ─────────────────────────────────────────────────
_experiment_lock = threading.Lock()
_experiment_state: dict[str, Any] = {
    'running': False,
    'progress': {},
    'log_lines': [],
    'error': None,
    'started': None,
    'finished': None,
    'stop_requested': False,
}


class _EventLog:
    """Events of the current experiment, kept for every listener of /experiment/stream.

    Each event has an increasing id; a listener keeps its own position (the SSE Last-Event-ID), so any number of
    pages can follow a run, a page opened late replays it from the start, and a reconnect resumes where it stopped.
    """

    def __init__(self, keep: int = 5000):
        self.keep = keep
        self.events: list[tuple[int, str]] = []
        self.last_id = 0
        self.cond = threading.Condition()

    def reset(self) -> None:
        with self.cond:
            self.events = []  # ids keep increasing, so a listener of the previous run sees nothing old
            self.cond.notify_all()

    def publish(self, payload: dict) -> None:
        with self.cond:
            self.last_id += 1
            self.events.append((self.last_id, json.dumps(payload)))
            del self.events[: -self.keep]
            self.cond.notify_all()

    def after(self, cursor: int, timeout: float) -> list[tuple[int, str]]:
        """Events with an id above `cursor`, waiting up to `timeout` seconds for one."""
        with self.cond:
            self.cond.wait_for(lambda: self.events and self.events[-1][0] > cursor, timeout=timeout)
            return [e for e in self.events if e[0] > cursor]


_events = _EventLog()
_experiment_thread: Optional[threading.Thread] = None


def create_app() -> Flask:
    app = Flask(
        __name__,
        template_folder='templates',
        static_folder='static',
    )
    app.secret_key = 'trans-bench-ui-secret-2025'

    from engine.loader import DescriptorLoader

    # versions are detected once, in the background (ui/data.py VersionCache), not per request
    loader = DescriptorLoader(base_dir=BASE_DIR, detect_versions=False)
    versions = uidata.VersionCache()

    # ── Helpers ─────────────────────────────────────────────────────────────

    def _get_systems():
        systems = loader.load_systems()
        try:
            versions.start([s.name for s in systems])
            for s in systems:
                s.version = versions.get(s.name) or 'detecting…'
        except Exception:  # descriptors without a name (tests use mocks)
            pass
        return systems

    def _system_ui(systems):
        """name -> {label, color, marker} for identity marks (the paper's plot style)."""
        styles = _plot_styles()
        return {s.name: styles.get(s.name, {'label': s.display_name, 'color': None, 'pointStyle': 'circle'})
                for s in systems if isinstance(getattr(s, 'name', None), str)}

    @app.context_processor
    def inject_globals():
        with _experiment_lock:
            running = _experiment_state['running']
        static = Path(app.static_folder)
        version = max(int((static / f).stat().st_mtime) for f in ('css/app.css', 'js/app.js'))
        return {'experiment_running': running, 'asset_version': version}

    def _get_graph_types():
        return loader.load_graph_types()

    def _get_config():
        return loader.load_global_config()

    def _plot_styles():
        """Per-system colour and point shape for the charts (engine/plot_style.py), as Chart.js options."""
        from matplotlib.colors import to_hex

        from engine.plot_style import SYSTEM_STYLE

        # matplotlib marker -> (Chart.js pointStyle, rotation)
        shapes = {'x': ('crossRot', 0), 's': ('rect', 0), 'v': ('triangle', 180), '^': ('triangle', 0),
                  '<': ('triangle', 270), '>': ('triangle', 90), 'D': ('rectRot', 0), 'o': ('circle', 0),
                  'P': ('cross', 0), '*': ('star', 0), 'p': ('rectRounded', 0), 'h': ('dash', 0)}
        # black (XSB) is drawn in the theme's ink colour, which works on light and dark backgrounds
        return {name: {'label': label, 'color': None if to_hex(color) == '#000000' else to_hex(color),
                       'pointStyle': shapes[marker][0], 'rotation': shapes[marker][1], 'dashed': linestyle != '-',
                       'marker': marker}
                for name, (label, color, marker, linestyle) in SYSTEM_STYLE.items()}

    def _transitive_modes():
        """The standard modes plus system-specific ones (e.g. DuckDB's doublerecurring_recursion)."""
        modes = ['right_recursion', 'left_recursion', 'double_recursion']
        for s in _get_systems():
            modes += [m for m in s.modes if m not in modes]
        return modes

    # ── Dashboard ────────────────────────────────────────────────────────────

    @app.route('/')
    def dashboard():
        systems = _get_systems()
        graph_types = _get_graph_types()

        # Collect result stats
        timing_dir = BASE_DIR / 'timing'
        csv_count = len(list(timing_dir.glob('**/*.csv'))) if timing_dir.exists() else 0
        system_dirs = [d.name for d in timing_dir.iterdir() if d.is_dir()] if timing_dir.exists() else []

        import os
        import platform

        machine_info = {
            'os': f"{platform.system()} {platform.release()}",
            'arch': platform.machine(),
            'cpu_count': os.cpu_count(),
            'python_version': platform.python_version(),
        }

        campaigns = [uidata.campaign_info(d) for d in uidata.campaign_dirs(BASE_DIR)]
        return render_template(
            'dashboard.html',
            systems=systems,
            graph_types=graph_types,
            csv_count=csv_count,
            benchmarked_systems=system_dirs,
            machine_info=machine_info,
            campaigns=campaigns,
            system_ui=_system_ui(systems),
            previews={g.name: uidata.graph_preview(str(BASE_DIR), g.name) for g in graph_types[:6]},
        )

    def _get_math_info(name: str):
        info = {
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
                'definition': r'\{(j, j+1) \mid i \in 1..n, j \in (i-1)n+1..in-1\} \cup \{(j, j+n) \mid i \in 1..n-1, j \in (i-1)n+1..in\}',
            },
            'binary_tree': {
                'symbol': 'B_h',
                'definition': r'\{(i, 2i) \mid i \in 1..2^{h-1}\} \cup \{(i, 2i+1) \mid i \in 1..2^{h-1}\}',
            },
            'reverse_binary_tree': {
                'symbol': 'V_h',
                'definition': r'\{(2i, i) \mid i \in 1..2^{h-1}\} \cup \{(2i+1, i) \mid i \in 1..2^{h-1}\}',
            },
            'x': {
                'symbol': 'X_{n,k}',
                'definition': r'\{(i, n+1) \mid i \in 1..n\} \cup \{(n+1, n+1+j) \mid j \in 1..k\}',
            },
            'y': {
                'symbol': 'Y_{n,k}',
                'definition': r'\{(i, n+1) \mid i \in 1..n\} \cup \{(i, i+1) \mid i \in n+1..n+k-1\}',
            },
            'w': {'symbol': 'W_{n,k}', 'definition': r'\{(i, n+1 + (i+j-1) \bmod n) \mid i \in 1..n, j \in 1..k\}'},
            'barabasi_albert': {
                'symbol': 'BA_{n,m}',
                'definition': r'\text{Scale-free network generated using preferential attachment with } m \text{ edges.}',
            },
            'scale_free': {'symbol': 'SF_n', 'definition': r'\text{Directed scale-free graph.}'},
            'star': {'symbol': 'S_n', 'definition': r'\{(i, 1) \mid i \in 2..n\}'},
        }
        return info.get(name, {'symbol': 'G_n', 'definition': r'\text{No formal definition available.}'})

    @app.route('/graphs/<name>')
    def graph_detail(name: str):
        graph_types = _get_graph_types()
        graph = next((g for g in graph_types if g.name == name), None)
        if not graph:
            return redirect(url_for('dashboard'))

        import sys

        if str(BASE_DIR) not in sys.path:
            sys.path.insert(0, str(BASE_DIR))

        try:
            import inspect

            from generate_db import DataGenerator

            gen_method_name = f'generate_{name}_graph'
            data_gen = DataGenerator()
            if hasattr(data_gen, gen_method_name):
                method = getattr(data_gen, gen_method_name)
                code_impl = inspect.getsource(method)
            else:
                code_impl = "# No Python implementation found."
        except Exception as e:
            code_impl = f"# Error loading implementation: {e}"

        math_info = _get_math_info(name)
        return render_template('graph_detail.html', graph=graph, code_impl=code_impl, math_info=math_info,
                               preview=uidata.graph_preview(str(BASE_DIR), name),
                               preview_range=uidata.preview_range(name))

    @app.route('/api/graphs/<name>/preview')
    def api_graph_preview(name: str):
        """A drawable instance of a topology for size parameter n (bounded, see ui/data.py preview_range)."""
        if name not in {g.name for g in _get_graph_types()}:
            return jsonify({'error': 'Unknown graph type'}), 404
        lo, default, hi = uidata.preview_range(name)
        try:
            n = min(hi, max(lo, int(request.args.get('n', default))))
        except ValueError:
            return jsonify({'error': 'n must be an integer'}), 400
        preview = uidata.graph_preview(str(BASE_DIR), name, n)
        if preview is None:
            return jsonify({'error': 'No preview for this size'}), 422
        return jsonify(preview)

    @app.route('/graphs')
    def graphs_list():
        graph_types = _get_graph_types()
        return render_template('graphs.html', graph_types=graph_types,
                               previews={g.name: uidata.graph_preview(str(BASE_DIR), g.name) for g in graph_types},
                               math={g.name: _get_math_info(g.name) for g in graph_types})

    # ── Systems ──────────────────────────────────────────────────────────────

    @app.route('/systems')
    def systems_list():
        systems = _get_systems()
        return render_template('systems.html', systems=systems, system_ui=_system_ui(systems))

    @app.route('/systems/<name>')
    def system_detail(name: str):
        system = loader.get_system(name)
        if system is None:
            return redirect(url_for('systems_list'))

        # Read raw descriptor YAML for editing
        with open(system.descriptor_path) as f:
            descriptor_yaml = f.read()

        # Check if credentials file exists
        cred_path = system.system_dir / 'credentials.yaml'
        cred_yaml = cred_path.read_text() if cred_path.exists() else ''

        # Gather existing rule files
        rule_files = sorted(system.rules_dir.glob(f'*{system.rule_extension}')) if system.rules_dir.exists() else []

        system.version = versions.get(system.name) or 'detecting…'
        return render_template(
            'system_detail.html',
            system=system,
            descriptor_yaml=descriptor_yaml,
            cred_yaml=cred_yaml,
            rule_files=[r.name for r in rule_files],
            system_ui=_system_ui([system]),
            has_example=(system.system_dir / 'credentials.example.yaml').exists(),
        )

    @app.route('/systems/<name>/save', methods=['POST'])
    def save_descriptor(name: str):
        system = loader.get_system(name)
        if system is None:
            return jsonify({'ok': False, 'error': 'System not found'}), 404

        yaml_text = request.form.get('descriptor_yaml', '')
        try:
            data = yaml.safe_load(yaml_text)
            if not isinstance(data, dict):
                raise ValueError('Not a valid YAML mapping')
            with open(system.descriptor_path, 'w') as f:
                f.write(yaml_text)
            return jsonify({'ok': True})
        except Exception as e:
            return jsonify({'ok': False, 'error': str(e)}), 400

    @app.route('/systems/<name>/creds', methods=['POST'])
    def save_credentials(name: str):
        system = loader.get_system(name)
        if system is None:
            return jsonify({'ok': False, 'error': 'System not found'}), 404

        yaml_text = request.form.get('cred_yaml', '')
        try:
            data = yaml.safe_load(yaml_text) or {}
            loader.save_system_credentials(name, data)
            return jsonify({'ok': True})
        except Exception as e:
            return jsonify({'ok': False, 'error': str(e)}), 400

    @app.route('/systems/<name>/rules/<path:filename>', methods=['GET'])
    def get_rule_file(name: str, filename: str):
        system = loader.get_system(name)
        if system is None:
            return jsonify({'error': 'System not found'}), 404

        rule_path = system.rules_dir / filename
        if not rule_path.exists() or not rule_path.is_file():
            return jsonify({'error': 'Rule file not found'}), 404

        try:
            content = rule_path.read_text()
            return jsonify({'content': content})
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    @app.route('/systems/<name>/rules/<path:filename>', methods=['POST'])
    def save_rule_file(name: str, filename: str):
        system = loader.get_system(name)
        if system is None:
            return jsonify({'ok': False, 'error': 'System not found'}), 404

        rule_path = system.rules_dir / filename
        if not rule_path.exists() or not rule_path.is_file():
            return jsonify({'ok': False, 'error': 'Rule file not found'}), 404

        content = request.form.get('content', '')
        try:
            rule_path.write_text(content)
            return jsonify({'ok': True})
        except Exception as e:
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/systems/new', methods=['GET', 'POST'])
    def new_system():
        if request.method == 'POST':
            from engine.bootstrap import BootstrapManager

            template_name = request.form.get('template', 'descriptor_sql_database.yaml')
            new_name = request.form.get('name', '').strip().lower().replace(' ', '_')
            if not new_name:
                return jsonify({'ok': False, 'error': 'Name required'}), 400
            import re

            if not re.fullmatch(r'[a-z][a-z0-9_]*', new_name):
                return jsonify({'ok': False, 'error': 'Use lowercase letters, digits and underscores, starting with a letter'}), 400
            if (BASE_DIR / 'systems' / new_name).exists():
                return jsonify({'ok': False, 'error': f'A system named {new_name} already exists'}), 409

            try:
                bootstrap_mgr = BootstrapManager(BASE_DIR)
                # Use template from templates/ directory if it exists, otherwise fall back to system template
                templates_dir = BASE_DIR / 'templates'
                if templates_dir.exists() and (templates_dir / template_name).exists():
                    bootstrap_mgr.bootstrap_system(new_name, template_name)
                else:
                    # Fallback: copy from existing system
                    new_dir = BASE_DIR / 'systems' / new_name
                    rules_dir = new_dir / 'rules'
                    new_dir.mkdir(exist_ok=True)
                    rules_dir.mkdir(exist_ok=True)
                    template_desc = BASE_DIR / 'systems' / template_name / 'descriptor.yaml'
                    if template_desc.exists():
                        content = template_desc.read_text().replace(f'name: {template_name}', f'name: {new_name}')
                        content = content.replace(
                            f'display_name: {template_name.title()}',
                            f'display_name: {new_name.replace("_", " ").title()}',
                        )
                        (new_dir / 'descriptor.yaml').write_text(content)
                return jsonify({'ok': True, 'redirect': url_for('system_detail', name=new_name)})
            except Exception as e:
                log.error(f'System creation failed: {e}')
                return jsonify({'ok': False, 'error': str(e)}), 400

        # Get available templates
        from engine.bootstrap import BootstrapManager

        bootstrap_mgr = BootstrapManager(BASE_DIR)
        available_templates = bootstrap_mgr.list_templates()
        systems = _get_systems()

        return render_template(
            'new_system.html',
            systems=systems,
            available_templates=available_templates,
        )

    @app.route('/domains/new', methods=['GET', 'POST'])
    def new_domain():
        if request.method == 'POST':
            from engine.bootstrap import BootstrapManager

            domain_name = request.form.get('name', '').strip().lower().replace(' ', '_')
            template_name = request.form.get('template', 'domain_shortest_path.yaml')

            if not domain_name:
                return jsonify({'ok': False, 'error': 'Domain name required'}), 400

            try:
                bootstrap_mgr = BootstrapManager(BASE_DIR)
                bootstrap_mgr.bootstrap_domain(domain_name, template_name)
                return jsonify({'ok': True, 'redirect': url_for('dashboard')})
            except Exception as e:
                log.error(f'Domain creation failed: {e}')
                return jsonify({'ok': False, 'error': str(e)}), 400

        from engine.bootstrap import BootstrapManager

        bootstrap_mgr = BootstrapManager(BASE_DIR)
        templates = bootstrap_mgr.list_templates()
        info = {}
        for t in templates.get('domain_templates', []):
            try:
                info[t] = yaml.safe_load((bootstrap_mgr.templates_dir / t).read_text()) or {}
            except Exception:
                info[t] = {}
        return render_template(
            'new_domain.html',
            domain_templates=templates.get('domain_templates', []),
            template_info=info,
            domains=loader.load_domains(),
        )

    @app.route('/graphs/new', methods=['GET', 'POST'])
    def new_graph():
        if request.method == 'POST':
            from engine.bootstrap import BootstrapManager

            graph_name = request.form.get('name', '').strip().lower().replace(' ', '_')
            description = request.form.get('description', '')

            if not graph_name:
                return jsonify({'ok': False, 'error': 'Graph name required'}), 400

            try:
                bootstrap_mgr = BootstrapManager(BASE_DIR)
                # Generate a default generator path
                generator_path = f'engine.data_generator.DataGenerator.generate_{graph_name}'
                bootstrap_mgr.bootstrap_graph(graph_name, generator_path, description)
                return jsonify(
                    {'ok': True, 'message': 'Graph descriptor created. Next: implement the generator method.'}
                )
            except Exception as e:
                log.error(f'Graph creation failed: {e}')
                return jsonify({'ok': False, 'error': str(e)}), 400

        return render_template('new_graph.html')

    # ── Validation API ────────────────────────────────────────────────────

    @app.route('/api/validate/<system_name>')
    def api_validate(system_name: str):
        from engine.validation import RuleValidator

        try:
            validator = RuleValidator(BASE_DIR)
            is_valid = validator.validate_system(system_name)
            return jsonify({'valid': is_valid, 'system': system_name})
        except Exception as e:
            log.error(f'Validation error: {e}')
            return jsonify({'valid': False, 'error': str(e)}), 400

    @app.route('/api/validate-domain/<domain_name>')
    def api_validate_domain(domain_name: str):
        from engine.validation import RuleValidator

        systems_filter = request.args.getlist('systems') or None
        try:
            validator = RuleValidator(BASE_DIR)
            results = validator.validate_domain(domain_name, system_names=systems_filter)
            all_ok = all(r.get('ok', False) for r in results.values())
            return jsonify({'valid': all_ok, 'domain': domain_name, 'results': results})
        except Exception as e:
            log.error(f'Domain validation error: {e}')
            return jsonify({'valid': False, 'error': str(e)}), 400

    @app.route('/api/test-rule', methods=['POST'])
    def api_test_rule():
        from engine.validation import RuleValidator

        data = request.get_json() or {}
        rule_path = data.get('rule_path', '')
        system_name = data.get('system_name')
        if not rule_path:
            return jsonify({'ok': False, 'error': 'rule_path required'}), 400
        try:
            validator = RuleValidator(BASE_DIR)
            ok = validator.test_rule_file(Path(rule_path), system_name=system_name)
            return jsonify({'ok': ok})
        except Exception as e:
            return jsonify({'ok': False, 'error': str(e)}), 400

    @app.route('/api/domains')
    def api_domains():
        domains = loader.load_domains()
        return jsonify(
            [
                {
                    'name': d.name,
                    'display_name': d.display_name,
                    'modes': d.modes,
                    'description': d.description,
                }
                for d in domains
            ]
        )

    @app.route('/api/domains/<domain_name>/modes')
    def api_domain_modes(domain_name: str):
        domain = loader.get_domain(domain_name)
        if domain:
            return jsonify({'domain': domain_name, 'modes': domain.modes})
        # Fallback: standard transitive modes
        return jsonify({'domain': domain_name, 'modes': _transitive_modes()})

    # ── Experiment ───────────────────────────────────────────────────────────

    @app.route('/experiment/new')
    def experiment_new():
        systems = _get_systems()
        graph_types = _get_graph_types()
        config = _get_config()
        domains = loader.load_domains()
        # Build domain options: always include transitive as default
        domain_options = [
            {
                'name': 'transitive',
                'display_name': 'Transitive Closure (default)',
                'modes': _transitive_modes(),
            }
        ]
        for d in domains:
            if d.name not in ('transitive', 'transitive_closure'):
                domain_options.append({'name': d.name, 'display_name': d.display_name, 'modes': d.modes})
        preselect = {
            'systems': [x for x in request.args.get('systems', '').split(',') if x],
            'graphs': [x for x in request.args.get('graphs', '').split(',') if x],
        }
        return render_template(
            'experiment_new.html',
            systems=systems,
            graph_types=graph_types,
            config=config,
            domain_options=domain_options,
            system_ui=_system_ui(systems),
            preselect=preselect,
            previews={g.name: uidata.graph_preview(str(BASE_DIR), g.name) for g in graph_types},
        )

    @app.route('/experiment/start', methods=['POST'])
    def experiment_start():
        global _experiment_thread

        with _experiment_lock:
            if _experiment_state['running']:
                return jsonify({'ok': False, 'error': 'An experiment is already running'}), 409

        data = request.json or {}
        selected_systems = data.get('systems', [])
        selected_graphs = data.get('graphs', [])
        selected_modes = data.get('modes', ['right_recursion', 'left_recursion'])
        domain = data.get('domain', 'transitive')
        query_mode = data.get('query_mode', 'full_materialization')  # full_materialization or demand_driven
        sizes = data.get('sizes', [10, 101, 10])
        num_runs = int(data.get('num_runs', 3))
        souffle_dir = data.get('souffle_include_dir')

        cli_args = ["python transitive.py"]
        if domain != 'transitive':
            cli_args.append(f"--domain {domain}")
        if query_mode != 'full_materialization':
            cli_args.append(f"--query-mode {query_mode}")
        if selected_systems:
            cli_args.append(f"--systems {' '.join(selected_systems)}")
        if selected_graphs:
            cli_args.append(f"--graphs {' '.join(selected_graphs)}")
        if selected_modes:
            cli_args.append(f"--modes {' '.join(selected_modes)}")
        if sizes:
            cli_args.append(f"--sizes {' '.join(map(str, sizes))}")
        cli_args.append(f"--num-runs {num_runs}")
        if souffle_dir:
            cli_args.append(f"--souffle-include-dir {souffle_dir}")
        cli_command = " ".join(cli_args)

        from engine.loader import DescriptorLoader
        from engine.runner import ExperimentRunner

        loader2 = DescriptorLoader(base_dir=BASE_DIR)
        systems = loader2.load_systems(names=selected_systems if selected_systems else None)
        graph_types = loader2.load_graph_types(names=selected_graphs if selected_graphs else None)
        config = loader2.load_global_config()

        if not systems or not graph_types:
            return jsonify({'ok': False, 'error': 'No systems or graph types found'}), 400

        if souffle_dir:
            config['souffle_include_dir'] = souffle_dir
            try:
                import yaml

                cfg_path = BASE_DIR / 'config.yaml'
                if cfg_path.exists():
                    with open(cfg_path, 'r') as f:
                        cfg_data = yaml.safe_load(f) or {}
                    cfg_data['souffle_include_dir'] = souffle_dir
                    with open(cfg_path, 'w') as f:
                        yaml.dump(cfg_data, f)
            except Exception as e:
                log.warning(f"Could not save souffle_include_dir to config.yaml: {e}")

        # Reset state
        with _experiment_lock:
            _experiment_state['running'] = True
            _experiment_state['cli_command'] = cli_command
            _experiment_state['progress'] = {}
            _experiment_state['log_lines'] = []
            _experiment_state['error'] = None
            _experiment_state['started'] = time.time()
            _experiment_state['finished'] = None
            _experiment_state['stop_requested'] = False
        _events.reset()

        def progress_cb(evt: dict):
            evt_type = evt.get('type', 'progress')
            if evt_type == 'log':
                # Plain log line from subprocess or connector
                msg = evt.get('message', '')
                level = evt.get('level', 'info')
                with _experiment_lock:
                    _experiment_state['log_lines'].append(msg)
                    _experiment_state['log_lines'] = _experiment_state['log_lines'][-500:]
                    if level == 'error':
                        _experiment_state['error'] = msg
                _events.publish({'type': 'log', 'message': msg, 'level': level})
            else:
                # Progress update
                msg = (
                    f"[{evt['pct']}%] {evt['system']} / {evt['graph']} / "
                    f"size={evt['size']} / {evt['mode']} → {evt['status']}"
                )
                with _experiment_lock:
                    _experiment_state['progress'] = evt
                    _experiment_state['log_lines'].append(msg)
                    _experiment_state['log_lines'] = _experiment_state['log_lines'][-500:]
                _events.publish(evt)

        def run_in_thread():
            try:
                runner = ExperimentRunner(
                    config=config,
                    systems=systems,
                    graph_types=graph_types,
                    size_range=sizes,
                    num_runs=num_runs,
                    modes=selected_modes,
                    domain=domain,
                    query_mode=query_mode,
                    progress_cb=progress_cb,
                    should_stop=lambda: _experiment_state['stop_requested'],
                )
                runner.run()
            except Exception as e:
                log.error(f'Experiment thread error: {e}')
                with _experiment_lock:
                    _experiment_state['error'] = str(e)
            finally:
                with _experiment_lock:
                    _experiment_state['running'] = False
                    _experiment_state['finished'] = time.time()
                _events.publish({'status': '__done__'})

        _experiment_thread = threading.Thread(target=run_in_thread, daemon=True)
        _experiment_thread.start()

        return jsonify({'ok': True})

    @app.route('/experiment/stream')
    def experiment_stream():
        # resume after the last event this page saw (EventSource sends Last-Event-ID when it reconnects)
        try:
            cursor = int(request.headers.get('Last-Event-ID') or request.args.get('after') or 0)
        except ValueError:
            cursor = 0

        def generate(cursor=cursor):
            yield 'data: {"status": "connected"}\n\n'
            while True:
                batch = _events.after(cursor, timeout=15)
                if not batch:
                    yield 'data: {"status": "heartbeat"}\n\n'
                    continue
                for event_id, msg in batch:
                    cursor = event_id
                    yield f'id: {event_id}\ndata: {msg}\n\n'
                    if '"__done__"' in msg:
                        return

        return Response(
            stream_with_context(generate()),
            mimetype='text/event-stream',
            headers={
                'Cache-Control': 'no-cache',
                'X-Accel-Buffering': 'no',
            },
        )

    @app.route('/experiment/status')
    def experiment_status():
        with _experiment_lock:
            return jsonify(
                {
                    'running': _experiment_state['running'],
                    'cli_command': _experiment_state.get('cli_command', ''),
                    'progress': _experiment_state['progress'],
                    'recent_logs': _experiment_state['log_lines'][-50:],
                    'error': _experiment_state['error'],
                    'started': _experiment_state['started'],
                    'finished': _experiment_state['finished'],
                    'stop_requested': _experiment_state['stop_requested'],
                    'now': time.time(),
                }
            )

    @app.route('/experiment/stop', methods=['POST'])
    def experiment_stop():
        # the runner checks this between configurations; the one that is running finishes first
        # `running` stays true until the thread has ended, so no second experiment starts alongside this one
        with _experiment_lock:
            if not _experiment_state['running']:
                return jsonify({'ok': False, 'error': 'No experiment is running'}), 409
            _experiment_state['stop_requested'] = True
        return jsonify({'ok': True})

    @app.route('/experiment/live')
    def experiment_live():
        return render_template('experiment_live.html', plot_styles=_plot_styles())

    # ── Verified campaigns ───────────────────────────────────────────────────

    @app.route('/campaigns')
    def campaigns_list():
        campaigns = [uidata.campaign_info(d) for d in uidata.campaign_dirs(BASE_DIR)]
        return render_template('campaigns.html', campaigns=campaigns)

    @app.route('/campaigns/<name>')
    def campaign_detail(name: str):
        directory = uidata.campaign_dir(BASE_DIR, name)
        if directory is None:
            abort(404)
        rows = uidata.summary_rows(directory)
        linear_sizes, linear_modes = uidata.sizes_and_modes(rows, uidata.LINEAR_GRAPHS)
        large_sizes, large_modes = uidata.sizes_and_modes(rows, uidata.LARGE_GRAPHS)
        files = [f for f in ('README.md', 'versions.txt', 'pip_freeze.txt', 'code.patch') if (directory / f).exists()]
        tables = sorted(p.name for p in (directory / 'analysis').glob('table_*.tex'))
        failures = uidata.failure_rows(directory)
        present = {r['graph'] for r in rows}
        limits = {float(f['limit_s']) for f in failures if f.get('limit_s')}
        return render_template(
            'campaign_detail.html',
            info=uidata.campaign_info(directory),
            figures=uidata.campaign_figures(directory),
            failures=failures,
            linear_sizes=linear_sizes, linear_modes=linear_modes, large_sizes=large_sizes, large_modes=large_modes,
            files=files, tables=tables, plot_styles=_plot_styles(),
            graphs=[g for g in uidata.LINEAR_GRAPHS + uidata.LARGE_GRAPHS if g in present],
            large_graphs=[g for g in uidata.LARGE_GRAPHS if g in present],
            graph_labels={g.name: g.display_name for g in _get_graph_types()},
            limit_s=max(limits) if limits else None,
        )

    @app.route('/campaigns/<name>/file/<path:filename>')
    def campaign_file(name: str, filename: str):
        """Files of a campaign: its README/versions, and anything under analysis/ (figures, tables)."""
        directory = uidata.campaign_dir(BASE_DIR, name)
        if directory is None:
            abort(404)
        if filename in ('README.md', 'versions.txt', 'pip_freeze.txt', 'code.patch'):
            return send_from_directory(directory, filename, mimetype='text/plain')
        mimetype = 'text/plain' if filename.endswith(('.tex', '.csv', '.json')) else None
        return send_from_directory(directory / 'analysis', filename, mimetype=mimetype)

    @app.route('/api/campaigns/<name>/matrix')
    def api_campaign_matrix(name: str):
        directory = uidata.campaign_dir(BASE_DIR, name)
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
        if family == 'large':  # rows are sizes there; return one matrix per size for the chosen graph
            graph = request.args.get('graph') or uidata.LARGE_GRAPHS[0]
            out = {'family': 'large', 'graph': graph, 'mode': mode, 'metric': metric, 'sizes': sizes, 'rows': []}
            for size in sizes:
                m = uidata.matrix(rows, [graph], size, mode, metric)
                out['rows'].append({'n': size, 'cells': m['cells'].get(graph, {})})
            out['series'] = sorted({s for r in out['rows'] for s in r['cells']})
            vals = [c['value'] for r in out['rows'] for c in r['cells'].values() if c['value'] is not None and c['status'] == 'ok']
            out['min'], out['max'] = (min(vals), max(vals)) if vals else (None, None)
            return jsonify(out)
        m = uidata.matrix(rows, graphs, n, mode, metric)
        return jsonify({'family': 'linear', 'n': n, 'mode': mode, 'metric': metric, 'sizes': sizes, 'modes': modes,
                        'graphs': m['graphs'], 'series': m['series'], 'cells': m['cells'],
                        'min': m['min'], 'max': m['max']})

    @app.route('/api/campaigns/<name>/series')
    def api_campaign_series(name: str):
        """Points of every series for one (graph, mode): mean time or memory per n, or the failure."""
        directory = uidata.campaign_dir(BASE_DIR, name)
        if directory is None:
            return jsonify({'error': 'Unknown campaign'}), 404
        graph = request.args.get('graph', 'complete')
        mode = request.args.get('mode', 'left_recursion')
        metric = 'memory' if request.args.get('metric') == 'memory' else 'time'
        return jsonify({'graph': graph, 'mode': mode, 'metric': metric,
                        'series': uidata.series_points(uidata.summary_rows(directory), graph, mode, metric)})

    # ── Results ──────────────────────────────────────────────────────────────

    @app.route('/results')
    def results():
        timing_dir = BASE_DIR / 'timing'
        tree: dict = {}
        all_systems = set()
        all_graphs = set()
        all_modes = set()

        import re

        mode_pattern = re.compile(r'^(.*?)_graph_\d+\.csv$')

        if timing_dir.exists():
            for domain_dir in sorted(timing_dir.iterdir()):
                if not domain_dir.is_dir():
                    continue
                domain_name = domain_dir.name
                if domain_name not in tree:
                    tree[domain_name] = {}

                for system_dir in sorted(domain_dir.iterdir()):
                    if not system_dir.is_dir():
                        continue
                    system_name = system_dir.name
                    if system_name not in tree[domain_name]:
                        tree[domain_name][system_name] = {}
                    all_systems.add(system_name)

                    for graph_dir in sorted(system_dir.iterdir()):
                        if not graph_dir.is_dir():
                            continue
                        graph_name = graph_dir.name
                        all_graphs.add(graph_name)

                        csvs = sorted(graph_dir.glob('*.csv'))
                        for c in csvs:
                            match = mode_pattern.match(c.name)
                            if match:
                                all_modes.add(match.group(1))
                        tree[domain_name][system_name][graph_name] = [c.name for c in csvs]
        # Custom sort for all_systems
        preferred_order = ['xsb', 'clingo', 'souffle']
        sorted_systems = sorted(
            list(all_systems),
            key=lambda x: (preferred_order.index(x.lower()) if x.lower() in preferred_order else 999, x.lower()),
        )

        return render_template(
            'results.html',
            file_count=sum(len(f) for sys_ in tree.values() for g in sys_.values() for f in g.values()),
            tree=tree,
            all_domains=sorted(list(tree.keys())),
            all_systems=sorted_systems,
            all_graphs=sorted(list(all_graphs)),
            all_modes=sorted(list(all_modes)),
            plot_styles=_plot_styles(),
            categories={s.name: s.category for s in loader.load_systems()},
            graph_labels={g.name: g.display_name for g in _get_graph_types()},
        )

    @app.route('/results/data/<domain>/<system>/<graph>/<filename>')
    def result_detail(domain: str, system: str, graph: str, filename: str):
        csv_path = BASE_DIR / 'timing' / domain / system / graph / filename
        if not csv_path.exists():
            return jsonify({'error': 'Not found'}), 404
        rows = []
        with open(csv_path) as f:
            import csv as csvmod

            reader = csvmod.reader(f)
            headers = next(reader, [])

            # The CSV might have 8 headers, but the average row has 9 columns (starts with "Average").
            # We will artificially add a 'Step' header at the beginning to align the data.
            aligned_headers = ['Step'] + headers

            row_idx = 1
            for row in reader:
                if not row:
                    continue

                clean_row = {}
                if row[0] == 'Average':
                    # Average row has 9 columns: ['Average', val1, val2, ..., val8]
                    clean_row['Step'] = 'Average'
                    for i, h in enumerate(headers):
                        clean_row[h] = row[i + 1] if i + 1 < len(row) else ''
                else:
                    # Data row has 8 columns: [val1, val2, ..., val8]
                    clean_row['Step'] = f'Run {row_idx}'
                    for i, h in enumerate(headers):
                        clean_row[h] = row[i] if i < len(row) else ''
                    row_idx += 1

                rows.append(clean_row)

        return jsonify({'file': filename, 'columns': aligned_headers, 'rows': rows})

    # ── JSON API ─────────────────────────────────────────────────────────────

    @app.route('/api/systems')
    def api_systems():
        systems = _get_systems()
        return jsonify([s.to_dict() for s in systems])

    @app.route('/api/nav')
    def api_nav():
        """Names for the command palette (no version detection, no I/O beyond the descriptors)."""
        return jsonify({
            'systems': [{'name': s.name, 'label': s.display_name} for s in loader.load_systems()],
            'graphs': [{'name': g.name, 'label': g.display_name} for g in _get_graph_types()],
            'campaigns': [d.name for d in uidata.campaign_dirs(BASE_DIR)],
        })

    @app.route('/api/graph-types')
    def api_graph_types():
        gts = _get_graph_types()
        return jsonify([g.to_dict() for g in gts])

    @app.route('/api/compare/trends', methods=['POST'])
    def api_compare_trends():
        req = request.get_json()
        if not req:
            return jsonify({'error': 'Invalid request'}), 400

        graph_type = req.get('graph_type')
        modes = req.get('modes', [])
        if not modes and req.get('mode'):
            modes = [req.get('mode')]
        req_systems = req.get('systems', [])

        if not graph_type or not modes or not req_systems:
            return jsonify({'error': 'Missing required fields'}), 400

        import csv
        import re

        timing_dir = BASE_DIR / 'timing'

        # size -> mode -> system -> { phase: time }
        results_by_size = {}

        domain = req.get('domain', 'transitive')  # We need domain now!

        for sys_name in req_systems:
            sys_graph_dir = timing_dir / domain / sys_name / graph_type
            if not sys_graph_dir.exists():
                continue

            for mode in modes:
                pattern = re.compile(rf'^{re.escape(mode)}_graph_(\d+)\.csv$')
                for csv_file in sys_graph_dir.glob(f'{mode}_graph_*.csv'):
                    match = pattern.match(csv_file.name)
                    if not match:
                        continue

                    size = int(match.group(1))
                    if size not in results_by_size:
                        results_by_size[size] = {}
                    if mode not in results_by_size[size]:
                        results_by_size[size][mode] = {}

                    with open(csv_file, 'r') as f:
                        reader = csv.reader(f)
                        headers = next(reader, [])
                        for row in reader:
                            if not row:
                                continue
                            if row[0] == 'Average':
                                phase_data = {}
                                for i, h in enumerate(headers):
                                    val_str = row[i + 1] if i + 1 < len(row) else ''
                                    try:
                                        phase_data[h] = float(val_str)
                                    except ValueError:
                                        phase_data[h] = 0.0
                                results_by_size[size][mode][sys_name] = phase_data
                                break

        # Sort by size
        sorted_results = [{'size': size, 'modes': results_by_size[size]} for size in sorted(results_by_size.keys())]

        return jsonify(sorted_results)

    return app
