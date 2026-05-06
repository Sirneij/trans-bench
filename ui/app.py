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
GET  /api/systems            JSON: all system descriptors
GET  /api/graph-types        JSON: all graph type descriptors
"""

from __future__ import annotations

import json
import logging
import queue
import threading
from pathlib import Path
from typing import Any, Optional

import yaml
from flask import (
    Flask,
    Response,
    jsonify,
    redirect,
    render_template,
    request,
    stream_with_context,
    url_for,
)

BASE_DIR = Path(__file__).parent.parent
log = logging.getLogger(__name__)

# ── Shared experiment state ─────────────────────────────────────────────────
_experiment_lock = threading.Lock()
_experiment_state: dict[str, Any] = {
    'running': False,
    'progress': {},
    'log_lines': [],
    'error': None,
}
_progress_queue: queue.Queue = queue.Queue(maxsize=500)
_experiment_thread: Optional[threading.Thread] = None


def create_app() -> Flask:
    app = Flask(
        __name__,
        template_folder='templates',
        static_folder='static',
    )
    app.secret_key = 'trans-bench-ui-secret-2025'

    from engine.loader import DescriptorLoader

    loader = DescriptorLoader(base_dir=BASE_DIR)

    # ── Helpers ─────────────────────────────────────────────────────────────

    def _get_systems():
        return loader.load_systems()

    def _get_graph_types():
        return loader.load_graph_types()

    def _get_config():
        return loader.load_global_config()

    # ── Dashboard ────────────────────────────────────────────────────────────

    @app.route('/')
    def dashboard():
        systems = _get_systems()
        graph_types = _get_graph_types()

        # Collect result stats
        timing_dir = BASE_DIR / 'timing'
        csv_count = len(list(timing_dir.glob('**/*.csv'))) if timing_dir.exists() else 0
        system_dirs = [d.name for d in timing_dir.iterdir() if d.is_dir()] if timing_dir.exists() else []

        import platform
        import os

        machine_info = {
            'os': f"{platform.system()} {platform.release()}",
            'arch': platform.machine(),
            'cpu_count': os.cpu_count(),
            'python_version': platform.python_version()
        }

        return render_template(
            'dashboard.html',
            systems=systems,
            graph_types=graph_types,
            csv_count=csv_count,
            benchmarked_systems=system_dirs,
            experiment_running=_experiment_state['running'],
            machine_info=machine_info,
        )

    def _get_math_info(name: str):
        info = {
            'complete': {
                'symbol': 'K_n',
                'definition': r'\{(i,j) \mid i \in 1..n, j \in 1..n\}'
            },
            'max_acyclic': {
                'symbol': 'T_n',
                'definition': r'\{(i,j) \mid i \in 1..n-1, j \in i+1..n\}'
            },
            'cycle': {
                'symbol': 'C_n',
                'definition': r'\{(i,i+1) \mid i \in 1..n-1\} \cup \{(n,1)\}'
            },
            'cycle_with_shortcuts': {
                'symbol': 'S_{n,k}',
                'definition': r'\{(i, (i-1 + t \cdot n/(k+1)) \bmod n + 1) \mid i \in 1..n, t \in 1..k\} \cup C_n'
            },
            'path': {
                'symbol': 'P_n',
                'definition': r'\{(i,i+1) \mid i \in 1..n-1\}'
            },
            'multi_path': {
                'symbol': 'M_{n,k}',
                'definition': r'\{(i,i+k) \mid i \in 1..(n-1) \cdot k\}'
            },
            'grid': {
                'symbol': 'G_{n \times n}',
                'definition': r'\{(j, j+1) \mid i \in 1..n, j \in (i-1)n+1..in-1\} \cup \{(j, j+n) \mid i \in 1..n-1, j \in (i-1)n+1..in\}'
            },
            'binary_tree': {
                'symbol': 'B_h',
                'definition': r'\{(i, 2i) \mid i \in 1..2^{h-1}\} \cup \{(i, 2i+1) \mid i \in 1..2^{h-1}\}'
            },
            'reverse_binary_tree': {
                'symbol': 'V_h',
                'definition': r'\{(2i, i) \mid i \in 1..2^{h-1}\} \cup \{(2i+1, i) \mid i \in 1..2^{h-1}\}'
            },
            'x': {
                'symbol': 'X_{n,k}',
                'definition': r'\{(i, n+1) \mid i \in 1..n\} \cup \{(n+1, n+1+j) \mid j \in 1..k\}'
            },
            'y': {
                'symbol': 'Y_{n,k}',
                'definition': r'\{(i, n+1) \mid i \in 1..n\} \cup \{(i, i+1) \mid i \in n+1..n+k-1\}'
            },
            'w': {
                'symbol': 'W_{n,k}',
                'definition': r'\{(i, n+1 + (i+j-1) \bmod n) \mid i \in 1..n, j \in 1..k\}'
            },
            'barabasi_albert': {
                'symbol': 'BA_{n,m}',
                'definition': r'\text{Scale-free network generated using preferential attachment with } m \text{ edges.}'
            },
            'scale_free': {
                'symbol': 'SF_n',
                'definition': r'\text{Directed scale-free graph.}'
            },
            'star': {
                'symbol': 'S_n',
                'definition': r'\{(i, 1) \mid i \in 2..n\}'
            }
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
            from generate_db import DataGenerator
            import inspect
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
        return render_template('graph_detail.html', graph=graph, code_impl=code_impl, math_info=math_info)

    # ── Systems ──────────────────────────────────────────────────────────────

    @app.route('/systems')
    def systems_list():
        systems = _get_systems()
        return render_template('systems.html', systems=systems)

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

        return render_template(
            'system_detail.html',
            system=system,
            descriptor_yaml=descriptor_yaml,
            cred_yaml=cred_yaml,
            rule_files=[r.name for r in rule_files],
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
            template_name = request.form.get('template', 'postgres')
            new_name = request.form.get('name', '').strip().lower().replace(' ', '_')
            if not new_name:
                return jsonify({'ok': False, 'error': 'Name required'}), 400

            new_dir = BASE_DIR / 'systems' / new_name
            rules_dir = new_dir / 'rules'
            new_dir.mkdir(exist_ok=True)
            rules_dir.mkdir(exist_ok=True)

            # Copy template descriptor
            template_desc = BASE_DIR / 'systems' / template_name / 'descriptor.yaml'
            if template_desc.exists():
                content = template_desc.read_text().replace(f'name: {template_name}', f'name: {new_name}')
                content = content.replace(
                    f'display_name: {template_name.title()}', f'display_name: {new_name.replace("_", " ").title()}'
                )
                (new_dir / 'descriptor.yaml').write_text(content)

            return jsonify({'ok': True, 'redirect': url_for('system_detail', name=new_name)})

        systems = _get_systems()
        return render_template('new_system.html', systems=systems)

    # ── Experiment ───────────────────────────────────────────────────────────

    @app.route('/experiment/new')
    def experiment_new():
        systems = _get_systems()
        graph_types = _get_graph_types()
        config = _get_config()
        return render_template(
            'experiment_new.html',
            systems=systems,
            graph_types=graph_types,
            config=config,
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
        sizes = data.get('sizes', [10, 101, 10])
        num_runs = int(data.get('num_runs', 3))
        souffle_dir = data.get('souffle_include_dir')
        
        cli_args = ["python transitive.py"]
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
        while not _progress_queue.empty():
            _progress_queue.get_nowait()

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
                try:
                    _progress_queue.put_nowait(json.dumps({'type': 'log', 'message': msg, 'level': level}))
                except queue.Full:
                    pass
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
                try:
                    _progress_queue.put_nowait(json.dumps(evt))
                except queue.Full:
                    pass

        def run_in_thread():
            try:
                runner = ExperimentRunner(
                    config=config,
                    systems=systems,
                    graph_types=graph_types,
                    size_range=sizes,
                    num_runs=num_runs,
                    modes=selected_modes,
                    progress_cb=progress_cb,
                )
                runner.run()
            except Exception as e:
                log.error(f'Experiment thread error: {e}')
                with _experiment_lock:
                    _experiment_state['error'] = str(e)
            finally:
                with _experiment_lock:
                    _experiment_state['running'] = False
                try:
                    _progress_queue.put_nowait(json.dumps({'status': '__done__'}))
                except queue.Full:
                    pass

        _experiment_thread = threading.Thread(target=run_in_thread, daemon=True)
        _experiment_thread.start()

        return jsonify({'ok': True})

    @app.route('/experiment/stream')
    def experiment_stream():
        def generate():
            yield 'data: {"status": "connected"}\n\n'
            while True:
                try:
                    msg = _progress_queue.get(timeout=30)
                    yield f'data: {msg}\n\n'
                    if json.loads(msg).get('status') == '__done__':
                        break
                except queue.Empty:
                    yield 'data: {"status": "heartbeat"}\n\n'

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
                }
            )

    @app.route('/experiment/stop', methods=['POST'])
    def experiment_stop():
        # Signal is best-effort; thread will finish current step
        with _experiment_lock:
            _experiment_state['running'] = False
        return jsonify({'ok': True})

    @app.route('/experiment/live')
    def experiment_live():
        return render_template('experiment_live.html')

    # ── Results ──────────────────────────────────────────────────────────────

    @app.route('/results')
    def results():
        timing_dir = BASE_DIR / 'timing'
        tree: dict = {}
        all_systems = set()
        all_graphs = set()
        all_modes = set()
        
        import re
        mode_pattern = re.compile(r'^timing_(.*)_graph_\d+\.csv$')

        if timing_dir.exists():
            for system_dir in sorted(timing_dir.iterdir()):
                if not system_dir.is_dir():
                    continue
                tree[system_dir.name] = {}
                all_systems.add(system_dir.name)
                for graph_dir in sorted(system_dir.iterdir()):
                    if not graph_dir.is_dir():
                        continue
                    all_graphs.add(graph_dir.name)
                    csvs = sorted(graph_dir.glob('*.csv'))
                    for c in csvs:
                        match = mode_pattern.match(c.name)
                        if match:
                            all_modes.add(match.group(1))
                    tree[system_dir.name][graph_dir.name] = [c.name for c in csvs]
        return render_template(
            'results.html', 
            tree=tree, 
            all_systems=sorted(list(all_systems)), 
            all_graphs=sorted(list(all_graphs)), 
            all_modes=sorted(list(all_modes))
        )

    @app.route('/results/data/<system>/<graph>/<filename>')
    def result_detail(system: str, graph: str, filename: str):
        csv_path = BASE_DIR / 'timing' / system / graph / filename
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
                        clean_row[h] = row[i+1] if i+1 < len(row) else ''
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
            
        import re
        import csv
        timing_dir = BASE_DIR / 'timing'
        
        # size -> mode -> system -> { phase: time }
        results_by_size = {}
        
        for sys_name in req_systems:
            sys_graph_dir = timing_dir / sys_name / graph_type
            if not sys_graph_dir.exists():
                continue
                
            for mode in modes:
                pattern = re.compile(rf'^timing_{mode}_graph_(\d+)\.csv$')
                for csv_file in sys_graph_dir.glob(f'timing_{mode}_graph_*.csv'):
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
                            if not row: continue
                            if row[0] == 'Average':
                                phase_data = {}
                                for i, h in enumerate(headers):
                                    val_str = row[i+1] if i+1 < len(row) else ''
                                    try:
                                        phase_data[h] = float(val_str)
                                    except ValueError:
                                        phase_data[h] = 0.0
                                results_by_size[size][mode][sys_name] = phase_data
                                break
                            
        # Sort by size
        sorted_results = [
            {'size': size, 'modes': results_by_size[size]}
            for size in sorted(results_by_size.keys())
        ]
        
        return jsonify(sorted_results)

    return app
