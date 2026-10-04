"""
Create the Flask application of the trans-bench Web UI.

The views live in four modules, each with a ROUTES list that create_app() registers under the
view's own name (so templates call url_for('dashboard') and so on):

    ui/pages.py        dashboard, topologies, systems, descriptors as JSON
    ui/results.py      campaigns and the results explorer
    ui/experiments.py  the wizard, and starting, following and stopping a campaign
    ui/editing.py      everything that changes files or runs checks against a server

Read-only mode is for the public deployment (Dockerfile, railway.json). It refuses every view that
changes files, starts a campaign or contacts a database, hides credentials, and shows a banner. It
is on when TRANS_BENCH_READ_ONLY is 1/true, or when the app runs on Railway (RAILWAY_PROJECT_ID is set).
"""

from __future__ import annotations

import logging
import os
import secrets
from pathlib import Path
from typing import Optional

from flask import Flask, jsonify, request

from ui import editing, experiments, pages, results
from ui.experiments import RUN
from ui.site import BASE_DIR, EXTENSION_KEY, Site

log = logging.getLogger(__name__)
# the branch of the repository that this code and the published campaigns come from
REPO_BRANCH = 'verified-rerun-2026'
REPO_URL = f'https://github.com/Sirneij/trans-bench/tree/{REPO_BRANCH}'
READ_ONLY_MESSAGE = (
    'This public copy of trans-bench is read-only: it shows the published campaigns. '
    'Clone the repository to run benchmarks or to edit systems.'
)


def _env_flag(name: str) -> bool:
    """Return whether an environment variable is set to a true value (1, true, yes, on)."""
    return os.environ.get(name, '').strip().lower() in ('1', 'true', 'yes', 'on')


def _refused(endpoint: Optional[str], method: str) -> bool:
    """Return whether read-only mode refuses a request to `endpoint`."""
    if endpoint in editing.RUNS_CHECKS:
        return True
    return method not in ('GET', 'HEAD', 'OPTIONS') and endpoint not in results.READ_ONLY_POSTS


def create_app(read_only: Optional[bool] = None) -> Flask:
    """Build the app; `read_only` defaults to the environment (see the module docstring)."""
    app = Flask(__name__, template_folder='templates', static_folder='static')
    app.secret_key = os.environ.get('SECRET_KEY') or secrets.token_hex(32)
    if read_only is None:
        read_only = _env_flag('TRANS_BENCH_READ_ONLY') or bool(os.environ.get('RAILWAY_PROJECT_ID'))
    app.config['READ_ONLY'] = read_only
    app.extensions[EXTENSION_KEY] = Site(BASE_DIR)

    for module in (pages, results, experiments, editing):
        for rule, view, methods in module.ROUTES:
            app.add_url_rule(rule, endpoint=view.__name__, view_func=view, methods=methods)

    @app.get('/healthz')
    def healthz():
        """Answer the platform's health check."""
        return jsonify({'ok': True, 'read_only': app.config['READ_ONLY']})

    @app.before_request
    def refuse_in_read_only_mode():
        """Refuse edits, runs and server checks in read-only mode, with a reason the pages can show."""
        if app.config['READ_ONLY'] and _refused(request.endpoint, request.method):
            return jsonify({'ok': False, 'error': READ_ONLY_MESSAGE, 'read_only': True}), 403
        return None

    @app.after_request
    def security_headers(response):
        """Add the headers that stop content sniffing and framing by other sites."""
        response.headers.setdefault('X-Content-Type-Options', 'nosniff')
        response.headers.setdefault('X-Frame-Options', 'SAMEORIGIN')
        response.headers.setdefault('Referrer-Policy', 'strict-origin-when-cross-origin')
        return response

    @app.context_processor
    def inject_globals():
        """Give every template the run state, the read-only flag and the asset version."""
        static = Path(app.static_folder or '')
        assets = [*static.glob('css/*.css'), *static.glob('js/*.js')]
        return {
            'experiment_running': RUN.running(),
            'read_only': app.config['READ_ONLY'],
            'read_only_message': READ_ONLY_MESSAGE,
            'repo_url': REPO_URL,
            'repo_branch': REPO_BRANCH,
            # changes when a stylesheet or script changes, so browsers fetch the new one
            'asset_version': max((int(f.stat().st_mtime) for f in assets), default=0),
        }

    return app
