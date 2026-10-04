"""
Start, follow and stop a campaign from the Web UI.

The campaign runs in a background thread through engine/campaign.py, the engine of benchmark.py
and transitive.py, so a run started here is measured, checked and recorded exactly like one started
from a terminal. Its events go to an event log that any number of pages can follow
(/experiment/stream, server-sent events); /experiment/status gives the state at a glance.
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from typing import Any, Optional

from flask import Response, jsonify, render_template, request, stream_with_context

from ui.site import plot_styles, site

log = logging.getLogger(__name__)
CAMPAIGN_NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,63}')


class _EventLog:
    """
    Events of the current campaign, kept for every listener of /experiment/stream.

    Each event has an increasing id; a listener keeps its own position (the SSE Last-Event-ID), so
    any number of pages can follow a run, a page opened late replays it from the start, and a
    reconnect resumes where it stopped.
    """

    def __init__(self, keep: int = 5000):
        """Keep at most the last `keep` events."""
        self.keep = keep
        self.events: list[tuple[int, str]] = []
        self.last_id = 0
        self.cond = threading.Condition()

    def reset(self) -> None:
        """Forget the events of the previous run (ids keep increasing, so no listener sees them again)."""
        with self.cond:
            self.events = []
            self.cond.notify_all()

    def publish(self, payload: dict) -> None:
        """Add an event, stamped with the time, and wake every waiting listener."""
        payload = {**payload, 't': time.time()}
        with self.cond:
            self.last_id += 1
            self.events.append((self.last_id, json.dumps(payload)))
            del self.events[: -self.keep]
            self.cond.notify_all()

    def after(self, cursor: int, timeout: float) -> list[tuple[int, str]]:
        """Return the events with an id above `cursor`, waiting up to `timeout` seconds for one."""
        with self.cond:
            self.cond.wait_for(lambda: self.events and self.events[-1][0] > cursor, timeout=timeout)
            return [e for e in self.events if e[0] > cursor]


class _Run:
    """The one campaign this UI process can run at a time: its state, its thread and its events."""

    def __init__(self):
        """Start idle."""
        self.lock = threading.Lock()
        self.state: dict[str, Any] = {
            'running': False,
            'progress': {},
            'log_lines': [],
            'error': None,
            'started': None,
            'finished': None,
            'stop_requested': False,
        }
        self.thread: Optional[threading.Thread] = None

    def running(self) -> bool:
        """Return whether a campaign is running."""
        with self.lock:
            return bool(self.state['running'])

    def stop_requested(self) -> bool:
        """Return whether Stop was pressed (polled by the campaign engine)."""
        return bool(self.state['stop_requested'])

    def publish(self, event: dict) -> None:
        """Keep an engine event in the state (for /experiment/status) and the event log (for the stream)."""
        with self.lock:
            if event.get('type') == 'log':
                self.state['log_lines'] = (self.state['log_lines'] + [event['message']])[-500:]
            elif event.get('type') == 'progress':
                self.state['progress'] = event
        _events.publish(event)


_events = _EventLog()
RUN = _Run()


def _log(message: str, level: str = 'info') -> None:
    """Publish one log line of the UI itself (not of the engine)."""
    RUN.publish({'type': 'log', 'message': message, 'level': level})


def _spec_from_request(data: dict):
    """Build the CampaignSpec of a start request; raises ValueError for a bad request."""
    from engine.campaign import CampaignSpec

    name = str(data.get('campaign') or time.strftime('run-%Y-%m-%d-%H%M'))
    if not CAMPAIGN_NAME.fullmatch(name):
        raise ValueError('Campaign names use letters, digits, dot, dash and underscore')
    start, stop, step = (int(x) for x in data.get('sizes', [10, 101, 10]))
    s = site()
    return CampaignSpec(
        systems=data.get('systems') or [x.name for x in s.loader.load_systems()],
        graphs=data.get('graphs') or [g.name for g in s.graph_types()],
        modes=data.get('modes') or ['left_recursion', 'right_recursion'],
        sizes=list(range(start, stop, max(step, 1)))[:500],
        runs=int(data.get('num_runs', 5)),
        timeout=float(data.get('timeout', 600)),
        campaign_dir=s.base_dir / 'results' / name,
        domain=data.get('domain', 'transitive'),
        query_mode=data.get('query_mode', 'full_materialization'),
        config_file=s.base_dir / 'config.yaml',
        souffle_include_dir=data.get('souffle_include_dir') or None,
    )


def _run_campaign(campaign, spec) -> None:
    """Run the campaign, then analyze it; this is the body of the background thread."""
    from engine.campaign import analyze

    try:
        counts = campaign.run()
        if not counts['stopped']:
            _log('Analyzing the campaign (tables and figures)…')
            code = analyze(spec.campaign_dir, spec.runs, on_line=lambda line: _log(f'[analysis] {line}'))
            if code != 0:
                _log(f'The analysis ended with exit code {code}; the runs are recorded in runs.jsonl.', 'warn')
    except Exception as e:  # shown on the live page
        log.error(f'Experiment thread error: {e}')
        with RUN.lock:
            RUN.state['error'] = str(e)
    finally:
        with RUN.lock:
            RUN.state['running'] = False
            RUN.state['finished'] = time.time()
        _events.publish({'status': '__done__'})


def experiment_new():
    """Show the wizard: systems, topologies, settings, review."""
    s = site()
    systems = s.systems()
    graph_types = s.graph_types()
    domain_options = [
        {'name': 'transitive', 'display_name': 'Transitive Closure (default)', 'modes': s.transitive_modes()}
    ]
    domain_options += [
        {'name': d.name, 'display_name': d.display_name, 'modes': d.modes}
        for d in s.loader.load_domains()
        if d.name not in ('transitive', 'transitive_closure')
    ]
    return render_template(
        'experiment_new.html',
        today=time.strftime('%Y-%m-%d'),
        systems=systems,
        graph_types=graph_types,
        config=s.loader.load_global_config(),
        domain_options=domain_options,
        system_ui=s.system_ui(systems),
        preselect={
            'systems': [x for x in request.args.get('systems', '').split(',') if x],
            'graphs': [x for x in request.args.get('graphs', '').split(',') if x],
        },
        previews=s.previews(graph_types),
    )


def experiment_start():
    """Start a campaign in a background thread (engine/campaign.py, as benchmark.py runs it)."""
    from engine.campaign import Campaign, CampaignError

    if RUN.running():
        return jsonify({'ok': False, 'error': 'An experiment is already running'}), 409
    try:
        spec = _spec_from_request(request.get_json(silent=True) or {})
        campaign = Campaign(spec, on_event=RUN.publish, should_stop=RUN.stop_requested)
        plan = campaign.prepare()
    except (CampaignError, TypeError, ValueError) as e:
        return jsonify({'ok': False, 'error': str(e)}), 400
    with RUN.lock:
        RUN.state.update(
            running=True,
            campaign=spec.campaign_dir.name,
            cli_command=spec.command(),
            progress={},
            log_lines=[],
            error=None,
            started=time.time(),
            finished=None,
            stop_requested=False,
            plan=plan,
        )
    _events.reset()
    RUN.thread = threading.Thread(target=_run_campaign, args=(campaign, spec), daemon=True)
    RUN.thread.start()
    return jsonify({'ok': True, 'campaign': spec.campaign_dir.name})


def experiment_stream():
    """Stream the run's events (server-sent events), resuming after the last one the page saw."""
    try:
        # EventSource sends Last-Event-ID when it reconnects
        cursor = int(request.headers.get('Last-Event-ID') or request.args.get('after') or 0)
    except ValueError:
        cursor = 0

    def generate(cursor=cursor):
        """Yield the events as they arrive, with a heartbeat every 15 s, until the run ends."""
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
        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'},
    )


def experiment_status():
    """Return the state of the current or last run."""
    with RUN.lock:
        st = RUN.state
        return jsonify(
            {
                'running': st['running'],
                'cli_command': st.get('cli_command', ''),
                'campaign': st.get('campaign'),
                'progress': st['progress'],
                'recent_logs': st['log_lines'][-50:],
                'error': st['error'],
                'started': st['started'],
                'finished': st['finished'],
                'stop_requested': st['stop_requested'],
                'plan': st.get('plan'),
                'now': time.time(),
            }
        )


def experiment_stop():
    """
    Ask the campaign to stop: the running trial is killed (not recorded) and nothing new starts.

    `running` stays true until the thread has ended, so no second campaign starts alongside this one.
    """
    with RUN.lock:
        if not RUN.state['running']:
            return jsonify({'ok': False, 'error': 'No experiment is running'}), 409
        RUN.state['stop_requested'] = True
    return jsonify({'ok': True})


def experiment_live():
    """Show the live monitor of the current or last run."""
    return render_template('experiment_live.html', plot_styles=plot_styles())


# (rule, view, methods), registered by create_app() under the view's name as endpoint
ROUTES = [
    ('/experiment/new', experiment_new, ['GET']),
    ('/experiment/start', experiment_start, ['POST']),
    ('/experiment/stream', experiment_stream, ['GET']),
    ('/experiment/status', experiment_status, ['GET']),
    ('/experiment/stop', experiment_stop, ['POST']),
    ('/experiment/live', experiment_live, ['GET']),
]
