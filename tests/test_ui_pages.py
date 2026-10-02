"""
tests/test_ui_pages.py

Every page of the web UI renders against the real repository (systems/, graph_types/, results/), and
the JSON endpoints behind the interactive views (campaign matrix and series, topology previews, the
command palette) return what the pages expect. Nothing here writes to the repository.
"""

import json

import pytest

from ui import data as uidata
from ui.app import BASE_DIR, create_app

CAMPAIGNS = [d.name for d in uidata.campaign_dirs(BASE_DIR)]
needs_campaign = pytest.mark.skipif(not CAMPAIGNS, reason='no verified campaign under results/')


@pytest.fixture(scope='module')
def client():
    app = create_app()
    app.config['TESTING'] = True
    return app.test_client()


@pytest.mark.parametrize('url,needle', [
    ('/', 'Every path'),
    ('/systems', 'Register a system'),
    ('/systems/postgres', 'Timing phases'),
    ('/systems/new', 'What gets created'),
    ('/graphs', 'Topologies'),
    ('/graphs/cycle', 'Closure lab'),
    ('/graphs/new', 'implement the generator'),
    ('/domains/new', 'Existing domains'),
    ('/experiment/new', 'Which systems?'),
    ('/experiment/new?systems=postgres&graphs=cycle', 'Which topologies?'),
    ('/experiment/live', 'Live monitor'),
    ('/campaigns', 'Campaigns'),
    ('/results', 'Results explorer'),
])
def test_page_renders(client, url, needle):
    r = client.get(url)
    assert r.status_code == 200, url
    html = r.get_data(as_text=True)
    assert needle in html
    assert 'js/app.js' in html and 'css/app.css' in html


def test_pages_have_no_native_dialogs():
    """alert()/confirm() are replaced by in-page dialogs and toasts (TB.confirm, TB.toast)."""
    for path in (BASE_DIR / 'ui' / 'templates').glob('*.html'):
        text = path.read_text()
        assert 'alert(' not in text and ' confirm(' not in text, path.name


def test_preselected_systems_are_checked(client):
    html = client.get('/experiment/new?systems=postgres').get_data(as_text=True)
    assert 'value="postgres" checked' in html


def test_unknown_system_redirects(client):
    assert client.get('/systems/no_such_system').status_code == 302


@needs_campaign
@pytest.mark.parametrize('name', CAMPAIGNS)
def test_campaign_page(client, name):
    r = client.get(f'/campaigns/{name}')
    assert r.status_code == 200
    html = r.get_data(as_text=True)
    assert 'Outcome by series' in html and name in html


def test_unknown_campaign_404(client):
    assert client.get('/campaigns/no_such_campaign').status_code == 404
    assert client.get('/api/campaigns/no_such_campaign/matrix').status_code == 404


@needs_campaign
def test_campaign_files(client):
    name = CAMPAIGNS[0]
    r = client.get(f'/campaigns/{name}/file/README.md')
    assert r.status_code == 200 and r.mimetype == 'text/plain'
    assert client.get(f'/campaigns/{name}/file/summary.csv').status_code == 200
    assert client.get(f'/campaigns/{name}/file/../../config.yaml').status_code == 404


@needs_campaign
def test_campaign_matrix_api(client):
    name = CAMPAIGNS[0]
    d = client.get(f'/api/campaigns/{name}/matrix?family=linear&mode=left_recursion').get_json()
    assert d['family'] == 'linear' and d['n'] == d['sizes'][-1]
    assert d['graphs'] and d['series']
    cell = next(iter(d['cells'][d['graphs'][0]].values()))
    assert {'status', 'failure', 'value', 'runs', 'mode'} <= set(cell)
    assert d['min'] <= d['max']
    large = client.get(f'/api/campaigns/{name}/matrix?family=large&graph=scale_free').get_json()
    assert large['family'] == 'large' and [r['n'] for r in large['rows']] == large['sizes']
    assert client.get(f'/api/campaigns/{name}/matrix?n=abc').status_code == 400


@needs_campaign
def test_campaign_series_api(client):
    d = client.get(f'/api/campaigns/{CAMPAIGNS[0]}/series?graph=cycle&mode=left_recursion').get_json()
    assert d['series']
    for points in d['series'].values():
        ns = [p['n'] for p in points]
        assert ns == sorted(ns)


def test_nav_api(client):
    d = client.get('/api/nav').get_json()
    assert {'postgres', 'xsb'} <= {s['name'] for s in d['systems']}
    assert 'cycle' in {g['name'] for g in d['graphs']}
    assert d['campaigns'] == CAMPAIGNS


def test_graph_preview_api(client):
    d = client.get('/api/graphs/path/preview?n=5').get_json()
    assert d['node_count'] == 5 and d['edge_count'] == 4
    assert d['closure_count'] == 10  # all pairs i < j of a path of 5 nodes
    lo, _, hi = uidata.preview_range('path')
    assert client.get('/api/graphs/path/preview?n=100000').get_json()['n'] == hi  # clamped
    assert client.get('/api/graphs/path/preview?n=0').get_json()['n'] == lo
    assert client.get('/api/graphs/no_such_graph/preview').status_code == 404
    assert client.get('/api/graphs/path/preview?n=x').status_code == 400


def test_graph_preview_closure_of_cycle():
    p = uidata.graph_preview(str(BASE_DIR), 'cycle', 6)
    # a cycle reaches every node, itself included: |TC| = n^2; drawn pairs exclude the edges and self pairs
    assert p['closure_count'] == 36
    assert len(p['closure']) == 36 - 6 - 6


def test_experiment_status_reports_times(client):
    d = json.loads(client.get('/experiment/status').data)
    assert {'running', 'started', 'finished', 'stop_requested', 'now'} <= set(d)


def test_new_system_rejects_bad_and_existing_names(client):
    r = client.post('/systems/new', data={'name': '9bad name!', 'template': 'postgres'})
    assert r.status_code == 400
    r = client.post('/systems/new', data={'name': 'postgres', 'template': 'duckdb'})
    assert r.status_code == 409  # never overwrite an existing system


def test_campaign_intro_is_plain_text():
    for d in uidata.campaign_dirs(BASE_DIR):
        intro = uidata.campaign_info(d)['intro']
        assert '](' not in intro and '*' not in intro and '`' not in intro


def test_event_log_replays_and_resumes():
    from ui.app import _EventLog

    log = _EventLog(keep=3)
    for i in range(4):
        log.publish({'i': i})
    assert [json.loads(m)['i'] for _, m in log.after(0, timeout=0)] == [1, 2, 3]  # oldest dropped
    assert [i for i, _ in log.after(3, timeout=0)] == [4]
    assert log.after(4, timeout=0) == []
    log.reset()
    log.publish({'i': 'new run'})
    assert log.after(4, timeout=0)[0][0] == 5  # ids keep increasing across runs


def test_stream_replays_the_run_to_every_listener(client):
    from ui.app import _events

    _events.reset()
    _events.publish({'type': 'log', 'message': 'hello', 'level': 'info'})
    _events.publish({'status': '__done__'})
    for _ in range(2):  # a second page gets the same events
        body = client.get('/experiment/stream').get_data(as_text=True)
        assert '"hello"' in body and '__done__' in body and 'id: ' in body
    last = _events.last_id
    resumed = client.get('/experiment/stream', headers={'Last-Event-ID': str(last - 1)}).get_data(as_text=True)
    assert '"hello"' not in resumed and '__done__' in resumed
    _events.reset()


def test_stop_without_a_run(client):
    assert client.post('/experiment/stop').status_code == 409


@needs_campaign
def test_campaign_page_has_race(client):
    html = client.get(f'/campaigns/{CAMPAIGNS[0]}').get_data(as_text=True)
    assert 'id="p-race"' in html and 'view-transition-name: camp-' in html


@needs_campaign
def test_leaderboard():
    directory = uidata.campaign_dirs(BASE_DIR)[0]
    board = uidata.leaderboard(uidata.summary_rows(directory))
    assert board
    # one winner per contest: wins add up to the number of contests that someone finished
    contests = len(uidata.LINEAR_GRAPHS) * 2
    assert sum(x['wins'] for x in board) <= contests
    assert board == sorted(board, key=lambda x: (-x['wins'], -x['podiums'], x['mean_rank'] or 99))
    for x in board:
        assert x['completed'] <= x['contests'] and all(p['value'] > 0 for p in x['trend'])


def test_dashboard_shows_leaderboard_and_flow(client):
    html = client.get('/').get_data(as_text=True)
    assert 'js/flow.js' in html and 'flow-host' in html
    if CAMPAIGNS:
        assert 'Fastest at the largest graphs' in html


def test_events_carry_time():
    from ui.app import _EventLog

    log = _EventLog()
    log.publish({'type': 'log', 'message': 'x'})
    event = json.loads(log.after(0, timeout=0)[0][1])
    assert isinstance(event['t'], float)


def test_status_has_plan_key(client):
    assert 'plan' in client.get('/experiment/status').get_json()
