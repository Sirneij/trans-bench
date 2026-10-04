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


@pytest.mark.parametrize(
    'url,needle',
    [
        ('/overview', 'Every path'),
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
    ],
)
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
    from ui.experiments import _EventLog

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
    from ui.experiments import _events

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
    html = client.get('/overview').get_data(as_text=True)
    assert 'js/flow.js' in html and 'flow-host' in html
    if CAMPAIGNS:
        assert 'Fastest at the largest graphs' in html


def test_events_carry_time():
    from ui.experiments import _EventLog

    log = _EventLog()
    log.publish({'type': 'log', 'message': 'x'})
    event = json.loads(log.after(0, timeout=0)[0][1])
    assert isinstance(event['t'], float)


def test_status_has_plan_key(client):
    assert 'plan' in client.get('/experiment/status').get_json()


@needs_campaign
def test_results_explorer_reads_campaign_runs(client):
    tree = uidata.timing_tree(BASE_DIR)
    campaign = next(iter(tree))
    series = next(iter(tree[campaign]))
    graph = next(iter(tree[campaign][series]))
    mode, sizes = next(iter(tree[campaign][series][graph].items()))
    d = client.get(f'/api/results/{campaign}/{series}/{graph}/{mode}/{sizes[0]}').get_json()
    assert d['columns'][0] == 'Run' and d['columns'][-2:] == ['Wall s', 'Status']
    assert d['rows'] and all(set(r) == set(d['columns']) for r in d['rows'])
    trends = client.post(
        '/api/compare/trends', json={'campaign': campaign, 'graph_type': graph, 'modes': [mode], 'systems': [series]}
    ).get_json()
    assert trends == sorted(trends, key=lambda x: x['size'])
    assert client.get(f'/api/results/{campaign}/nope/{graph}/{mode}/1').status_code == 404
    assert client.post('/api/compare/trends', json={}).status_code == 400


@pytest.mark.parametrize(
    'payload, needle',
    [
        ({'campaign': '../escape'}, 'Campaign names'),
        ({'campaign': 'ok-name', 'systems': ['nope']}, 'unknown system'),
        ({'campaign': 'ok-name', 'systems': ['duckdb'], 'sizes': [50, 10, 10]}, 'at least one'),
    ],
)
def test_start_rejects_bad_campaigns(client, payload, needle):
    r = client.post('/experiment/start', json=payload)
    assert r.status_code == 400 and needle in r.get_json()['error']


def test_start_runs_the_campaign_engine(client, monkeypatch):
    """/experiment/start hands a CampaignSpec to engine/campaign.py; nothing is written here (Campaign is mocked)."""
    import engine.campaign as campaign_mod

    seen = {}

    class FakeCampaign:
        def __init__(self, spec, on_event=None, should_stop=None):
            seen['spec'] = spec
            self.on_event = on_event

        def prepare(self):
            return {
                'systems': ['duckdb'],
                'graphs': ['path'],
                'sizes': [10],
                'modes': ['left_recursion'],
                'system_modes': {'duckdb': ['left_recursion']},
            }

        def run(self):
            self.on_event({'type': 'log', 'message': 'fake run', 'level': 'info'})
            return {'ok': 1, 'failed': 0, 'skipped': 0, 'resumed': 0, 'stopped': True}

    monkeypatch.setattr(campaign_mod, 'Campaign', FakeCampaign)
    r = client.post(
        '/experiment/start',
        json={
            'campaign': 'ui-test',
            'systems': ['duckdb'],
            'graphs': ['path'],
            'modes': ['left_recursion'],
            'sizes': [10, 11, 1],
            'num_runs': 1,
            'timeout': 5,
        },
    )
    assert r.status_code == 200 and r.get_json()['campaign'] == 'ui-test'
    from ui.experiments import RUN

    RUN.thread.join(timeout=10)
    status = client.get('/experiment/status').get_json()
    assert status['campaign'] == 'ui-test' and 'benchmark.py' in status['cli_command'] and not status['running']
    spec = seen['spec']
    assert spec.sizes == [10] and spec.timeout == 5 and spec.campaign_dir.name == 'ui-test'
    assert not (BASE_DIR / 'results' / 'ui-test').exists()


@pytest.fixture(scope='module')
def public():
    """The app as deployed publicly: read-only."""
    app = create_app(read_only=True)
    app.config['TESTING'] = True
    return app.test_client()


@pytest.mark.parametrize(
    'method, url',
    [
        ('post', '/experiment/start'),
        ('post', '/experiment/stop'),
        ('post', '/systems/postgres/save'),
        ('post', '/systems/postgres/creds'),
        ('post', '/systems/postgres/rules/transitive_left_recursion.py'),
        ('post', '/systems/new'),
        ('post', '/domains/new'),
        ('post', '/graphs/new'),
        ('post', '/api/test-rule'),
        ('get', '/api/validate/postgres'),
        ('get', '/api/validate-domain/transitive'),
    ],
)
def test_read_only_refuses_writes_runs_and_server_checks(public, method, url):
    r = getattr(public, method)(url, json={})
    assert r.status_code == 403 and r.get_json()['read_only'] is True


def test_read_only_pages_show_the_banner_and_no_credentials(public):
    html = public.get('/systems/postgres').get_data(as_text=True)
    assert 'Read-only copy' in html and 'id="t-credentials"' not in html and 'cred-editor' not in html
    assert 'class="read-only"' in public.get('/experiment/new').get_data(as_text=True)
    assert public.get('/healthz').get_json() == {'ok': True, 'read_only': True}
    assert public.post('/api/compare/trends', json={}).status_code == 400  # a POST that only reads is allowed


def test_repository_links_point_to_the_branch(public):
    """Every link to GitHub opens the branch the published campaigns come from."""
    from ui.app import REPO_URL  # pylint: disable=import-outside-toplevel

    for url in ('/', '/overview'):
        html = public.get(url).get_data(as_text=True)
        assert f'href="{REPO_URL}"' in html
        assert 'href="https://github.com/Sirneij/trans-bench"' not in html
    assert REPO_URL.endswith('/tree/verified-rerun-2026')


def test_read_only_from_the_environment(monkeypatch):
    monkeypatch.setenv('RAILWAY_PROJECT_ID', 'a-project-id')
    assert create_app().config['READ_ONLY'] is True
    monkeypatch.delenv('RAILWAY_PROJECT_ID')
    monkeypatch.setenv('TRANS_BENCH_READ_ONLY', '0')
    assert create_app().config['READ_ONLY'] is False


@pytest.mark.parametrize('name', ['../descriptor.yaml', '..%2fdescriptor.yaml', '../../../config.yaml'])
def test_rule_files_stay_inside_the_rules_directory(client, name):
    assert client.get(f'/systems/postgres/rules/{name}').status_code == 404


def test_security_headers(client):
    r = client.get('/')
    assert r.headers['X-Content-Type-Options'] == 'nosniff' and r.headers['X-Frame-Options'] == 'SAMEORIGIN'


def test_landing_page(client):
    """The landing page stands alone (no app sidebar) and carries the data its animations need."""
    r = client.get('/')
    assert r.status_code == 200
    html = r.get_data(as_text=True)
    assert 'js/landing.js' in html and 'css/landing.css' in html and 'class="sidebar"' not in html
    assert 'window.LANDING' in html and 'id="lp-field"' in html
    if CAMPAIGNS:
        assert 'id="lp-race"' in html and 'Every answer is checked' in html


@needs_campaign
def test_landing_facts_follow_the_campaign():
    facts = uidata.landing(BASE_DIR)
    info = uidata.campaign_info(uidata.campaign_dir(BASE_DIR, facts['campaign']))
    assert facts['executed'] == info['executed'] and facts['incorrect'] == info['incorrect']
    for graph, race in facts['race'].items():
        values = [e['value'] for e in race['entries'] if e['value'] is not None]
        assert values == sorted(values), graph  # fastest first, failures last
        assert all(e['failure'] for e in race['entries'] if e['value'] is None)
