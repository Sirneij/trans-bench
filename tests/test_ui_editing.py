"""
tests/test_ui_editing.py

The views that change files (ui/editing.py), run against a temporary copy of the descriptors so
that nothing in the repository is written.
"""

import shutil

import pytest
import yaml

from ui.app import BASE_DIR, create_app
from ui.site import EXTENSION_KEY, Site


@pytest.fixture
def repo(tmp_path):
    """A copy of systems/postgres, the graph types, the domains and the templates."""
    shutil.copytree(
        BASE_DIR / 'systems' / 'postgres',
        tmp_path / 'systems' / 'postgres',
        ignore=shutil.ignore_patterns('credentials.yaml', '__pycache__'),
    )
    for name in ('graph_types', 'templates', 'domains'):
        if (BASE_DIR / name).is_dir():
            shutil.copytree(BASE_DIR / name, tmp_path / name)
    return tmp_path


@pytest.fixture
def client(repo):
    app = create_app(read_only=False)
    app.config['TESTING'] = True
    app.extensions[EXTENSION_KEY] = Site(repo)
    return app.test_client()


def test_save_descriptor_checks_yaml(client, repo):
    path = repo / 'systems' / 'postgres' / 'descriptor.yaml'
    original = path.read_text()
    assert client.post('/systems/postgres/save', data={'descriptor_yaml': '- not a mapping'}).status_code == 400
    assert path.read_text() == original
    changed = original.replace('display_name: PostgreSQL', 'display_name: PostgreSQL (edited)')
    assert client.post('/systems/postgres/save', data={'descriptor_yaml': changed}).get_json() == {'ok': True}
    assert path.read_text() == changed
    assert client.post('/systems/nope/save', data={'descriptor_yaml': 'a: 1'}).status_code == 404


def test_save_credentials(client, repo):
    r = client.post('/systems/postgres/creds', data={'cred_yaml': 'dbURL: postgresql://localhost/test\n'})
    assert r.get_json() == {'ok': True}
    saved = yaml.safe_load((repo / 'systems' / 'postgres' / 'credentials.yaml').read_text())
    assert saved == {'dbURL': 'postgresql://localhost/test'}


def test_rule_files_read_and_write_inside_rules_only(client, repo):
    name = 'transitive_left_recursion.py'
    content = client.get(f'/systems/postgres/rules/{name}').get_json()['content']
    assert 'WITH RECURSIVE' in content
    assert client.post(f'/systems/postgres/rules/{name}', data={'content': content + '\n# edited\n'}).get_json()['ok']
    assert (repo / 'systems' / 'postgres' / 'rules' / name).read_text().endswith('# edited\n')
    # a path out of the rules directory is refused for reading and for writing
    assert client.post('/systems/postgres/rules/../descriptor.yaml', data={'content': 'x'}).status_code == 404
    assert client.get('/systems/postgres/rules/missing.py').status_code == 404


def test_new_system_names(client, repo):
    assert client.get('/systems/new').status_code == 200
    assert client.post('/systems/new', data={'name': ''}).status_code == 400
    assert client.post('/systems/new', data={'name': '9bad'}).status_code == 400
    assert client.post('/systems/new', data={'name': 'postgres'}).status_code == 409
    r = client.post('/systems/new', data={'name': 'my_db', 'template': 'descriptor_sql_database.yaml'})
    assert r.get_json()['ok'] and (repo / 'systems' / 'my_db' / 'descriptor.yaml').exists()


def test_new_domain_and_graph(client, repo):
    assert client.get('/domains/new').status_code == 200
    assert client.post('/domains/new', data={'name': ''}).status_code == 400
    assert client.post('/domains/new', data={'name': 'my_domain'}).get_json()['ok']
    assert client.get('/graphs/new').status_code == 200
    assert client.post('/graphs/new', data={'name': ''}).status_code == 400
    assert client.post('/graphs/new', data={'name': 'my_graph', 'description': 'test'}).get_json()['ok']
    assert (repo / 'graph_types' / 'my_graph.yaml').exists()


def test_validation_views(client):
    assert client.get('/api/validate/postgres').get_json()['system'] == 'postgres'
    assert client.post('/api/test-rule', json={}).status_code == 400
    domain = client.get('/api/validate-domain/transitive').get_json()
    assert set(domain) >= {'valid', 'domain'} or 'error' in domain
