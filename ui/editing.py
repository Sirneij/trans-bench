"""
Serve the views that change the repository: descriptors, credentials, rule files, new components.

They also run the rule checks of engine/validation.py, which may connect to a server. The public
read-only deployment refuses all of them (create_app() in ui/app.py).
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Optional

import yaml
from flask import jsonify, render_template, request, url_for

from engine.bootstrap import BootstrapManager
from engine.loader import SystemDescriptor
from ui.site import site

log = logging.getLogger(__name__)
SYSTEM_NAME = re.compile(r'[a-z][a-z0-9_]*')


def _rule_file(system: SystemDescriptor, filename: str) -> Optional[Path]:
    """Return the rule file `filename` of a system, or None if it is not a file inside its rules directory."""
    rules_dir = system.rules_dir.resolve()
    path = (rules_dir / filename).resolve()
    if not path.is_relative_to(rules_dir) or not path.is_file():  # no ../ out of the rules directory
        return None
    return path


def save_descriptor(name: str):
    """Replace a system's descriptor.yaml with the posted text, if it is a YAML mapping."""
    system = site().system(name)
    if system is None:
        return jsonify({'ok': False, 'error': 'System not found'}), 404
    yaml_text = request.form.get('descriptor_yaml', '')
    try:
        if not isinstance(yaml.safe_load(yaml_text), dict):
            raise ValueError('Not a valid YAML mapping')
        system.descriptor_path.write_text(yaml_text, encoding='utf-8')
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 400


def save_credentials(name: str):
    """Write a system's credentials.yaml (gitignored) from the posted YAML."""
    if site().system(name) is None:
        return jsonify({'ok': False, 'error': 'System not found'}), 404
    try:
        site().loader.save_system_credentials(name, yaml.safe_load(request.form.get('cred_yaml', '')) or {})
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 400


def get_rule_file(name: str, filename: str):
    """Return the text of one rule file of a system."""
    system = site().system(name)
    if system is None:
        return jsonify({'error': 'System not found'}), 404
    path = _rule_file(system, filename)
    if path is None:
        return jsonify({'error': 'Rule file not found'}), 404
    return jsonify({'content': path.read_text(encoding='utf-8')})


def save_rule_file(name: str, filename: str):
    """Replace the text of one existing rule file of a system."""
    system = site().system(name)
    if system is None:
        return jsonify({'ok': False, 'error': 'System not found'}), 404
    path = _rule_file(system, filename)
    if path is None:
        return jsonify({'ok': False, 'error': 'Rule file not found'}), 404
    try:
        path.write_text(request.form.get('content', ''), encoding='utf-8')
        return jsonify({'ok': True})
    except OSError as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


def _create_system(new_name: str, template_name: str) -> None:
    """Create systems/<new_name>/ from a template file, or by copying an existing system's descriptor."""
    base = site().base_dir
    manager = BootstrapManager(base)
    if (base / 'templates' / template_name).exists():
        manager.bootstrap_system(new_name, template_name)
        return
    new_dir = base / 'systems' / new_name
    (new_dir / 'rules').mkdir(parents=True, exist_ok=True)
    template_desc = base / 'systems' / template_name / 'descriptor.yaml'
    if template_desc.exists():
        content = template_desc.read_text(encoding='utf-8').replace(f'name: {template_name}', f'name: {new_name}')
        content = content.replace(
            f'display_name: {template_name.title()}', f'display_name: {new_name.replace("_", " ").title()}'
        )
        (new_dir / 'descriptor.yaml').write_text(content, encoding='utf-8')


def new_system():
    """Show the new-system form (GET), or create the system from a template (POST)."""
    if request.method == 'GET':
        return render_template(
            'new_system.html',
            systems=site().systems(),
            available_templates=BootstrapManager(site().base_dir).list_templates(),
        )
    new_name = request.form.get('name', '').strip().lower().replace(' ', '_')
    if not new_name:
        return jsonify({'ok': False, 'error': 'Name required'}), 400
    if not SYSTEM_NAME.fullmatch(new_name):
        return (
            jsonify({'ok': False, 'error': 'Use lowercase letters, digits and underscores, starting with a letter'}),
            400,
        )
    if (site().base_dir / 'systems' / new_name).exists():
        return jsonify({'ok': False, 'error': f'A system named {new_name} already exists'}), 409
    try:
        _create_system(new_name, request.form.get('template', 'descriptor_sql_database.yaml'))
        return jsonify({'ok': True, 'redirect': url_for('system_detail', name=new_name)})
    except Exception as e:
        log.error(f'System creation failed: {e}')
        return jsonify({'ok': False, 'error': str(e)}), 400


def new_domain():
    """Show the new-domain form (GET), or create the domain descriptor from a template (POST)."""
    manager = BootstrapManager(site().base_dir)
    if request.method == 'POST':
        domain_name = request.form.get('name', '').strip().lower().replace(' ', '_')
        if not domain_name:
            return jsonify({'ok': False, 'error': 'Domain name required'}), 400
        try:
            manager.bootstrap_domain(domain_name, request.form.get('template', 'domain_shortest_path.yaml'))
            return jsonify({'ok': True, 'redirect': url_for('dashboard')})
        except Exception as e:
            log.error(f'Domain creation failed: {e}')
            return jsonify({'ok': False, 'error': str(e)}), 400
    templates = manager.list_templates().get('domain_templates', [])
    info = {}
    for t in templates:
        try:
            info[t] = yaml.safe_load((manager.templates_dir / t).read_text(encoding='utf-8')) or {}
        except Exception:
            info[t] = {}
    return render_template(
        'new_domain.html', domain_templates=templates, template_info=info, domains=site().loader.load_domains()
    )


def new_graph():
    """Show the new-graph form (GET), or create the graph type descriptor (POST)."""
    if request.method == 'GET':
        return render_template('new_graph.html')
    graph_name = request.form.get('name', '').strip().lower().replace(' ', '_')
    if not graph_name:
        return jsonify({'ok': False, 'error': 'Graph name required'}), 400
    try:
        BootstrapManager(site().base_dir).bootstrap_graph(
            graph_name,
            f'engine.data_generator.DataGenerator.generate_{graph_name}',
            request.form.get('description', ''),
        )
        return jsonify({'ok': True, 'message': 'Graph descriptor created. Next: implement the generator method.'})
    except Exception as e:
        log.error(f'Graph creation failed: {e}')
        return jsonify({'ok': False, 'error': str(e)}), 400


def api_validate(system_name: str):
    """Check every rule file of a system."""
    from engine.validation import RuleValidator

    try:
        return jsonify({'valid': RuleValidator(site().base_dir).validate_system(system_name), 'system': system_name})
    except Exception as e:
        log.error(f'Validation error: {e}')
        return jsonify({'valid': False, 'error': str(e)}), 400


def api_validate_domain(domain_name: str):
    """Check that the systems have a rule file for every mode of a domain."""
    from engine.validation import RuleValidator

    try:
        valid = RuleValidator(site().base_dir).validate_domain(
            domain_name, system_names=request.args.getlist('systems') or None
        )
        return jsonify({'valid': valid, 'domain': domain_name})
    except Exception as e:
        log.error(f'Domain validation error: {e}')
        return jsonify({'valid': False, 'error': str(e)}), 400


def api_test_rule():
    """Check one rule file (syntax, and a live EXPLAIN on the named system's server)."""
    from engine.validation import RuleValidator

    data = request.get_json(silent=True) or {}
    rule_path = data.get('rule_path', '')
    if not rule_path:
        return jsonify({'ok': False, 'error': 'rule_path required'}), 400
    try:
        ok = RuleValidator(site().base_dir).test_rule_file(Path(rule_path), system_name=data.get('system_name'))
        return jsonify({'ok': ok})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 400


# (rule, view, methods), registered by create_app() under the view's name as endpoint
ROUTES = [
    ('/systems/<name>/save', save_descriptor, ['POST']),
    ('/systems/<name>/creds', save_credentials, ['POST']),
    ('/systems/<name>/rules/<path:filename>', get_rule_file, ['GET']),
    ('/systems/<name>/rules/<path:filename>', save_rule_file, ['POST']),
    ('/systems/new', new_system, ['GET', 'POST']),
    ('/domains/new', new_domain, ['GET', 'POST']),
    ('/graphs/new', new_graph, ['GET', 'POST']),
    ('/api/validate/<system_name>', api_validate, ['GET']),
    ('/api/validate-domain/<domain_name>', api_validate_domain, ['GET']),
    ('/api/test-rule', api_test_rule, ['POST']),
]
# Views that run checks against servers; the read-only deployment refuses them like the POST views.
RUNS_CHECKS = {'api_validate', 'api_validate_domain', 'api_test_rule'}
