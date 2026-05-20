import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import transitive


def test_load_legacy_config(tmp_path):
    conf = tmp_path / "config.json"
    assert transitive.load_legacy_config(conf) == {}

    conf.write_text(json.dumps({"test": "value"}))
    assert transitive.load_legacy_config(conf) == {"test": "value"}

def test_merge_config():
    args = MagicMock()
    args.souffle_include_dir = "/test/include"
    cfg = transitive.merge_config({"global": "1"}, args)
    
    assert cfg["global"] == "1"
    assert cfg["timing_dir"] == "timing"
    assert cfg["souffle_include_dir"] == "/test/include"

@patch('sys.argv', ['transitive.py', '--list-templates'])
@patch('engine.bootstrap.BootstrapManager.list_templates')
def test_main_list_templates(mock_list, capsys):
    mock_list.return_value = {
        'system_descriptors': ['sys1'],
        'domain_templates': ['dom1'],
        'rule_templates': ['rule1']
    }
    with pytest.raises(SystemExit) as exc:
        transitive.main()
    assert exc.value.code == 0
    out, err = capsys.readouterr()
    assert 'sys1' in out
    assert 'dom1' in out
    assert 'rule1' in out

@patch('sys.argv', ['transitive.py', '--bootstrap-system', 'newsys'])
@patch('engine.bootstrap.BootstrapManager.bootstrap_system')
def test_main_bootstrap_system(mock_boot):
    with pytest.raises(SystemExit) as exc:
        transitive.main()
    assert exc.value.code == 0

    mock_boot.side_effect = Exception("error")
    with pytest.raises(SystemExit) as exc:
        transitive.main()
    assert exc.value.code == 1

@patch('sys.argv', ['transitive.py', '--bootstrap-domain', 'newdom'])
@patch('engine.bootstrap.BootstrapManager.bootstrap_domain')
def test_main_bootstrap_domain(mock_boot):
    with pytest.raises(SystemExit) as exc:
        transitive.main()
    assert exc.value.code == 0

    mock_boot.side_effect = Exception("error")
    with pytest.raises(SystemExit) as exc:
        transitive.main()
    assert exc.value.code == 1

@patch('sys.argv', ['transitive.py', '--bootstrap-graph', 'newgraph', '--bootstrap-graph-generator', 'gen'])
@patch('engine.bootstrap.BootstrapManager.bootstrap_graph')
def test_main_bootstrap_graph(mock_boot):
    with pytest.raises(SystemExit) as exc:
        transitive.main()
    assert exc.value.code == 0

    mock_boot.side_effect = Exception("error")
    with pytest.raises(SystemExit) as exc:
        transitive.main()
    assert exc.value.code == 1

@patch('sys.argv', ['transitive.py', '--bootstrap-graph', 'newgraph'])
def test_main_bootstrap_graph_missing_gen():
    with pytest.raises(SystemExit) as exc:
        transitive.main()
    assert exc.value.code == 1

@patch('sys.argv', ['transitive.py', '--validate-rules', 'sys1'])
@patch('engine.validation.RuleValidator.validate_system')
def test_main_validate_rules(mock_val):
    mock_val.return_value = True
    with pytest.raises(SystemExit) as exc:
        transitive.main()
    assert exc.value.code == 0

    mock_val.side_effect = Exception("err")
    with pytest.raises(SystemExit) as exc:
        transitive.main()
    assert exc.value.code == 1

@patch('sys.argv', ['transitive.py', '--validate-domain', 'dom1'])
@patch('engine.validation.RuleValidator.validate_domain')
def test_main_validate_domain(mock_val):
    mock_val.return_value = True
    with pytest.raises(SystemExit) as exc:
        transitive.main()
    assert exc.value.code == 0

    mock_val.side_effect = Exception("err")
    with pytest.raises(SystemExit) as exc:
        transitive.main()
    assert exc.value.code == 1

@patch('sys.argv', ['transitive.py', '--test-rule', 'rule.sql'])
@patch('engine.validation.RuleValidator.test_rule_file')
def test_main_test_rule(mock_test):
    mock_test.return_value = True
    with pytest.raises(SystemExit) as exc:
        transitive.main()
    assert exc.value.code == 0

    mock_test.side_effect = Exception("err")
    with pytest.raises(SystemExit) as exc:
        transitive.main()
    assert exc.value.code == 1

@patch('sys.argv', ['transitive.py', '--ui'])
@patch('ui.app.create_app')
def test_main_ui(mock_create):
    mock_app = MagicMock()
    mock_create.return_value = mock_app
    transitive.main()
    mock_app.run.assert_called_once()

@patch('sys.argv', ['transitive.py'])
@patch('engine.loader.DescriptorLoader.load_global_config')
@patch('engine.loader.DescriptorLoader.load_systems')
@patch('engine.loader.DescriptorLoader.load_graph_types')
@patch('engine.loader.DescriptorLoader.get_domain')
@patch('engine.runner.ExperimentRunner.run')
def test_main_experiment(mock_run, mock_dom, mock_graph, mock_sys, mock_cfg):
    mock_cfg.return_value = {}
    
    mock_sys.return_value = []
    with pytest.raises(SystemExit) as exc:
        transitive.main()
    assert exc.value.code == 1

    sys_desc = MagicMock()
    sys_desc.name = 'sys1'
    mock_sys.return_value = [sys_desc]
    
    mock_graph.return_value = []
    with pytest.raises(SystemExit) as exc:
        transitive.main()
    assert exc.value.code == 1

    graph_desc = MagicMock()
    graph_desc.name = 'graph1'
    mock_graph.return_value = [graph_desc]

    mock_dom.return_value = None
    transitive.main()
    mock_run.assert_called_once()

    mock_dom.return_value = MagicMock()
    transitive.main()
    assert mock_run.call_count == 2
