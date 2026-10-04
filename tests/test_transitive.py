import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import transitive


@patch('sys.argv', ['transitive.py', '--list-templates'])
@patch('engine.bootstrap.BootstrapManager.list_templates')
def test_main_list_templates(mock_list, capsys):
    mock_list.return_value = {'system_descriptors': ['sys1'], 'domain_templates': ['dom1'], 'rule_templates': ['rule1']}
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


@patch(
    'sys.argv',
    [
        'transitive.py',
        '--systems',
        'duckdb',
        '--graphs',
        'cycle',
        '--sizes',
        '10',
        '31',
        '10',
        '--num-runs',
        '2',
        '--timeout',
        '30',
        '--campaign',
        'results/x',
        '--no-analysis',
    ],
)
@patch('engine.campaign.Campaign')
def test_main_runs_a_campaign(mock_campaign):
    """The run options of transitive.py become a CampaignSpec for engine/campaign.py."""
    mock_campaign.return_value.run.return_value = {'ok': 3, 'failed': 0, 'skipped': 0, 'resumed': 0, 'stopped': False}
    with pytest.raises(SystemExit) as exc:
        transitive.main()
    assert exc.value.code == 0
    spec = mock_campaign.call_args[0][0]
    assert spec.systems == ['duckdb'] and spec.graphs == ['cycle'] and spec.sizes == [10, 20, 30]
    assert spec.runs == 2 and spec.timeout == 30 and spec.campaign_dir.name == 'x'


@patch('sys.argv', ['transitive.py', '--systems', 'nope', '--no-analysis'])
def test_main_unknown_system_exits_with_error():
    with pytest.raises(SystemExit) as exc:
        transitive.main()
    assert exc.value.code == 1
