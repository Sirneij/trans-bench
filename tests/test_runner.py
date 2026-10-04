"""Tests of engine/runner.py: one trial of one configuration, with a mocked connector."""

import csv
from unittest.mock import MagicMock, patch

import pytest

from engine.loader import GraphTypeDescriptor, SystemDescriptor, TimingPhase
from engine.runner import TrialRunner, edge_file, input_path


def _system(tmp_path, input_format='tsv', modes=('right_recursion',)):
    return SystemDescriptor(
        name='test_sys',
        display_name='Test System',
        category='db',
        protocol='test_proto',
        timing_phases=[TimingPhase('load', 'Load')],
        input_format=input_format,
        modes=list(modes),
        rule_extension='.sql',
        flags={},
        execution={},
        descriptor_path=tmp_path / 'systems' / 'test_sys' / 'descriptor.yaml',
        rules_dir=tmp_path / 'systems' / 'test_sys' / 'rules',
        credentials={},
        version='0.1',
    )


def _graph():
    return GraphTypeDescriptor(
        name='test_graph', display_name='Test Graph', description='Desc', generator='some.module.func', parameters={}
    )


@pytest.fixture
def setup(tmp_path, monkeypatch):
    """A system with one rule file, an input graph of size 10, and a runner writing under tmp_path/timing."""
    monkeypatch.chdir(tmp_path)  # input paths are relative to the working directory
    system = _system(tmp_path)
    system.rules_dir.mkdir(parents=True)
    (system.rules_dir / 'transitive_right_recursion.sql').write_text('SELECT * FROM tc;')
    edges = tmp_path / 'input' / 'souffle' / 'test_graph' / '10' / 'edge.facts'
    edges.parent.mkdir(parents=True)
    edges.write_text('1\t2\n')
    runner = TrialRunner({'database': ':memory:'}, timing_dir=tmp_path / 'timing')
    runner.base_dir = tmp_path
    return runner, system, _graph(), tmp_path


def _connector(timing=None, connect_error=None):
    conn = MagicMock()
    conn.errors = []
    conn.memory = None
    conn.run_experiment.return_value = timing or {'LoadRealTime': 0.05, 'LoadCPUTime': 0.04, 'LoadMaxRAM_MB': 1.2}
    if connect_error:
        conn.connect.side_effect = connect_error
    return MagicMock(return_value=conn), conn


class TestRunTrial:
    @patch('engine.runner.get_connector')
    def test_one_trial_writes_one_row(self, get_connector, setup):
        runner, system, graph, tmp_path = setup
        get_connector.return_value, conn = _connector()
        outcome = runner.run_trial(system, graph, 10, 'right_recursion')
        get_connector.assert_called_with('test_proto')
        conn.connect.assert_called_once()
        conn.close.assert_called_once()
        assert outcome['errors'] == []
        timing_file = tmp_path / 'timing' / 'transitive' / 'test_sys' / 'test_graph' / 'right_recursion_graph_10.csv'
        assert outcome['timing_path'] == str(timing_file)
        rows = list(csv.reader(open(timing_file)))
        assert rows == [['LoadRealTime', 'LoadCPUTime', 'LoadMaxRAM_MB'], ['0.05', '0.04', '1.2']]

    @patch('engine.runner.get_connector')
    def test_demand_driven_bindings(self, get_connector, setup):
        runner, system, graph, tmp_path = setup
        runner.query_mode = 'demand_driven'
        with open(tmp_path / 'input' / 'souffle' / 'test_graph' / '10' / 'queries_10.csv', 'w', newline='') as f:
            csv.writer(f).writerows([['src', 'dst'], ['42', '99']])
        get_connector.return_value, conn = _connector()
        runner.run_trial(system, graph, 10, 'right_recursion')
        args, kwargs = conn.run_experiment.call_args
        assert kwargs['query_bindings'] == {'src': '42', 'dst': '99'}
        assert args[4]['query_mode'] == 'demand_driven'

    @patch('engine.runner.get_connector')
    def test_connect_failure_is_an_error_without_row(self, get_connector, setup):
        runner, system, graph, tmp_path = setup
        get_connector.return_value, conn = _connector(connect_error=Exception('Connection failed'))
        outcome = runner.run_trial(system, graph, 10, 'right_recursion')
        assert outcome['timing'] is None
        assert 'Connection failed' in outcome['errors'][0]
        conn.close.assert_called_once()  # closed even after a failure
        assert not (
            tmp_path / 'timing' / 'transitive' / 'test_sys' / 'test_graph' / 'right_recursion_graph_10.csv'
        ).exists()

    def test_undeclared_mode(self, setup):
        runner, system, graph, _ = setup
        outcome = runner.run_trial(system, graph, 10, 'left_recursion')
        assert outcome['timing'] is None and 'does not declare' in outcome['errors'][0]

    def test_missing_input(self, setup):
        runner, system, graph, _ = setup
        outcome = runner.run_trial(system, graph, 20, 'right_recursion')
        assert 'not found' in outcome['errors'][0]


class TestRulePath:
    def test_full_domain_prefix(self, setup):
        runner, system, _, _ = setup
        assert (
            runner.resolve_rule_path(system, 'right_recursion') == system.rules_dir / 'transitive_right_recursion.sql'
        )

    def test_shortened_domain_prefix(self, setup):
        runner, system, _, _ = setup
        runner.domain = 'transitive_closure'
        assert (
            runner.resolve_rule_path(system, 'right_recursion') == system.rules_dir / 'transitive_right_recursion.sql'
        )

    def test_mode_without_prefix(self, setup):
        runner, system, _, _ = setup
        (system.rules_dir / 'left_recursion.sql').write_text('SELECT 1;')
        assert runner.resolve_rule_path(system, 'left_recursion') == system.rules_dir / 'left_recursion.sql'

    def test_not_found(self, setup):
        runner, system, _, _ = setup
        assert runner.resolve_rule_path(system, 'nonexistent_mode') is None


class TestInputPath:
    @pytest.mark.parametrize(
        'fmt, expected',
        [
            ('tsv', 'input/souffle/g/100/edge.facts'),
            ('facts', 'input/souffle/g/100'),
            ('lp', 'input/clingo_xsb/g/graph_100.lp'),
            ('pickle', 'input/alda/g/graph_100.da'),  # the name generate_db.py writes
            ('custom', 'input/test_sys/g/graph_100'),
        ],
    )
    def test_formats(self, tmp_path, fmt, expected):
        assert str(input_path(_system(tmp_path, input_format=fmt), 'g', 100)) == expected

    def test_edge_file(self):
        assert str(edge_file('cycle', 10)) == 'input/souffle/cycle/10/edge.facts'


class TestTimingFiles:
    def test_timing_path_and_output_folder_are_created(self, setup):
        runner, system, graph, _ = setup
        assert runner.timing_path(system, graph, 10, 'mode').parent.is_dir()
        assert runner.output_folder(system, graph, 10, 'mode').is_dir()

    def test_write_timing_appends_without_second_header(self, setup):
        runner, system, graph, _ = setup
        path = runner.timing_path(system, graph, 10, 'right_recursion')
        headers = ['LoadRealTime', 'LoadCPUTime']
        runner._write_timing(path, headers, {'LoadRealTime': 0.5, 'LoadCPUTime': 0.3})
        runner._write_timing(path, headers, {'LoadRealTime': 0.6})
        assert list(csv.reader(open(path))) == [headers, ['0.5', '0.3'], ['0.6', '0.0']]
