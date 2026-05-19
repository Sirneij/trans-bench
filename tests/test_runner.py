import csv
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

from engine.loader import (
    DomainDescriptor,
    GraphTypeDescriptor,
    SystemDescriptor,
    TimingPhase,
)
from engine.runner import ExperimentRunner


class TestExperimentRunner:
    @pytest.fixture
    def mock_runner_fs(self, tmp_path):
        # Create systems directory
        sys_dir = tmp_path / 'systems' / 'test_sys'
        sys_dir.mkdir(parents=True)
        (sys_dir / 'descriptor.yaml').write_text('''
name: test_sys
display_name: Test System
category: db
protocol: test_proto
timing_phases:
  - {id: load, label: Load}
input_format: tsv
modes: [right_recursion]
rule_extension: .sql
flags:
  requires_credentials: false
        ''')
        # Create rules directory inside system
        rules_dir = sys_dir / 'rules'
        rules_dir.mkdir(parents=True)
        (rules_dir / 'transitive_right_recursion.sql').write_text('SELECT * FROM tc;')

        # Create graph types file
        graph_dir = tmp_path / 'graph_types'
        graph_dir.mkdir(parents=True)
        (graph_dir / 'test_graph.yaml').write_text('''
name: test_graph
display_name: Test Graph
description: Desc
generator: some.module.func
parameters: {p1: 1}
        ''')

        # Create timing destination dir
        timing_dir = tmp_path / 'timing'
        timing_dir.mkdir(parents=True)

        # Create input directory mock structure
        input_dir = tmp_path / 'input' / 'souffle' / 'test_graph' / '10'
        input_dir.mkdir(parents=True)
        (input_dir / 'edge.facts').write_text('1\t2\n')

        return {
            'base_dir': tmp_path,
            'timing_dir': timing_dir,
            'input_dir': tmp_path / 'input',
        }

    @patch('subprocess.run')
    @patch('engine.runner.get_connector')
    def test_experiment_runner_lifecycle(self, mock_get_connector, mock_run, mock_runner_fs):
        base_dir = mock_runner_fs['base_dir']
        timing_dir = mock_runner_fs['timing_dir']

        # Setup mock system descriptor
        sys_desc = SystemDescriptor(
            name="test_sys", display_name="Test System", category="db", protocol="test_proto",
            timing_phases=[TimingPhase("load", "Load")],
            input_format="tsv", modes=["right_recursion"], rule_extension=".sql", flags={}, execution={},
            descriptor_path=base_dir / 'systems' / 'test_sys' / 'descriptor.yaml',
            rules_dir=base_dir / 'systems' / 'test_sys' / 'rules', credentials={}, version="0.1"
        )

        # Setup mock graph descriptor
        graph_desc = GraphTypeDescriptor(
            name="test_graph", display_name="Test Graph", description="Desc",
            generator="some.module.func", parameters={"p1": 1}
        )

        # Mock connector
        mock_conn_class = MagicMock()
        mock_conn = MagicMock()
        mock_conn.run_experiment.return_value = {"LoadRealTime": 0.05, "LoadCPUTime": 0.04, "LoadMaxRAM_MB": 1.2}
        mock_conn_class.return_value = mock_conn
        mock_get_connector.return_value = mock_conn_class

        # Setup progress callback
        progresses = []
        def progress_cb(data):
            progresses.append(data)

        # Instantiate runner
        config = {
            'timing_dir': str(timing_dir),
            'database': ':memory:',
        }

        domain_desc = DomainDescriptor(
            name="transitive", display_name="Transitive", description="Transitive closure",
            category="recursion", modes=["right_recursion"], query_parameters=[],
            output_schema=[], data_requirements={},
            descriptor_path=base_dir / 'domains' / 'transitive' / 'descriptor.yaml'
        )

        runner = ExperimentRunner(
            config=config,
            systems=[sys_desc],
            graph_types=[graph_desc],
            size_range=[10, 11, 1], # size = 10 only
            num_runs=2,
            modes=["right_recursion"],
            domain="transitive",
            progress_cb=progress_cb,
            domain_descriptor=domain_desc
        )

        runner.base_dir = base_dir
        # Isolate runner filesystem paths
        runner._resolve_input_path = lambda system, graph, size: base_dir / 'input' / 'souffle' / graph.name / str(size) / 'edge.facts'
        runner._generate_input_data = lambda system: None

        # Run the benchmark
        runner.run()

        # Assert connector was resolved and connect/close was called
        mock_get_connector.assert_called_with("test_proto")
        mock_conn.connect.assert_called()
        mock_conn.run_experiment.assert_called()
        mock_conn.close.assert_called()

        # Assert csv results were created
        timing_file = timing_dir / 'transitive' / 'test_sys' / 'test_graph' / 'right_recursion_graph_10.csv'
        assert timing_file.exists()

        # Check content of timing CSV
        with open(timing_file) as f:
            reader = csv.reader(f)
            headers = next(reader)
            assert headers == ["LoadRealTime", "LoadCPUTime", "LoadMaxRAM_MB"]
            row1 = next(reader)
            assert float(row1[0]) == 0.05
            row2 = next(reader)
            assert float(row2[0]) == 0.05
            avg_row = next(reader)
            assert avg_row[0] == "Average"
            assert float(avg_row[1]) == 0.05

        # Check subprocess run was called for plot generation
        mock_run.assert_called()

        # Assert progress callback was called
        assert len(progresses) > 0
        assert any(p['type'] == 'progress' and p['status'] == 'done' for p in progresses)

    @patch('subprocess.run')
    @patch('engine.runner.get_connector')
    def test_demand_driven_bindings(self, mock_get_connector, mock_run, mock_runner_fs):
        base_dir = mock_runner_fs['base_dir']
        timing_dir = mock_runner_fs['timing_dir']

        # Setup mock system descriptor
        sys_desc = SystemDescriptor(
            name="test_sys", display_name="Test System", category="db", protocol="test_proto",
            timing_phases=[TimingPhase("load", "Load")],
            input_format="tsv", modes=["right_recursion"], rule_extension=".sql", flags={}, execution={},
            descriptor_path=base_dir / 'systems' / 'test_sys' / 'descriptor.yaml',
            rules_dir=base_dir / 'systems' / 'test_sys' / 'rules', credentials={}, version="0.1"
        )

        # Setup mock graph descriptor
        graph_desc = GraphTypeDescriptor(
            name="test_graph", display_name="Test Graph", description="Desc",
            generator="some.module.func", parameters={"p1": 1}
        )

        # Create query csv file for demand-driven (in input_path.parent)
        query_file = base_dir / 'input' / 'souffle' / 'test_graph' / '10' / 'queries_10.csv'
        query_file.parent.mkdir(parents=True, exist_ok=True)
        with open(query_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['src', 'dst'])
            writer.writerow(['42', '99'])

        # Mock connector
        mock_conn_class = MagicMock()
        mock_conn = MagicMock()
        mock_conn.run_experiment.return_value = {"LoadRealTime": 0.05, "LoadCPUTime": 0.04, "LoadMaxRAM_MB": 1.2}
        mock_conn_class.return_value = mock_conn
        mock_get_connector.return_value = mock_conn_class

        # Instantiate runner
        config = {
            'timing_dir': str(timing_dir),
            'database': ':memory:',
        }

        domain_desc = DomainDescriptor(
            name="transitive", display_name="Transitive", description="Transitive closure",
            category="recursion", modes=["right_recursion"], query_parameters=[],
            output_schema=[], data_requirements={},
            descriptor_path=base_dir / 'domains' / 'transitive' / 'descriptor.yaml'
        )

        runner = ExperimentRunner(
            config=config,
            systems=[sys_desc],
            graph_types=[graph_desc],
            size_range=[10, 11, 1], # size = 10 only
            num_runs=1,
            modes=["right_recursion"],
            domain="transitive",
            query_mode="demand_driven",
            domain_descriptor=domain_desc
        )

        runner.base_dir = base_dir
        # Isolate runner filesystem paths
        runner._resolve_input_path = lambda system, graph, size: base_dir / 'input' / 'souffle' / graph.name / str(size) / 'edge.facts'
        runner._generate_input_data = lambda system: None

        # Run
        runner.run()

        # Check if the query_bindings were correctly parsed and passed to the connector
        args, kwargs = mock_conn.run_experiment.call_args
        assert kwargs['query_bindings'] == {'src': '42', 'dst': '99'}


class TestExperimentRunnerHelpers:
    """Test private helper methods of ExperimentRunner."""

    @pytest.fixture
    def setup_runner(self, tmp_path):
        """Setup a basic runner instance for helper method tests."""
        sys_desc = SystemDescriptor(
            name="test_sys", display_name="Test System", category="db", protocol="test_proto",
            timing_phases=[TimingPhase("load", "Load")],
            input_format="tsv", modes=["right_recursion"], rule_extension=".sql", flags={}, execution={},
            descriptor_path=tmp_path / 'systems' / 'test_sys' / 'descriptor.yaml',
            rules_dir=tmp_path / 'systems' / 'test_sys' / 'rules', credentials={}, version="0.1"
        )
        
        graph_desc = GraphTypeDescriptor(
            name="test_graph", display_name="Test Graph", description="Desc",
            generator="some.module.func", parameters={"p1": 1}
        )
        
        domain_desc = DomainDescriptor(
            name="transitive", display_name="Transitive", description="Transitive closure",
            category="recursion", modes=["right_recursion"], query_parameters=[],
            output_schema=[], data_requirements={},
            descriptor_path=tmp_path / 'domains' / 'transitive' / 'descriptor.yaml'
        )
        
        runner = ExperimentRunner(
            config={'timing_dir': str(tmp_path / 'timing'), 'database': ':memory:'},
            systems=[sys_desc],
            graph_types=[graph_desc],
            size_range=[10, 20, 5],
            num_runs=1,
            modes=["right_recursion"],
            domain="transitive",
            domain_descriptor=domain_desc
        )
        runner.base_dir = tmp_path
        return runner, sys_desc, graph_desc, tmp_path

    def test_resolve_rule_path_full_domain_prefix(self, setup_runner):
        """Test _resolve_rule_path with full domain prefix."""
        runner, sys_desc, _, tmp_path = setup_runner
        
        # Create rule file with full domain prefix
        (sys_desc.rules_dir).mkdir(parents=True, exist_ok=True)
        rule_file = sys_desc.rules_dir / 'transitive_right_recursion.sql'
        rule_file.write_text('SELECT * FROM tc;')
        
        result = runner._resolve_rule_path(sys_desc, 'right_recursion')
        assert result == rule_file
        assert result.exists()

    def test_resolve_rule_path_shortened_domain_prefix(self, setup_runner):
        """Test _resolve_rule_path with shortened domain prefix fallback."""
        runner, sys_desc, _, tmp_path = setup_runner
        
        # Create rule file with shortened domain prefix
        (sys_desc.rules_dir).mkdir(parents=True, exist_ok=True)
        rule_file = sys_desc.rules_dir / 'transitive_right_recursion.sql'
        rule_file.write_text('SELECT * FROM tc;')
        
        result = runner._resolve_rule_path(sys_desc, 'right_recursion')
        assert result == rule_file

    def test_resolve_rule_path_legacy_no_prefix(self, setup_runner):
        """Test _resolve_rule_path with legacy non-prefixed fallback."""
        runner, sys_desc, _, tmp_path = setup_runner
        
        # Create only the legacy non-prefixed file
        (sys_desc.rules_dir).mkdir(parents=True, exist_ok=True)
        rule_file = sys_desc.rules_dir / 'right_recursion.sql'
        rule_file.write_text('SELECT * FROM tc;')
        
        result = runner._resolve_rule_path(sys_desc, 'right_recursion')
        assert result == rule_file

    def test_resolve_rule_path_not_found(self, setup_runner):
        """Test _resolve_rule_path returns None when no rule file exists."""
        runner, sys_desc, _, tmp_path = setup_runner
        (sys_desc.rules_dir).mkdir(parents=True, exist_ok=True)
        
        result = runner._resolve_rule_path(sys_desc, 'nonexistent_mode')
        assert result is None

    def test_resolve_input_path_tsv(self, setup_runner):
        """Test _resolve_input_path for TSV format."""
        runner, _, graph_desc, _ = setup_runner
        sys_desc = runner.systems[0]
        sys_desc.input_format = 'tsv'
        
        path = runner._resolve_input_path(sys_desc, graph_desc, 100)
        assert 'souffle' in str(path)
        assert 'edge.facts' in str(path)
        assert str(100) in str(path)

    def test_resolve_input_path_lp(self, setup_runner):
        """Test _resolve_input_path for LP (Datalog) format."""
        runner, _, graph_desc, _ = setup_runner
        sys_desc = runner.systems[0]
        sys_desc.input_format = 'lp'
        
        path = runner._resolve_input_path(sys_desc, graph_desc, 100)
        assert 'clingo_xsb' in str(path)
        assert '.lp' in str(path)

    def test_resolve_input_path_pickle(self, setup_runner):
        """Test _resolve_input_path for Pickle format."""
        runner, _, graph_desc, _ = setup_runner
        sys_desc = runner.systems[0]
        sys_desc.input_format = 'pickle'
        
        path = runner._resolve_input_path(sys_desc, graph_desc, 100)
        assert 'alda' in str(path)
        assert '.pkl' in str(path)

    def test_resolve_input_path_facts(self, setup_runner):
        """Test _resolve_input_path for facts format."""
        runner, _, graph_desc, _ = setup_runner
        sys_desc = runner.systems[0]
        sys_desc.input_format = 'facts'
        
        path = runner._resolve_input_path(sys_desc, graph_desc, 100)
        assert 'souffle' in str(path)

    def test_resolve_input_path_custom(self, setup_runner):
        """Test _resolve_input_path for custom format."""
        runner, _, graph_desc, _ = setup_runner
        sys_desc = runner.systems[0]
        sys_desc.input_format = 'custom'
        
        path = runner._resolve_input_path(sys_desc, graph_desc, 100)
        assert sys_desc.name in str(path)

    def test_all_modes_exist_true(self, setup_runner, tmp_path):
        """Test _all_modes_exist returns True when all modes have files."""
        runner, sys_desc, graph_desc, _ = setup_runner
        
        # Create timing files for all modes
        timing_file = runner._timing_path(sys_desc, graph_desc, 10, 'right_recursion')
        timing_file.parent.mkdir(parents=True, exist_ok=True)
        timing_file.write_text('test')
        
        result = runner._all_modes_exist(sys_desc, graph_desc, 10)
        assert result is True

    def test_all_modes_exist_false(self, setup_runner):
        """Test _all_modes_exist returns False when modes are missing."""
        runner, sys_desc, graph_desc, _ = setup_runner
        
        result = runner._all_modes_exist(sys_desc, graph_desc, 10)
        assert result is False

    def test_write_timing_new_file(self, setup_runner, tmp_path):
        """Test _write_timing creates new CSV with headers."""
        runner, sys_desc, graph_desc, _ = setup_runner
        timing_path = runner._timing_path(sys_desc, graph_desc, 10, 'right_recursion')
        
        headers = ['LoadRealTime', 'LoadCPUTime', 'LoadMaxRAM_MB']
        timing = {'LoadRealTime': 0.5, 'LoadCPUTime': 0.3, 'LoadMaxRAM_MB': 10.5}
        
        runner._write_timing(timing_path, headers, timing)
        
        assert timing_path.exists()
        with open(timing_path) as f:
            reader = csv.reader(f)
            assert next(reader) == headers
            assert next(reader) == ['0.5', '0.3', '10.5']

    def test_write_timing_append_file(self, setup_runner):
        """Test _write_timing appends to existing CSV without duplicate headers."""
        runner, sys_desc, graph_desc, _ = setup_runner
        timing_path = runner._timing_path(sys_desc, graph_desc, 10, 'right_recursion')
        
        headers = ['LoadRealTime', 'LoadCPUTime']
        timing1 = {'LoadRealTime': 0.5, 'LoadCPUTime': 0.3}
        timing2 = {'LoadRealTime': 0.6, 'LoadCPUTime': 0.4}
        
        runner._write_timing(timing_path, headers, timing1)
        runner._write_timing(timing_path, headers, timing2)
        
        with open(timing_path) as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        assert len(rows) == 3  # headers + 2 data rows
        assert rows[0] == headers
        assert rows[1] == ['0.5', '0.3']
        assert rows[2] == ['0.6', '0.4']

    def test_append_average_normal_data(self, setup_runner):
        """Test _append_average calculates correct averages."""
        runner, sys_desc, graph_desc, _ = setup_runner
        timing_path = runner._timing_path(sys_desc, graph_desc, 10, 'right_recursion')
        
        headers = ['LoadRealTime', 'LoadCPUTime']
        timing1 = {'LoadRealTime': 0.5, 'LoadCPUTime': 0.2}
        timing2 = {'LoadRealTime': 0.7, 'LoadCPUTime': 0.4}
        
        runner._write_timing(timing_path, headers, timing1)
        runner._write_timing(timing_path, headers, timing2)
        runner._append_average(timing_path)
        
        with open(timing_path) as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        assert len(rows) == 4  # headers + 2 data + average
        assert rows[3][0] == 'Average'
        assert float(rows[3][1]) == pytest.approx(0.6)  # (0.5 + 0.7) / 2
        assert float(rows[3][2]) == pytest.approx(0.3)  # (0.2 + 0.4) / 2

    def test_append_average_nonexistent_file(self, setup_runner):
        """Test _append_average handles nonexistent file gracefully."""
        runner, sys_desc, graph_desc, _ = setup_runner
        timing_path = runner._timing_path(sys_desc, graph_desc, 10, 'right_recursion')
        
        # Should not raise exception
        runner._append_average(timing_path)

    def test_append_average_empty_file(self, setup_runner):
        """Test _append_average handles empty CSV gracefully."""
        runner, sys_desc, graph_desc, _ = setup_runner
        timing_path = runner._timing_path(sys_desc, graph_desc, 10, 'right_recursion')
        timing_path.parent.mkdir(parents=True, exist_ok=True)
        timing_path.write_text('')
        
        runner._append_average(timing_path)
        
        # File should still exist and be mostly empty (only header line attempted)
        assert timing_path.exists()

    def test_append_average_with_invalid_values(self, setup_runner):
        """Test _append_average handles non-numeric values gracefully."""
        runner, sys_desc, graph_desc, _ = setup_runner
        timing_path = runner._timing_path(sys_desc, graph_desc, 10, 'right_recursion')
        
        timing_path.parent.mkdir(parents=True, exist_ok=True)
        with open(timing_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Time1', 'Time2'])
            writer.writerow(['0.5', 'invalid'])
            writer.writerow(['0.6', '0.4'])
        
        runner._append_average(timing_path)
        
        with open(timing_path) as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        assert rows[-1][0] == 'Average'
        assert float(rows[-1][1]) == pytest.approx(0.55)

    @patch('subprocess.run')
    def test_generate_input_data_missing_graphs(self, mock_run, setup_runner):
        """Test _generate_input_data generates missing input."""
        runner, sys_desc, _, tmp_path = setup_runner
        
        mock_run.return_value = MagicMock(returncode=0, stdout='', stderr='')
        
        runner._generate_input_data(sys_desc)
        
        # Should call generate_db.py
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert 'generate_db.py' in args

    @patch('subprocess.run')
    def test_generate_input_data_with_errors(self, mock_run, setup_runner):
        """Test _generate_input_data emits errors on subprocess failure."""
        runner, sys_desc, _, tmp_path = setup_runner
        
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout='',
            stderr='Error: failed to generate'
        )
        
        logs = []
        def log_cb(data):
            if data['type'] == 'log':
                logs.append(data)
        
        runner.progress_cb = log_cb
        runner._generate_input_data(sys_desc)
        
        # Should emit error log
        assert len(logs) > 0
        assert any('error' in log['level'].lower() for log in logs)

    @patch('engine.runner.get_connector')
    def test_run_single_exception_handling(self, mock_get_connector, setup_runner):
        """Test _run_single handles connector exceptions gracefully."""
        runner, sys_desc, graph_desc, tmp_path = setup_runner
        
        (sys_desc.rules_dir).mkdir(parents=True, exist_ok=True)
        (sys_desc.rules_dir / 'right_recursion.sql').write_text('SELECT * FROM tc;')
        
        # Make connector raise exception
        mock_conn_class = MagicMock()
        mock_conn = MagicMock()
        mock_conn.connect.side_effect = Exception('Connection failed')
        mock_conn_class.return_value = mock_conn
        mock_get_connector.return_value = mock_conn_class
        
        logs = []
        def log_cb(data):
            if data['type'] == 'log':
                logs.append(data)
        
        runner.progress_cb = log_cb
        
        # Should not raise exception
        rule_path = sys_desc.rules_dir / 'right_recursion.sql'
        input_path = tmp_path / 'input.tsv'
        input_path.write_text('1\t2\n')
        output_folder = tmp_path / 'output'
        timing_path = tmp_path / 'timing.csv'
        
        runner._run_single(sys_desc, rule_path, input_path, output_folder, timing_path, 100)
        
        # Should close connector even on exception
        mock_conn.close.assert_called()

    def test_timing_path_creates_dirs(self, setup_runner):
        """Test _timing_path creates required directories."""
        runner, sys_desc, graph_desc, _ = setup_runner
        
        timing_path = runner._timing_path(sys_desc, graph_desc, 10, 'mode')
        
        # Should create parent directories
        assert timing_path.parent.exists()

    def test_prepare_output_folder_creates_dirs(self, setup_runner):
        """Test _prepare_output_folder creates required directories."""
        runner, sys_desc, graph_desc, _ = setup_runner
        
        folder = runner._prepare_output_folder(sys_desc, graph_desc, 10, 'mode')
        
        assert folder.exists()
        assert folder.is_dir()

    def test_emit_with_progress_callback(self, setup_runner):
        """Test _emit sends progress update to callback."""
        runner, sys_desc, graph_desc, _ = setup_runner
        
        progresses = []
        def progress_cb(data):
            progresses.append(data)
        
        runner.progress_cb = progress_cb
        
        runner._emit(50, 100, sys_desc.name, graph_desc.name, 10, 'mode', 'running')
        
        assert len(progresses) == 1
        assert progresses[0]['type'] == 'progress'
        assert progresses[0]['done'] == 50
        assert progresses[0]['total'] == 100
        assert progresses[0]['pct'] == 50.0
        assert progresses[0]['status'] == 'running'

    def test_emit_without_callback(self, setup_runner):
        """Test _emit doesn't crash without callback."""
        runner, sys_desc, graph_desc, _ = setup_runner
        runner.progress_cb = None
        
        # Should not raise exception
        runner._emit(50, 100, sys_desc.name, graph_desc.name, 10, 'mode', 'running')

    def test_emit_log_with_callback(self, setup_runner):
        """Test _emit_log sends log messages to callback."""
        runner, _, _, _ = setup_runner
        
        logs = []
        def progress_cb(data):
            logs.append(data)
        
        runner.progress_cb = progress_cb
        
        runner._emit_log('Test message', level='warning')
        
        assert len(logs) == 1
        assert logs[0]['type'] == 'log'
        assert logs[0]['message'] == 'Test message'
        assert logs[0]['level'] == 'warning'

    def test_clean_empty_timing_dirs(self, setup_runner):
        """Test _clean_empty_timing_dirs removes empty directories."""
        runner, _, _, _ = setup_runner
        
        # Create empty subdirectory
        empty_dir = runner.timing_dir / 'empty_domain'
        empty_dir.mkdir(parents=True, exist_ok=True)
        
        # Create directory with CSV
        csv_dir = runner.timing_dir / 'domain_with_csv'
        csv_dir.mkdir(parents=True, exist_ok=True)
        (csv_dir / 'test.csv').write_text('data')
        
        runner._clean_empty_timing_dirs()
        
        # Empty dir should be deleted
        assert not empty_dir.exists()
        # Dir with CSV should remain
        assert csv_dir.exists()

    def test_domain_filtering_with_unsupported_modes(self, setup_runner):
        """Test domain filtering removes unsupported modes."""
        runner, sys_desc, graph_desc, tmp_path = setup_runner
        
        domain_desc = DomainDescriptor(
            name="limited", display_name="Limited Domain", description="Desc",
            category="recursion", modes=["mode1"],  # Only mode1 is supported
            query_parameters=[], output_schema=[], data_requirements={},
            descriptor_path=tmp_path / 'domains' / 'limited' / 'descriptor.yaml'
        )
        
        runner2 = ExperimentRunner(
            config={'timing_dir': str(tmp_path / 'timing'), 'database': ':memory:'},
            systems=[sys_desc],
            graph_types=[graph_desc],
            size_range=[10, 20, 5],
            num_runs=1,
            modes=["mode1", "mode2", "mode3"],  # Request 3 modes
            domain="limited",
            domain_descriptor=domain_desc
        )
        
        # Should filter to only mode1
        assert runner2.modes == ["mode1"]

    def test_domain_filtering_all_unsupported(self, setup_runner):
        """Test domain filtering falls back when all modes unsupported."""
        runner, sys_desc, graph_desc, tmp_path = setup_runner
        
        domain_desc = DomainDescriptor(
            name="incompatible", display_name="Incompatible", description="Desc",
            category="recursion", modes=["other_mode"],
            query_parameters=[], output_schema=[], data_requirements={},
            descriptor_path=tmp_path / 'domains' / 'incompatible' / 'descriptor.yaml'
        )
        
        runner2 = ExperimentRunner(
            config={'timing_dir': str(tmp_path / 'timing'), 'database': ':memory:'},
            systems=[sys_desc],
            graph_types=[graph_desc],
            size_range=[10, 20, 5],
            num_runs=1,
            modes=["mode1", "mode2"],
            domain="incompatible",
            domain_descriptor=domain_desc
        )
        
        # Should fall back to requested modes
        assert runner2.modes == ["mode1", "mode2"]
