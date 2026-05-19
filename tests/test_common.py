import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch
import tempfile
import pytest

from common import Base, AnalyzeSystems

class TestCommonBase:
    def test_init(self):
        b = Base({"neo4j": {"uri": "bolt://localhost"}})
        assert len(b.headers_rdbms) == 12
        assert len(b.headers_neo4j) == 12
        assert len(b.headers_mongodb) == 10

    def test_connect_db_unsupported(self):
        b = Base({})
        with pytest.raises(ValueError):
            b.connect_db('unsupported')

    @patch('duckdb.connect')
    def test_connect_duckdb(self, mock_connect):
        b = Base({})
        b.connect_db('duckdb', 'some/path/rule.sql')
        mock_connect.assert_called_once()
        assert b.db_path.name == 'duckdb_file.db'

    @patch('neo4j.GraphDatabase.driver')
    def test_connect_neo4j(self, mock_driver):
        b = Base({'neo4j': {'uri': 'bolt', 'user': 'u', 'password': 'p'}})
        b.connect_db('neo4j')
        mock_driver.assert_called_once_with('bolt', auth=('u', 'p'))

    @patch('common.MongoClient')
    def test_connect_mongodb(self, mock_client):
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance
        b = Base({'mongodb': {'uri': 'mongo', 'database': 'db'}})
        b.connect_db('mongodb')
        mock_client.assert_called_once_with('mongo')
        mock_instance.__getitem__.assert_called_once_with('db')

    @patch('psycopg2.connect')
    def test_connect_psycopg2(self, mock_connect):
        b = Base({'postgres': {'dbURL': 'pg'}})
        b.connect_db('postgres')
        mock_connect.assert_called_once_with('pg')

    @patch('MySQLdb.connect')
    def test_connect_mariadb(self, mock_connect):
        b = Base({'mariadb': {'database': 'db', 'user': 'u', 'password': 'p', 'host': 'h', 'port': '123'}})
        b.connect_db('mariadb')
        mock_connect.assert_called_once_with(db='db', user='u', passwd='p', host='h', port='123', local_infile=1)

    def test_close(self, tmp_path):
        b = Base({})
        b.driver = MagicMock()
        b.db_path = tmp_path / "test.db"
        b.db_path.touch()
        b.close()
        assert b.driver is None
        assert not b.db_path.exists()

class TestAnalyzeSystems:
    def test_discover_rules(self, tmp_path):
        a = AnalyzeSystems({}, "souffle")
        (tmp_path / "rule_test.dl").touch()
        (tmp_path / "other.txt").touch()
        rules = a.discover_rules(tmp_path, ".dl")
        assert "test" in rules

    def test_estimate_time_duration(self):
        a = AnalyzeSystems({}, "clingo")
        t1 = (1.0, 1.0, 1.0, 1.0, 10.0)
        t2 = (2.0, 2.0, 2.0, 2.0, 15.0)
        real, cpu = a.estimate_time_duration(t1, t2)
        assert real == 5.0
        assert cpu == 4.0

    @patch('subprocess.run')
    @patch('os.times')
    def test_run_souffle_command(self, mock_times, mock_run):
        a = AnalyzeSystems({}, "souffle")
        mock_times.side_effect = [(0,0,0,0,0), (1,1,1,1,5)]
        res = MagicMock()
        res.returncode = 0
        res.stdout = "load time: 1.5 seconds\nquery time: 2.0 seconds"
        res.stderr = ""
        mock_run.return_value = res
        
        time_str, timings = a.run_souffle_command("echo test")
        assert time_str == "5,4"
        assert timings["load time"] == 1.5

    @patch('subprocess.run')
    def test_run_souffle_command_error(self, mock_run):
        a = AnalyzeSystems({}, "souffle")
        mock_run.side_effect = subprocess.CalledProcessError(1, "cmd")
        time_str, timings = a.run_souffle_command("cmd")
        assert time_str == "Command failed"
        
        mock_run.side_effect = Exception("error")
        time_str, timings = a.run_souffle_command("cmd")
        assert time_str == "Error"

        # test return code != 0
        res = MagicMock()
        res.returncode = 1
        res.stdout = ""
        res.stderr = ""
        mock_run.side_effect = None
        mock_run.return_value = res
        time_str, timings = a.run_souffle_command("cmd")
        assert time_str == "0,0"

    @patch('builtins.open', side_effect=IOError("mock error"))
    def test_replace_rule_file_content_read_error(self, mock_open, tmp_path):
        a = AnalyzeSystems({}, "clingo")
        rule = tmp_path / "rule.lp"
        with pytest.raises(IOError):
            a.replace_rule_file_content(rule, "clingo", "path(X, Z)")

    @patch('tempfile.NamedTemporaryFile', side_effect=Exception("temp error"))
    def test_replace_rule_file_content_write_error(self, mock_temp, tmp_path):
        a = AnalyzeSystems({}, "clingo")
        rule = tmp_path / "rule.lp"
        rule.write_text("#show path/2.")
        with pytest.raises(IOError):
            a.replace_rule_file_content(rule, "clingo", "path(X, Z)")

    def test_replace_rule_file_content(self, tmp_path):
        a = AnalyzeSystems({}, "clingo")
        rule = tmp_path / "rule.lp"
        rule.write_text("#show path/2.")
        
        out = a.replace_rule_file_content(rule, "clingo", "path(X, Z)")
        assert out is not None
        assert "#show ppath/1." in Path(out).read_text()
        
        a.environment = "souffle"
        rule.write_text(".output path")
        out = a.replace_rule_file_content(rule, "souffle", "path(X, Z)")
        assert out is not None
        assert ".output ppath" in Path(out).read_text()

        # Unchanged path(x, y)
        out = a.replace_rule_file_content(rule, "souffle", "path(X, y)")
        assert out is None

        with pytest.raises(ValueError):
            a.replace_rule_file_content(rule, "invalid", "path(X, Z)")

        with pytest.raises(ValueError):
            a.replace_rule_file_content(rule, "souffle", "invalid")

    def test_set_output_folder(self, tmp_path):
        a = AnalyzeSystems({}, "duckdb")
        a.timing_path = tmp_path / "timing_mode1_graph_100.csv"
        a.set_output_folder()
        assert a.output_folder.name == "100"
        assert a.output_folder.parent.name == "mode1"

    def test_set_file_paths(self, tmp_path):
        a = AnalyzeSystems({"defaults": {"systems": {"environmentExtensions": {"duckdb": [".sql"]}, "dbSystems": ["duckdb"]}}}, "duckdb")
        with patch('common.AnalyzeSystems.discover_rules') as mock_disc:
            mock_disc.return_value = {"mode1": Path("rule.sql")}
            a.set_file_paths("mode1", "cycle", 100, str(tmp_path))
            assert a.rule_path.name == "rule.sql"
            assert "timing_mode1" in a.timing_path.name
            assert a.input_path.name == "edge.facts"

        a2 = AnalyzeSystems({"defaults": {"systems": {"environmentExtensions": {"souffle": [".dl"]}, "dbSystems": ["duckdb"]}}}, "souffle")
        with patch('common.AnalyzeSystems.discover_rules') as mock_disc:
            mock_disc.return_value = {"mode1": Path("rule.dl")}
            a2.set_file_paths("mode1", "cycle", 100, str(tmp_path))
            assert a2.input_path.name == "100"

        # test rule missing
        a3 = AnalyzeSystems({}, "souffle")
        with patch('common.AnalyzeSystems.discover_rules') as mock_disc:
            mock_disc.return_value = {}
            a3.set_file_paths("mode_missing", "cycle", 100, str(tmp_path))
            assert a3.rule_path is None

        # test clingo input path
        a4 = AnalyzeSystems({"defaults": {"systems": {"environmentExtensions": {"clingo": [".lp"]}, "dbSystems": ["duckdb"]}}}, "clingo")
        with patch('common.AnalyzeSystems.discover_rules') as mock_disc:
            mock_disc.return_value = {"mode1": Path("rule.lp")}
            a4.set_file_paths("mode1", "cycle", 100, str(tmp_path))
            assert a4.input_path.name == "graph_100.lp"

    def test_analyze(self):
        a = AnalyzeSystems({}, "duckdb")
        with pytest.raises(NotImplementedError):
            a.analyze()
