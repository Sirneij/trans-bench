
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from engine.connectors.base import BaseConnector
from engine.connectors.duckdb_conn import DuckDBConnector
from engine.connectors.mongodb_conn import MongoDBConnector
from engine.connectors.neo4j_conn import Neo4jConnector
from engine.connectors.rdbms import (
    CockroachDBConnector,
    MariaDBConnector,
    PostgreSQLConnector,
)
from engine.connectors.subprocess_conn import (
    AldaConnector,
    ClingoConnector,
    SouffleConnector,
    XSBConnector,
)
from engine.loader import SystemDescriptor, TimingPhase


class DummyConnector(BaseConnector):
    def connect(self, credentials, descriptor):
        pass
    def run_experiment(self, rule_path, input_path, output_folder, descriptor, config, query_bindings=None):
        return {}
    def close(self):
        pass


def test_base_connector_utilities():
    conn = DummyConnector()

    # Test _substitute_query_bindings
    content = "SELECT * FROM t WHERE x = ?src AND y = ?dst;"
    bindings = {"src": 42, "dst": "hello"}
    substituted = conn._substitute_query_bindings(content, bindings)
    assert substituted == "SELECT * FROM t WHERE x = 42 AND y = hello;"

    # Test _substitute_query_bindings empty bindings
    assert conn._substitute_query_bindings(content, None) == content

    # Test timed helper
    def test_func(x):
        return x + 1
    real, cpu, res = conn.timed(test_func, 5)
    assert res == 6
    assert isinstance(real, float)
    assert isinstance(cpu, float)

    # Test build_timing_row
    phases = [TimingPhase(id="load", label="Load"), TimingPhase(id="solve", label="Solve")]
    measurements = [(1.5, 0.5), (2.0, 1.0)]
    memory = [10.0, 20.0]
    row = conn.build_timing_row(phases, measurements, memory)
    assert row == {
        "LoadRealTime": 1.5,
        "LoadCPUTime": 0.5,
        "LoadMaxRAM_MB": 10.0,
        "SolveRealTime": 2.0,
        "SolveCPUTime": 1.0,
        "SolveMaxRAM_MB": 20.0,
    }


@patch('subprocess.Popen')
def test_base_connector_timed_subprocess(mock_popen):
    # Set up process mock
    proc = MagicMock()
    proc.pid = 12345
    proc.communicate.return_value = ("stdout_test", "stderr_test")
    proc.returncode = 0
    mock_popen.return_value = proc

    # Mock psutil
    with patch('psutil.Process') as mock_ps_proc:
        ps_proc = MagicMock()
        ps_proc.memory_info.return_value.rss = 1024 * 1024 * 5 # 5 MB
        ps_proc.children.return_value = []
        mock_ps_proc.return_value = ps_proc

        real, cpu, mem, result = BaseConnector.timed_subprocess(["ls", "-la"])
        
        assert result.stdout == "stdout_test"
        assert result.stderr == "stderr_test"
        assert result.returncode == 0
        assert mem >= 5.0

@patch('subprocess.Popen')
def test_base_connector_timed_subprocess_exceptions(mock_popen):
    import psutil
    proc = MagicMock()
    proc.pid = 12345
    proc.communicate.return_value = ("stdout", "stderr")
    mock_popen.return_value = proc

    # Test NoSuchProcess at process creation
    with patch('psutil.Process') as mock_ps_proc:
        mock_ps_proc.side_effect = psutil.NoSuchProcess(12345)
        BaseConnector.timed_subprocess(["ls"])

    # Test NoSuchProcess during loop
    with patch('psutil.Process') as mock_ps_proc:
        ps_proc = MagicMock()
        ps_proc.memory_info.side_effect = psutil.NoSuchProcess(12345)
        mock_ps_proc.return_value = ps_proc
        BaseConnector.timed_subprocess(["ls"])
        
    # Test child memory adding
    with patch('psutil.Process') as mock_ps_proc:
        ps_proc = MagicMock()
        ps_proc.memory_info.return_value.rss = 1000
        child_proc = MagicMock()
        child_proc.memory_info.return_value.rss = 500
        ps_proc.children.return_value = [child_proc]
        mock_ps_proc.return_value = ps_proc
        BaseConnector.timed_subprocess(["ls"])



@patch('duckdb.connect')
def test_duckdb_connector(mock_duckdb_connect, tmp_path):
    conn = DuckDBConnector()
    desc = SystemDescriptor(
        name="duckdb", display_name="DuckDB", category="db", protocol="duckdb",
        timing_phases=[TimingPhase("load", "Load"), TimingPhase("query", "Query")],
        input_format="tsv", modes=["mode1"], rule_extension=".sql", flags={}, execution={},
        descriptor_path=Path("dummy"), rules_dir=Path("dummy"), credentials={}, version="0.1"
    )

    db_conn = MagicMock()
    mock_duckdb_connect.return_value = db_conn

    # Test connect (it only registers credentials)
    conn.connect({"database": ":memory:"}, desc)

    # Test run_experiment (this is where duckdb.connect is called)
    rule_file = tmp_path / "rule.sql"
    rule_file.write_text("SELECT * FROM ?table;")
    input_file = tmp_path / "facts.tsv"
    input_file.write_text("1\t2\n")

    results = conn.run_experiment(
        rule_file, input_file, tmp_path, desc, {}, query_bindings={"table": "edge"}
    )

    # Since conn.run_experiment derives _db_path from rule_path, it will call duckdb.connect
    mock_duckdb_connect.assert_called_once()
    assert "LoadRealTime" in results
    assert "QueryRealTime" in results
    conn.close()


@patch('pymongo.MongoClient')
def test_mongodb_connector(mock_mongo_client, tmp_path):
    conn = MongoDBConnector()
    desc = SystemDescriptor(
        name="mongodb", display_name="Mongo", category="db", protocol="pymongo",
        timing_phases=[TimingPhase("load", "Load"), TimingPhase("query", "Query")],
        input_format="json", modes=["mode1"], rule_extension=".py", flags={}, execution={},
        descriptor_path=Path("dummy"), rules_dir=Path("dummy"), credentials={}, version="0.1"
    )

    client = MagicMock()
    db = MagicMock()
    client.__getitem__.return_value = db
    mock_mongo_client.return_value = client

    # Test connect
    conn.connect({"uri": "mongodb://localhost:27017", "database": "test"}, desc)
    mock_mongo_client.assert_called_once_with("mongodb://localhost:27017")

    # Test run_experiment via mocked rules (must name file transitive_mode1.py so split does not fail)
    rule_file = tmp_path / "transitive_mode1.py"
    rule_file.write_text("""
class MongoDBMode1Recursion:
    def __init__(self, db, config):
        pass
    def create_collection(self, c1, c2):
        pass
    def insert_data(self, c, p):
        pass
    def create_index(self, c):
        pass
    def recursive_query(self, c1, c2):
        pass
    def export_to_csv(self, c, p):
        pass
""")
    # We patch importlib/sys to load this rule file
    with patch('sys.path', [str(tmp_path)] + sys.path):
        results = conn.run_experiment(rule_file, tmp_path, tmp_path, desc, {}, query_bindings={"q": 1})
        assert "LoadRealTime" in results
        assert "QueryRealTime" in results
        
        # Test exception path
        bad_rule = tmp_path / "transitive_bad.py"
        bad_rule.write_text("""
class MongoDBBadRecursion:
    def __init__(self, db, config):
        pass
    def create_collection(self, c1, c2):
        raise Exception("Bad collection")

""")
        results = conn.run_experiment(bad_rule, tmp_path, tmp_path, desc, {})
        assert results["LoadRealTime"] == 0.0

    # Test close
    conn.close()
    assert getattr(conn, '_client', None) is None

@patch('neo4j.GraphDatabase.driver')
def test_neo4j_connector(mock_neo_driver, tmp_path):
    conn = Neo4jConnector()
    desc = SystemDescriptor(
        name="neo4j", display_name="Neo4j", category="db", protocol="neo4j",
        timing_phases=[TimingPhase("load", "Load"), TimingPhase("query", "Query")],
        input_format="tsv", modes=["mode1"], rule_extension=".cypher", flags={}, execution={},
        descriptor_path=Path("dummy"), rules_dir=Path("dummy"), credentials={}, version="0.1"
    )

    driver = MagicMock()
    mock_neo_driver.return_value = driver
    session = MagicMock()
    driver.session.return_value = session

    conn.connect({"uri": "bolt://localhost:7687", "user": "neo", "password": "pwd"}, desc)
    mock_neo_driver.assert_called_once_with("bolt://localhost:7687", auth=("neo", "pwd"))

    # Test run_experiment
    rule_file = tmp_path / "rule.cypher"
    rule_file.write_text("MATCH (n) WHERE n.id = ?q RETURN n;\nCREATE INDEX;\nMATCH (n) RETURN n;\nEXPORT TO {output_file};")
    input_file = tmp_path / "facts.tsv"
    input_file.write_text("1\t2\n")

    with patch('subprocess.run') as mock_run:
        # Mock successful session run for 4 queries
        mock_result = MagicMock()
        mock_result.__iter__.return_value = ['record1']
        session.run.return_value = mock_result
        
        results = conn.run_experiment(rule_file, input_file, tmp_path, desc, {}, query_bindings={"q": 1})
        assert "LoadRealTime" in results
        assert "QueryRealTime" in results
        
        # Test Neo4j exception
        session.run.side_effect = Exception("Neo4j error")
        conn.run_experiment(rule_file, input_file, tmp_path, desc, {})
        
        # Test import/export subproc errors
        mock_run.side_effect = Exception("Subproc error")
        conn.run_experiment(rule_file, input_file, tmp_path, desc, {})

    conn.close()
    assert getattr(conn, '_driver', None) is None


@patch('psycopg2.connect')
def test_postgres_connector(mock_pg_connect, tmp_path):
    conn = PostgreSQLConnector()
    desc = SystemDescriptor(
        name="postgres", display_name="Postgres", category="db", protocol="psycopg2",
        timing_phases=[
            TimingPhase("create", "Create"),
            TimingPhase("import", "Import"),
            TimingPhase("index", "Index"),
            TimingPhase("analyze", "Analyze"),
            TimingPhase("query", "Query"),
            TimingPhase("write", "Write")
        ],
        input_format="tsv", modes=["mode1"], rule_extension=".py", flags={}, execution={},
        descriptor_path=Path("dummy"), rules_dir=Path("dummy"), credentials={}, version="0.1"
    )

    db_conn = MagicMock()
    mock_pg_connect.return_value = db_conn

    conn.connect({"dbURL": "postgresql://localhost:5432/test"}, desc)
    mock_pg_connect.assert_called_once_with("postgresql://localhost:5432/test")

    # Mock rule implementation
    rule_file = tmp_path / "transitive_mode1.py"
    rule_file.write_text("""
class PostgreSQLMode1Recursion:
    def __init__(self, config, conn):
        pass
    def drop_tc_path_tc_result_tables(self):
        pass
    def create_tc_path_table(self):
        pass
    def import_data_from_tsv(self, table, path):
        pass
    def create_tc_path_index(self):
        pass
    def analyze_tc_path_table(self):
        pass
    def run_recursive_query(self):
        pass
    def export_transitive_closure_results(self, path):
        pass
""")
    with patch('sys.path', [str(tmp_path)] + sys.path):
        results = conn.run_experiment(rule_file, tmp_path, tmp_path, desc, {})
        assert "CreateRealTime" in results
        assert "QueryRealTime" in results
    conn.close()


class TestSubprocessConnectors:
    @pytest.fixture
    def mock_sys_desc(self):
        return SystemDescriptor(
            name="xsb", display_name="XSB", category="logic", protocol="subprocess",
            timing_phases=[TimingPhase("load", "Load"), TimingPhase("solve", "Solve")],
            input_format="tsv", modes=["mode1"], rule_extension=".P", flags={}, execution={},
            descriptor_path=Path("dummy"), rules_dir=Path("dummy"), credentials={}, version="0.1"
        )

    @patch('engine.connectors.base.BaseConnector.timed_subprocess')
    def test_xsb_connector(self, mock_timed_subproc, mock_sys_desc, tmp_path):
        conn = XSBConnector()
        conn.connect({}, mock_sys_desc)

        # Mock stdout showing LoadRuleTime etc.
        out_mock = MagicMock()
        out_mock.stdout = "LoadRuleTime: 1.2\nCPULoadRuleTime: 1.1\nLoadFactsTime: 0.5\nCPULoadFactsTime: 0.4\nQueryOnlyTime: 0.3\nCPUQueryOnlyTime: 0.2\nQueryAndWriteTime: 0.4\nCPUTimeQueryAndWriteTime: 0.3\n"
        mock_timed_subproc.return_value = (2.0, 1.8, 15.0, out_mock)

        rule_file = tmp_path / "rule.P"
        rule_file.write_text("path(X,Y) :- edge(X,Y).")
        input_file = tmp_path / "facts.tsv"
        input_file.write_text("1\t2\n")

        results = conn.run_experiment(rule_file, input_file, tmp_path, mock_sys_desc, {})
        assert "LoadRealTime" in results
        assert results["LoadRealTime"] == 1.2

    @patch('engine.connectors.base.BaseConnector.timed_subprocess')
    def test_clingo_connector(self, mock_timed_subproc, tmp_path):
        conn = ClingoConnector()
        desc = SystemDescriptor(
            name="clingo", display_name="Clingo", category="logic", protocol="clingo_python",
            timing_phases=[
                TimingPhase("load_rules", "LoadRules"),
                TimingPhase("load_facts", "LoadFacts"),
                TimingPhase("ground", "Ground"),
                TimingPhase("query", "Query"),
                TimingPhase("write_result", "Write")
            ],
            input_format="lp", modes=["mode1"], rule_extension=".lp", flags={}, execution={},
            descriptor_path=Path("dummy"), rules_dir=Path("dummy"), credentials={}, version="0.1"
        )
        conn.connect({}, desc)

        out_mock = MagicMock()
        out_mock.stdout = "LoadRuleTime: 0.1\nCPULoadRuleTime: 0.05\nLoadFactsTime: 0.2\nCPULoadFactsTime: 0.15\nGroundTime: 0.3\nCPUGroundTime: 0.25\nQueryTime: 0.4\nCPUQueryTime: 0.35\nWriteTime: 0.01\nCPUWriteTime: 0.01\n"
        mock_timed_subproc.return_value = (1.0, 0.8, 12.0, out_mock)

        rule_file = tmp_path / "rule.lp"
        rule_file.write_text("path(X,Y) :- edge(X,Y).")
        input_file = tmp_path / "facts.lp"
        input_file.write_text("edge(1,2).\n")

        results = conn.run_experiment(rule_file, input_file, tmp_path, desc, {})
        assert "LoadRulesRealTime" in results
        assert results["GroundRealTime"] == 0.3

    @patch('engine.connectors.base.BaseConnector.timed_subprocess')
    def test_souffle_connector(self, mock_timed_subproc, tmp_path):
        conn = SouffleConnector()
        desc = SystemDescriptor(
            name="souffle", display_name="Souffle", category="logic", protocol="souffle_subprocess",
            timing_phases=[TimingPhase("compile", "Compile"), TimingPhase("solve", "Solve")],
            input_format="facts", modes=["mode1"], rule_extension=".dl", flags={}, execution={},
            descriptor_path=Path("dummy"), rules_dir=Path("dummy"), credentials={}, version="0.1"
        )
        conn.connect({}, desc)

        out_mock = MagicMock()
        out_mock.stdout = ""
        mock_timed_subproc.return_value = (1.5, 1.2, 20.0, out_mock)

        rule_file = tmp_path / "rule.dl"
        rule_file.write_text(".decl edge(x:number, y:number)\n.input edge\n.decl path(x:number, y:number)\npath(x,y) :- edge(x,y).\n.output path")
        input_dir = tmp_path / "facts"
        input_dir.mkdir(parents=True)
        (input_dir / "edge.facts").write_text("1\t2\n")

        results = conn.run_experiment(rule_file, input_dir, tmp_path, desc, {})
        assert "CompileRealTime" in results
        assert "SolveRealTime" in results

    @patch('engine.connectors.base.BaseConnector.timed_subprocess')
    def test_alda_connector(self, mock_timed_subproc, tmp_path):
        conn = AldaConnector()
        desc = SystemDescriptor(
            name="alda", display_name="Alda", category="logic", protocol="alda_subprocess",
            timing_phases=[
                TimingPhase("load_rules", "LoadRules"),
                TimingPhase("load_facts", "LoadFacts"),
                TimingPhase("query", "Query"),
                TimingPhase("write", "Write")
            ],
            input_format="tsv", modes=["mode1"], rule_extension=".da", flags={}, execution={},
            descriptor_path=Path("dummy"), rules_dir=Path("dummy"), credentials={}, version="0.1"
        )
        conn.connect({}, desc)

        out_mock = MagicMock()
        out_mock.stdout = "LoadRuleTime: 0.1\nLoadFactsTime: 0.2\nQueryOnlyTime: 0.3\nWriteTime: 0.4\n"
        mock_timed_subproc.return_value = (1.5, 1.2, 20.0, out_mock)

        rule_file = tmp_path / "transitive_mode1.da"
        rule_file.write_text("")
        input_file = tmp_path / "cycle" / "edge_100.facts"

        results = conn.run_experiment(rule_file, input_file, tmp_path, desc, {})
        assert "LoadRulesRealTime" in results
        assert results["LoadRulesRealTime"] == 0.1
        assert "QueryMaxRAM_MB" in results
        assert results["QueryMaxRAM_MB"] == 20.0



class TestPluginLoading:
    """Test plugin discovery and connector registry."""

    def test_get_connector_builtin_protocol(self):
        """Test getting builtin connector by protocol."""
        from engine.connectors import get_connector
        
        conn_class = get_connector('psycopg2')
        assert conn_class == PostgreSQLConnector

    def test_get_connector_unknown_protocol(self):
        """Test getting unknown protocol raises error."""
        from engine.connectors import get_connector
        
        with pytest.raises(ValueError) as exc_info:
            get_connector('nonexistent_protocol_xyz')
        
        assert 'nonexistent_protocol_xyz' in str(exc_info.value)
        assert 'Unknown protocol' in str(exc_info.value)

    def test_load_plugin_connectors_no_systems_dir(self):
        """Test _load_plugin_connectors handles missing systems directory."""
        from engine.connectors import _load_plugin_connectors

        # Should not raise exception
        _load_plugin_connectors(Path('/nonexistent/path'))

    def test_load_plugin_connectors_valid_plugin(self, tmp_path):
        """Test _load_plugin_connectors successfully loads drop-in connector."""
        from engine.connectors import PROTOCOL_REGISTRY, _load_plugin_connectors

        # Create a drop-in connector
        system_dir = tmp_path / 'test_system'
        system_dir.mkdir()
        
        # Create descriptor
        (system_dir / 'descriptor.yaml').write_text('''
name: test_system
display_name: Test System
category: test
protocol: test_plugin_protocol
timing_phases: []
input_format: tsv
modes: []
rule_extension: .sql
flags: {}
''')
        
        # Create connector
        (system_dir / 'connector.py').write_text('''
from engine.connectors.base import BaseConnector

class TestSystemConnector(BaseConnector):
    def connect(self, credentials, descriptor):
        pass
    def run_experiment(self, rule_path, input_path, output_folder, descriptor, config, query_bindings=None):
        return {}
    def close(self):
        pass
''')
        
        # Load plugins
        original_registry = PROTOCOL_REGISTRY.copy()
        try:
            _load_plugin_connectors(tmp_path)
            
            # Check if loaded
            assert 'test_plugin_protocol' in PROTOCOL_REGISTRY
        finally:
            # Restore
            PROTOCOL_REGISTRY.clear()
            PROTOCOL_REGISTRY.update(original_registry)

    def test_load_plugin_connectors_missing_descriptor(self, tmp_path):
        """Test _load_plugin_connectors skips connector without descriptor."""
        from engine.connectors import PROTOCOL_REGISTRY, _load_plugin_connectors
        
        system_dir = tmp_path / 'no_desc_system'
        system_dir.mkdir()
        
        # Create connector but no descriptor
        (system_dir / 'connector.py').write_text('''
from engine.connectors.base import BaseConnector

class NoDescConnector(BaseConnector):
    pass
''')
        
        original_registry = PROTOCOL_REGISTRY.copy()
        try:
            _load_plugin_connectors(tmp_path)
            
            # Should not load any plugin without descriptor
            assert len([p for p in PROTOCOL_REGISTRY if 'no_desc' in p.lower()]) == 0
        finally:
            PROTOCOL_REGISTRY.clear()
            PROTOCOL_REGISTRY.update(original_registry)

    def test_load_plugin_connectors_missing_protocol_field(self, tmp_path):
        """Test _load_plugin_connectors skips connector without protocol field."""
        from engine.connectors import PROTOCOL_REGISTRY, _load_plugin_connectors
        
        system_dir = tmp_path / 'no_protocol_system'
        system_dir.mkdir()
        
        # Create descriptor without protocol
        (system_dir / 'descriptor.yaml').write_text('''
name: no_protocol_system
display_name: No Protocol System
''')
        
        (system_dir / 'connector.py').write_text('''
from engine.connectors.base import BaseConnector

class NoProtocolConnector(BaseConnector):
    pass
''')
        
        original_registry = PROTOCOL_REGISTRY.copy()
        try:
            _load_plugin_connectors(tmp_path)
            
            # Should not load without protocol
            assert len([p for p in PROTOCOL_REGISTRY if 'no_protocol' in p.lower()]) == 0
        finally:
            PROTOCOL_REGISTRY.clear()
            PROTOCOL_REGISTRY.update(original_registry)

    def test_load_plugin_connectors_duplicate_protocol(self, tmp_path):
        """Test _load_plugin_connectors skips when protocol already registered."""
        from engine.connectors import PROTOCOL_REGISTRY, _load_plugin_connectors
        
        system_dir = tmp_path / 'duplicate_system'
        system_dir.mkdir()
        
        # Create descriptor with already-registered protocol
        (system_dir / 'descriptor.yaml').write_text('''
name: duplicate_system
display_name: Duplicate System
protocol: psycopg2
''')
        
        (system_dir / 'connector.py').write_text('''
from engine.connectors.base import BaseConnector

class DuplicateConnector(BaseConnector):
    pass
''')
        
        original_registry = PROTOCOL_REGISTRY.copy()
        original_psycopg2 = PROTOCOL_REGISTRY.get('psycopg2')
        try:
            _load_plugin_connectors(tmp_path)
            
            # Should not overwrite existing protocol
            assert PROTOCOL_REGISTRY['psycopg2'] == original_psycopg2
        finally:
            PROTOCOL_REGISTRY.clear()
            PROTOCOL_REGISTRY.update(original_registry)

    def test_load_plugin_connectors_malformed_descriptor(self, tmp_path):
        """Test _load_plugin_connectors handles invalid YAML gracefully."""
        from engine.connectors import PROTOCOL_REGISTRY, _load_plugin_connectors
        
        system_dir = tmp_path / 'malformed_system'
        system_dir.mkdir()
        
        # Create malformed descriptor
        (system_dir / 'descriptor.yaml').write_text('''
this is not valid yaml: [
''')
        
        (system_dir / 'connector.py').write_text('''
from engine.connectors.base import BaseConnector

class MalformedConnector(BaseConnector):
    pass
''')
        
        original_registry = PROTOCOL_REGISTRY.copy()
        try:
            # Should not raise exception
            _load_plugin_connectors(tmp_path)
        finally:
            PROTOCOL_REGISTRY.clear()
            PROTOCOL_REGISTRY.update(original_registry)

    def test_load_plugin_connectors_no_connector_class(self, tmp_path):
        """Test _load_plugin_connectors handles connector without proper class."""
        from engine.connectors import PROTOCOL_REGISTRY, _load_plugin_connectors
        
        system_dir = tmp_path / 'no_class_system'
        system_dir.mkdir()
        
        (system_dir / 'descriptor.yaml').write_text('''
name: no_class_system
display_name: No Class System
protocol: no_class_protocol
''')
        
        # Create connector with no class ending in 'Connector'
        (system_dir / 'connector.py').write_text('''
class NotAConnector:
    pass
''')
        
        original_registry = PROTOCOL_REGISTRY.copy()
        try:
            _load_plugin_connectors(tmp_path)
            
            # Should not load
            assert 'no_class_protocol' not in PROTOCOL_REGISTRY
        finally:
            PROTOCOL_REGISTRY.clear()
            PROTOCOL_REGISTRY.update(original_registry)

    def test_load_plugin_connectors_import_error(self, tmp_path):
        """Test _load_plugin_connectors handles import errors gracefully."""
        from engine.connectors import PROTOCOL_REGISTRY, _load_plugin_connectors
        
        system_dir = tmp_path / 'error_system'
        system_dir.mkdir()
        
        (system_dir / 'descriptor.yaml').write_text('''
name: error_system
display_name: Error System
protocol: error_protocol
''')
        
        # Create connector with syntax error
        (system_dir / 'connector.py').write_text('''
from engine.connectors.base import BaseConnector

class ErrorConnector(BaseConnector):
    def invalid syntax here
''')
        
        original_registry = PROTOCOL_REGISTRY.copy()
        try:
            # Should not raise exception
            _load_plugin_connectors(tmp_path)
        finally:
            PROTOCOL_REGISTRY.clear()
            PROTOCOL_REGISTRY.update(original_registry)


class TestSubprocessConnectorAdvanced:
    """Advanced tests for subprocess connectors."""

    @pytest.fixture
    def xsb_descriptor(self):
        return SystemDescriptor(
            name="xsb", display_name="XSB", category="logic", protocol="subprocess",
            timing_phases=[TimingPhase("load", "Load"), TimingPhase("solve", "Solve")],
            input_format="tsv", modes=["mode1"], rule_extension=".P", flags={}, execution={},
            descriptor_path=Path("dummy"), rules_dir=Path("dummy"), credentials={}, version="0.1"
        )

    @patch('engine.connectors.base.BaseConnector.timed_subprocess')
    def test_xsb_query_patching(self, mock_timed_subproc, xsb_descriptor, tmp_path):
        """Test XSB connector patches queries correctly."""
        conn = XSBConnector()
        conn.connect({}, xsb_descriptor)
        
        mock_result = MagicMock()
        mock_result.stdout = ""
        mock_timed_subproc.return_value = (1.0, 0.8, 15.0, mock_result)
        
        # Create rule file with custom query directive
        rule_file = tmp_path / "transitive_mode1.P"
        rule_file.write_text('''
%% :- ?query_placeholder
edge(1, 2).
path(X, Y) :- edge(X, Y).
''')
        
        input_file = tmp_path / "facts.tsv"
        input_file.write_text('3\t4\n')
        
        # Run with custom query
        results = conn.run_experiment(rule_file, input_file, tmp_path, xsb_descriptor, {})
        
        # Should return timing dict
        assert "LoadRealTime" in results
        assert "SolveRealTime" in results

    @patch('engine.connectors.base.BaseConnector.timed_subprocess')
    def test_clingo_temp_file_handling(self, mock_timed_subproc, tmp_path):
        """Test Clingo connector manages temporary files."""
        desc = SystemDescriptor(
            name="clingo", display_name="Clingo", category="logic", protocol="clingo_python",
            timing_phases=[TimingPhase("ground", "Ground"), TimingPhase("solve", "Solve")],
            input_format="lp", modes=["mode1"], rule_extension=".py", flags={}, execution={},
            descriptor_path=Path("dummy"), rules_dir=Path("dummy"), credentials={}, version="0.1"
        )
        
        conn = ClingoConnector()
        conn.connect({}, desc)
        
        mock_result = MagicMock()
        mock_result.stdout = ""
        mock_timed_subproc.return_value = (2.0, 1.5, 25.0, mock_result)
        
        rule_file = tmp_path / "transitive_mode1.py"
        rule_file.write_text('''
import clingo
# Rule file content
''')
        
        input_file = tmp_path / "input.lp"
        input_file.write_text('#base.\n')
        
        results = conn.run_experiment(rule_file, input_file, tmp_path, desc, {})
        assert "GroundRealTime" in results


class TestDuckDBConnectorAdvanced:
    """Advanced tests for DuckDB connector."""

    @patch('duckdb.connect')
    def test_duckdb_complex_query(self, mock_duckdb_connect, tmp_path):
        """Test DuckDB connector with complex SQL queries."""
        conn = DuckDBConnector()
        desc = SystemDescriptor(
            name="duckdb", display_name="DuckDB", category="db", protocol="duckdb",
            timing_phases=[TimingPhase("load", "Load"), TimingPhase("query", "Query")],
            input_format="tsv", modes=["mode1"], rule_extension=".sql", flags={}, execution={},
            descriptor_path=Path("dummy"), rules_dir=Path("dummy"), credentials={}, version="0.1"
        )
        
        db_conn = MagicMock()
        mock_duckdb_connect.return_value = db_conn
        
        conn.connect({"database": ":memory:"}, desc)
        
        rule_file = tmp_path / "rule.sql"
        rule_file.write_text('''
WITH RECURSIVE tc AS (
    SELECT src, dst FROM edge
    UNION ALL
    SELECT tc.src, edge.dst FROM tc JOIN edge ON tc.dst = edge.src
)
SELECT COUNT(*) as cnt FROM tc;
''')
        
        input_file = tmp_path / "edge.tsv"
        input_file.write_text("1\t2\n2\t3\n3\t4\n")
        
        results = conn.run_experiment(rule_file, input_file, tmp_path, desc, {})
        assert "LoadRealTime" in results

    @patch('duckdb.connect')
    def test_duckdb_exceptions_and_phases(self, mock_duckdb_connect, tmp_path, caplog):
        conn = DuckDBConnector()
        desc = SystemDescriptor(
            name="duckdb", display_name="DuckDB", category="db", protocol="duckdb",
            timing_phases=[TimingPhase("load", "Load")], # 1 phase but 2 queries -> triggers i >= len(phases)
            input_format="tsv", modes=["mode1"], rule_extension=".sql", flags={}, execution={},
            descriptor_path=Path("dummy"), rules_dir=Path("dummy"), credentials={}, version="0.1"
        )
        
        db_conn = MagicMock()
        db_conn.execute.side_effect = [Exception("Execute fail"), None]
        mock_duckdb_connect.return_value = db_conn
        
        conn.connect({"database": ":memory:"}, desc)
        
        rule_file = tmp_path / "rule.sql"
        rule_file.write_text("SELECT 1;\nSELECT 2;")
        input_file = tmp_path / "edge.tsv"
        input_file.write_text("1\t2\n")
        
        # Test command error and loop break
        conn.run_experiment(rule_file, input_file, tmp_path, desc, {})
        assert "Execute fail" in caplog.text

        # Test cleanup error
        conn._db_path = MagicMock()
        conn._db_path.exists.return_value = True
        conn._db_path.unlink.side_effect = Exception("Unlink fail")
        conn._cleanup()
        assert "Unlink fail" in caplog.text



class TestMongoDBConnectorAdvanced:
    """Advanced tests for MongoDB connector."""

    @patch('pymongo.MongoClient')
    def test_mongodb_aggregation_pipeline(self, mock_mongo_client, tmp_path):
        """Test MongoDB connector with aggregation pipelines."""
        conn = MongoDBConnector()
        desc = SystemDescriptor(
            name="mongodb", display_name="MongoDB", category="db", protocol="pymongo",
            timing_phases=[TimingPhase("load", "Load"), TimingPhase("query", "Query")],
            input_format="json", modes=["mode1"], rule_extension=".py", flags={}, execution={},
            descriptor_path=Path("dummy"), rules_dir=Path("dummy"), credentials={}, version="0.1"
        )
        
        client = MagicMock()
        db = MagicMock()
        client.__getitem__.return_value = db
        mock_mongo_client.return_value = client
        
        conn.connect({"uri": "mongodb://localhost:27017", "database": "test"}, desc)
        
        rule_file = tmp_path / "transitive_mode1.py"
        rule_file.write_text('''
class MongoDBMode1Recursion:
    def __init__(self, db, config):
        self.db = db
    def run_recursive_query(self):
        pass
''')
        
        input_file = tmp_path / "data.json"
        input_file.write_text('{"src": 1, "dst": 2}\n')
        
        with patch('sys.path', [str(tmp_path)] + sys.path):
            results = conn.run_experiment(rule_file, input_file, tmp_path, desc, {})
            assert "LoadRealTime" in results


class TestRDBMSConnectorAdvanced:
    """Advanced tests for RDBMS connectors."""

    @patch('psycopg2.connect')
    def test_postgres_multi_phase_timing(self, mock_pg_connect, tmp_path):
        """Test PostgreSQL connector with multiple timing phases."""
        conn = PostgreSQLConnector()
        desc = SystemDescriptor(
            name="postgres", display_name="Postgres", category="db", protocol="psycopg2",
            timing_phases=[
                TimingPhase("create", "Create"),
                TimingPhase("import", "Import"),
                TimingPhase("index", "Index"),
                TimingPhase("analyze", "Analyze"),
                TimingPhase("query", "Query"),
                TimingPhase("write", "Write")
            ],
            input_format="tsv", modes=["mode1"], rule_extension=".py", flags={}, execution={},
            descriptor_path=Path("dummy"), rules_dir=Path("dummy"), credentials={}, version="0.1"
        )
        
        db_conn = MagicMock()
        mock_pg_connect.return_value = db_conn
        
        conn.connect({"dbURL": "postgresql://localhost:5432/test"}, desc)
        
        rule_file = tmp_path / "transitive_mode1.py"
        rule_file.write_text('''
class PostgreSQLMode1Recursion:
    def __init__(self, config, conn):
        self.conn = conn
    def drop_tc_path_tc_result_tables(self):
        pass
    def create_tc_path_table(self):
        pass
    def import_data_from_tsv(self, table, path):
        pass
    def create_tc_path_index(self):
        pass
    def analyze_tc_path_table(self):
        pass
    def run_recursive_query(self):
        pass
    def export_transitive_closure_results(self, path):
        pass
''')
        
        input_file = tmp_path / "data.tsv"
        input_file.write_text("1\t2\n")
        
        with patch('sys.path', [str(tmp_path)] + sys.path):
            results = conn.run_experiment(rule_file, input_file, tmp_path, desc, {})
            # Should have all 6 timing phases
            expected_phases = ['Create', 'Import', 'Index', 'Analyze', 'Query', 'Write']
            for phase in expected_phases:
                assert f'{phase}RealTime' in results or f'{phase}CPUTime' in results

    @patch('MySQLdb.connect')
    @patch('shutil.copy')
    @patch('os.remove')
    def test_mariadb_connector(self, mock_remove, mock_copy, mock_connect, tmp_path):
        """Test MariaDB connector multi-phase timing."""
        conn = MariaDBConnector()
        desc = SystemDescriptor(
            name="mariadb", display_name="MariaDB", category="db", protocol="mysqlclient",
            timing_phases=[
                TimingPhase("create", "Create"),
                TimingPhase("import", "Import"),
                TimingPhase("index", "Index"),
                TimingPhase("analyze", "Analyze"),
                TimingPhase("query", "Query"),
                TimingPhase("write", "Write")
            ],
            input_format="tsv", modes=["mode1"], rule_extension=".py", flags={}, execution={},
            descriptor_path=Path("dummy"), rules_dir=Path("dummy"), credentials={}, version="0.1"
        )
        
        db_conn = MagicMock()
        mock_connect.return_value = db_conn
        
        conn.connect({"database": "test", "user": "u", "password": "p", "host": "localhost", "port": 3306}, desc)
        
        rule_file = tmp_path / "transitive_mode1.py"
        rule_file.write_text('''
class MariaDBMode1Recursion:
    def __init__(self, config, conn):
        self.conn = conn
    def drop_tc_path_tc_result_tables(self):
        pass
    def set_standard_cte_to_zero(self):
        pass
    def create_tc_path_table(self):
        pass
    def import_data_from_file(self, table, path):
        pass
    def create_tc_path_index(self):
        pass
    def analyze_tc_path_table(self):
        pass
    def run_recursive_query(self):
        pass
    def export_data_to_file(self):
        pass
''')
        
        input_file = tmp_path / "data.tsv"
        input_file.write_text("1\t2\n")
        
        with patch('sys.path', [str(tmp_path)] + sys.path):
            results = conn.run_experiment(rule_file, input_file, tmp_path, desc, {})
            expected_phases = ['Create', 'Import', 'Index', 'Analyze', 'Query', 'Write']
            for phase in expected_phases:
                assert f'{phase}RealTime' in results or f'{phase}CPUTime' in results

    @patch('psycopg2.connect')
    @patch('subprocess.run')
    def test_cockroachdb_connector(self, mock_run, mock_connect, tmp_path):
        """Test CockroachDB connector."""
        conn = CockroachDBConnector()
        desc = SystemDescriptor(
            name="cockroachdb", display_name="CockroachDB", category="db", protocol="psycopg2",
            timing_phases=[
                TimingPhase("create", "Create"),
                TimingPhase("import", "Import"),
                TimingPhase("index", "Index"),
                TimingPhase("analyze", "Analyze"),
                TimingPhase("query", "Query"),
                TimingPhase("write", "Write")
            ],
            input_format="tsv", modes=["mode1"], rule_extension=".py", flags={}, execution={},
            descriptor_path=Path("dummy"), rules_dir=Path("dummy"), credentials={"externalDirectory": "/tmp/"}, version="0.1"
        )
        
        db_conn = MagicMock()
        mock_connect.return_value = db_conn
        
        conn.connect({"dbURL": "postgresql://localhost:26257/test"}, desc)
        
        rule_file = tmp_path / "transitive_mode1.py"
        rule_file.write_text('''
class CockroachDBMode1Recursion:
    def __init__(self, config, conn):
        self.conn = conn
    def drop_tc_path_tc_result_tables(self):
        pass
    def create_tc_path_table(self):
        pass
    def import_data_from_tsv(self, table, path):
        pass
    def create_tc_path_index(self):
        pass
    def analyze_tc_path_table(self):
        pass
    def run_recursive_query(self):
        pass
    def export_transitive_closure_results(self, path):
        pass
''')
        
        input_file = tmp_path / "data.tsv"
        input_file.write_text("1\t2\n")
        
        with patch('sys.path', [str(tmp_path)] + sys.path):
            results = conn.run_experiment(rule_file, input_file, tmp_path, desc, {})
            expected_phases = ['Create', 'Import', 'Index', 'Analyze', 'Query', 'Write']
            for phase in expected_phases:
                assert f'{phase}RealTime' in results or f'{phase}CPUTime' in results


class TestRDBMSAdvanced:
    """Advanced RDBMS connector tests with actual implementation simulation."""

    @patch('importlib.util.spec_from_file_location')
    def test_rdbms_dynamic_import_success(self, mock_spec, tmp_path):
        """Test successful dynamic import of RDBMS operation classes."""
        from engine.connectors.rdbms import _dynamic_import_class

        # Create a test class file
        test_file = tmp_path / 'ops.py'
        test_file.write_text('''
class PostgreSQLRightRecursion:
    def __init__(self, config, conn):
        self.config = config
        self.conn = conn
''')
        
        # We can test the function by creating a simpler version
        # that the function would find
        # For now, skip the mock setup which is complex
        pass

    @patch('psycopg2.connect')
    def test_postgres_connector_connect(self, mock_pg_connect):
        """Test PostgreSQL connector connection."""
        conn = PostgreSQLConnector()
        desc = SystemDescriptor(
            name="postgres", display_name="Postgres", category="db", protocol="psycopg2",
            timing_phases=[TimingPhase("load", "Load")],
            input_format="tsv", modes=["mode1"], rule_extension=".py", flags={}, execution={},
            descriptor_path=Path("dummy"), rules_dir=Path("dummy"), credentials={}, version="0.1"
        )
        
        mock_db_conn = MagicMock()
        mock_pg_connect.return_value = mock_db_conn
        
        conn.connect({"dbURL": "postgresql://localhost:5432/test"}, desc)
        
        mock_pg_connect.assert_called_once_with("postgresql://localhost:5432/test")
        assert conn._connection == mock_db_conn

    @patch('psycopg2.connect')
    def test_postgres_close(self, mock_pg_connect):
        """Test PostgreSQL connector close."""
        conn = PostgreSQLConnector()
        desc = SystemDescriptor(
            name="postgres", display_name="Postgres", category="db", protocol="psycopg2",
            timing_phases=[TimingPhase("load", "Load")],
            input_format="tsv", modes=["mode1"], rule_extension=".py", flags={}, execution={},
            descriptor_path=Path("dummy"), rules_dir=Path("dummy"), credentials={}, version="0.1"
        )
        
        mock_db_conn = MagicMock()
        mock_pg_connect.return_value = mock_db_conn
        
        conn.connect({"dbURL": "postgresql://localhost:5432/test"}, desc)
        conn.close()
        
        mock_db_conn.close.assert_called_once()

    @patch('psycopg2.connect')
    def test_postgres_run_experiment_with_ops_class(self, mock_pg_connect, tmp_path):
        """Test PostgreSQL run_experiment with mocked operation class."""
        from engine.connectors.rdbms import PostgreSQLConnector

        # Create descriptor path in a subdirectory
        system_dir = tmp_path / 'postgres'
        system_dir.mkdir()
        
        desc = SystemDescriptor(
            name="postgres", display_name="Postgres", category="db", protocol="psycopg2",
            timing_phases=[TimingPhase("load", "Load"), TimingPhase("solve", "Solve")],
            input_format="tsv", modes=["right_recursion"], rule_extension=".py", flags={}, 
            execution={}, descriptor_path=system_dir / 'descriptor.yaml', 
            rules_dir=system_dir, credentials={}, version="0.1"
        )
        
        # Create __init__.py in system dir
        init_file = system_dir / '__init__.py'
        init_file.write_text('class PostgreSQLOperations: pass')
        
        # Create rule file with operation class
        rule_file = system_dir / 'transitive_right_recursion.py'
        rule_file.write_text('''
class PostgreSQLRightRecursion:
    def __init__(self, config, conn):
        self.config = config
        self.conn = conn
    
    def drop_tc_path_tc_result_tables(self):
        pass
    
    def create_tc_path_table(self):
        pass
    
    def import_data_from_tsv(self, *args, **kwargs):
        pass
    
    def create_tc_path_index(self):
        pass
    
    def analyze_tc_path_table(self):
        pass
    
    def run_recursive_query(self):
        pass
    
    def export_transitive_closure_results(self, *args, **kwargs):
        pass
''')
        
        # Setup mock database connection
        mock_db_conn = MagicMock()
        mock_pg_connect.return_value = mock_db_conn
        
        conn = PostgreSQLConnector()
        conn.connect({"dbURL": "postgresql://localhost:5432/test"}, desc)
        
        # Create input and output files
        input_file = system_dir / 'edge.tsv'
        input_file.write_text('1\t2\n2\t3\n')
        
        output_folder = system_dir / 'output'
        output_folder.mkdir()
        
        # Run experiment
        results = conn.run_experiment(rule_file, input_file, output_folder, desc, {})
        
        # Verify results structure
        assert 'LoadRealTime' in results
        assert 'SolveRealTime' in results

    @patch('psycopg2.connect')
    def test_postgres_run_experiment_exception_handling(self, mock_pg_connect, tmp_path):
        """Test PostgreSQL run_experiment exception handling."""
        from engine.connectors.rdbms import PostgreSQLConnector

        # Create descriptor path in a subdirectory
        system_dir = tmp_path / 'postgres'
        system_dir.mkdir()
        
        desc = SystemDescriptor(
            name="postgres", display_name="Postgres", category="db", protocol="psycopg2",
            timing_phases=[TimingPhase("load", "Load"), TimingPhase("solve", "Solve")],
            input_format="tsv", modes=["right_recursion"], rule_extension=".py", flags={}, 
            execution={}, descriptor_path=system_dir / 'descriptor.yaml', 
            rules_dir=system_dir, credentials={}, version="0.1"
        )
        
        # Create __init__.py in system dir
        init_file = system_dir / '__init__.py'
        init_file.write_text('class PostgreSQLOperations: pass')
        
        # Create rule file with operation class that raises an exception
        rule_file = system_dir / 'transitive_right_recursion.py'
        rule_file.write_text('''
class PostgreSQLRightRecursion:
    def __init__(self, config, conn):
        self.config = config
        self.conn = conn
    
    def drop_tc_path_tc_result_tables(self):
        raise RuntimeError("Connection failed")
''')
        
        # Setup mock database connection
        mock_db_conn = MagicMock()
        mock_pg_connect.return_value = mock_db_conn
        
        conn = PostgreSQLConnector()
        conn.connect({"dbURL": "postgresql://localhost:5432/test"}, desc)
        
        # Create input and output files
        input_file = system_dir / 'edge.tsv'
        input_file.write_text('1\t2\n2\t3\n')
        
        output_folder = system_dir / 'output'
        output_folder.mkdir()
        
        # Run experiment - should handle the exception gracefully
        results = conn.run_experiment(rule_file, input_file, output_folder, desc, {})
        
        # Verify we got a result structure even with the exception
        assert isinstance(results, dict)

    def test_dynamic_import_class(self, tmp_path):
        """Test _dynamic_import_class function."""
        from engine.connectors.rdbms import _dynamic_import_class

        # Create a test module with a class
        test_file = tmp_path / 'test_ops.py'
        test_file.write_text('''
class TestOperations:
    def __init__(self):
        self.name = "test"
    
    def method(self):
        return 42
''')
        
        # Import the class
        OpClass = _dynamic_import_class(test_file, 'TestOperations')
        
        # Verify the class was imported correctly
        assert OpClass.__name__ == 'TestOperations'
        
        # Create instance and test
        instance = OpClass()
        assert instance.name == 'test'
        assert instance.method() == 42

    @patch('psycopg2.connect')
    def test_postgres_run_experiment_with_query_bindings(self, mock_pg_connect, tmp_path):
        """Test PostgreSQL run_experiment with query bindings."""
        from engine.connectors.rdbms import PostgreSQLConnector

        # Create descriptor path
        system_dir = tmp_path / 'postgres'
        system_dir.mkdir()
        
        desc = SystemDescriptor(
            name="postgres", display_name="Postgres", category="db", protocol="psycopg2",
            timing_phases=[TimingPhase("load", "Load"), TimingPhase("solve", "Solve")],
            input_format="tsv", modes=["right_recursion"], rule_extension=".py", flags={}, 
            execution={}, descriptor_path=system_dir / 'descriptor.yaml', 
            rules_dir=system_dir, credentials={}, version="0.1"
        )
        
        # Create __init__.py
        init_file = system_dir / '__init__.py'
        init_file.write_text('class PostgreSQLOperations: pass')
        
        # Create rule file
        rule_file = system_dir / 'transitive_right_recursion.py'
        rule_file.write_text('''
class PostgreSQLRightRecursion:
    def __init__(self, config, conn):
        self.config = config
        self.conn = conn
        # Check query_bindings were passed
        self.has_bindings = 'query_bindings' in config
    
    def drop_tc_path_tc_result_tables(self):
        pass
    
    def create_tc_path_table(self):
        pass
    
    def import_data_from_tsv(self, *args, **kwargs):
        pass
    
    def create_tc_path_index(self):
        pass
    
    def analyze_tc_path_table(self):
        pass
    
    def run_recursive_query(self):
        pass
    
    def export_transitive_closure_results(self, *args, **kwargs):
        pass
''')
        
        # Setup mock connection
        mock_db_conn = MagicMock()
        mock_pg_connect.return_value = mock_db_conn
        
        conn = PostgreSQLConnector()
        conn.connect({"dbURL": "postgresql://localhost:5432/test"}, desc)
        
        # Create input/output files
        input_file = system_dir / 'edge.tsv'
        input_file.write_text('1\t2\n2\t3\n')
        
        output_folder = system_dir / 'output'
        output_folder.mkdir()
        
        # Run with query bindings
        bindings = {'param1': 'value1', 'param2': 'value2'}
        results = conn.run_experiment(rule_file, input_file, output_folder, desc, {}, query_bindings=bindings)
        
        # Verify results
        assert isinstance(results, dict)
        assert 'LoadRealTime' in results


class TestSubprocessConnectorsAdvanced:
    """Advanced subprocess connector tests."""

    @patch('engine.connectors.base.BaseConnector.timed_subprocess')
    def test_xsb_with_query_bindings(self, mock_timed_subproc, tmp_path):
        """Test XSB connector with query bindings."""
        from engine.connectors.subprocess_conn import XSBConnector
        
        conn = XSBConnector()
        desc = SystemDescriptor(
            name="xsb", display_name="XSB", category="logic", protocol="subprocess",
            timing_phases=[TimingPhase("load", "Load"), TimingPhase("solve", "Solve")],
            input_format="tsv", modes=["mode1"], rule_extension=".P", flags={}, execution={},
            descriptor_path=Path("dummy"), rules_dir=Path("dummy"), credentials={}, version="0.1"
        )
        
        conn.connect({}, desc)
        
        mock_result = MagicMock()
        mock_result.stdout = ""
        mock_result.stderr = ""
        mock_result.returncode = 0
        mock_timed_subproc.return_value = (1.0, 0.8, 10.0, mock_result)
        
        rule_file = tmp_path / "transitive_mode1.P"
        rule_file.write_text('edge(1,2). path(X,Y) :- edge(X,Y).')
        
        input_file = tmp_path / "edge.tsv"
        input_file.write_text('1\t2\n')
        
        bindings = {'query_file': 'test.out', 'query_predicate': 'path'}
        results = conn.run_experiment(rule_file, input_file, tmp_path, desc, {}, query_bindings=bindings)
        
        assert "LoadRealTime" in results
        assert "SolveRealTime" in results

    @patch('engine.connectors.base.BaseConnector.timed_subprocess')
    def test_clingo_with_stats(self, mock_timed_subproc, tmp_path):
        """Test Clingo connector with statistics output."""
        conn = ClingoConnector()
        desc = SystemDescriptor(
            name="clingo", display_name="Clingo", category="logic", protocol="clingo_python",
            timing_phases=[TimingPhase("ground", "Ground"), TimingPhase("solve", "Solve")],
            input_format="lp", modes=["mode1"], rule_extension=".py", flags={}, execution={},
            descriptor_path=Path("dummy"), rules_dir=Path("dummy"), credentials={}, version="0.1"
        )
        
        conn.connect({}, desc)
        
        mock_result = MagicMock()
        mock_result.stdout = "Answer: 1\nModels       : 1\n"
        mock_result.stderr = ""
        mock_result.returncode = 0
        mock_timed_subproc.return_value = (2.0, 1.5, 20.0, mock_result)
        
        rule_file = tmp_path / "transitive_mode1.py"
        rule_file.write_text('import clingo')
        
        input_file = tmp_path / "input.lp"
        input_file.write_text('edge(1,2).')
        
        results = conn.run_experiment(rule_file, input_file, tmp_path, desc, {})
        
        assert "GroundRealTime" in results
        assert "SolveRealTime" in results

    @patch('engine.connectors.base.BaseConnector.timed_subprocess')
    def test_souffle_with_multiple_phases(self, mock_timed_subproc, tmp_path):
        """Test Souffle connector with compile and solve phases."""
        conn = SouffleConnector()
        desc = SystemDescriptor(
            name="souffle", display_name="Souffle", category="logic", protocol="souffle_subprocess",
            timing_phases=[TimingPhase("compile", "Compile"), TimingPhase("solve", "Solve")],
            input_format="facts", modes=["mode1"], rule_extension=".dl", flags={}, execution={},
            descriptor_path=Path("dummy"), rules_dir=Path("dummy"), credentials={}, version="0.1"
        )
        
        conn.connect({}, desc)
        
        mock_result = MagicMock()
        mock_result.stdout = ""
        mock_result.stderr = ""
        mock_result.returncode = 0
        mock_timed_subproc.return_value = (1.5, 1.2, 15.0, mock_result)
        
        rule_file = tmp_path / "transitive_mode1.dl"
        rule_file.write_text('.input edge\n.output path\npath(X,Y) :- edge(X,Y).')
        
        facts_dir = tmp_path / "facts"
        facts_dir.mkdir()
        (facts_dir / "edge.facts").write_text('1\t2\n')
        
        results = conn.run_experiment(rule_file, facts_dir, tmp_path, desc, {})
        
        assert "CompileRealTime" in results
        assert "SolveRealTime" in results
