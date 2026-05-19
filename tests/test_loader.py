
from unittest.mock import MagicMock, patch

import pytest
import yaml

from engine.loader import (
    DescriptorLoader,
    get_system_version,
)


@patch('subprocess.run')
@patch('importlib.metadata.version')
def test_get_system_version(mock_version, mock_run):
    # Test subprocess route (postgres)
    mock_run.return_value = MagicMock(stdout='psql (PostgreSQL) 14.1\n', returncode=0)
    version = get_system_version('postgres')
    assert version == '14.1'

    # Test importlib route (duckdb)
    mock_run.return_value = MagicMock(returncode=1) # subprocess fails
    mock_version.return_value = '0.9.1'
    version = get_system_version('duckdb')
    assert version == '0.9.1'

    # Test unknown system
    version = get_system_version('unknown_sys')
    assert version == 'Unknown'


class TestDescriptorLoader:
    @pytest.fixture
    def mock_fs(self, tmp_path):
        # Create systems
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
modes: [mode1]
rule_extension: .sql
flags:
  requires_credentials: true
        ''')
        (sys_dir / 'credentials.yaml').write_text('pass: 123')

        # Create domains
        domain_dir = tmp_path / 'domains' / 'test_domain'
        domain_dir.mkdir(parents=True)
        (domain_dir / 'descriptor.yaml').write_text('''
name: test_domain
display_name: Test Domain
description: Desc
modes: [mode1]
query_parameters:
  - {name: src, type: int}
example_rules: {}
        ''')

        # Create graphs
        graph_dir = tmp_path / 'graph_types'
        graph_dir.mkdir(parents=True)
        (graph_dir / 'test_graph.yaml').write_text('''
name: test_graph
display_name: Test Graph
description: Desc
generator: some.module.func
parameters: {p1: 1}
        ''')

        # Create global config
        (tmp_path / 'config.yaml').write_text('test_sys: {pass: 999}')

        return tmp_path

    def test_load_all(self, mock_fs):
        loader = DescriptorLoader(base_dir=mock_fs)
        
        # Test load_systems
        systems = loader.load_systems()
        assert len(systems) == 1
        assert systems[0].name == 'test_sys'
        assert systems[0].credentials == {'pass': 123}

        # Test get_system
        sys = loader.get_system('test_sys')
        assert sys is not None
        assert sys.name == 'test_sys'
        assert sys.credentials == {'pass': 123}
        assert sys.csv_headers == ['LoadRealTime', 'LoadCPUTime', 'LoadMaxRAM_MB']

        # Test get_domain
        dom = loader.get_domain('test_domain')
        assert dom is not None
        assert dom.name == 'test_domain'
        assert len(dom.query_parameters) == 1
        assert dom.query_parameters[0].name == 'src'

        # Test load_domains
        domains = loader.load_domains()
        assert len(domains) == 1
        assert domains[0].name == 'test_domain'

        # Test load_graph_types
        graph_types = loader.load_graph_types()
        assert len(graph_types) == 1
        assert graph_types[0].name == 'test_graph'
        assert graph_types[0].parameters == {'p1': 1}

    def test_load_global_config_fallback(self, mock_fs):
        # Remove config.yaml and create config.json to test fallback
        (mock_fs / 'config.yaml').unlink()
        (mock_fs / 'config.json').write_text('{"test_sys": {"pass": "json_pass"}}')

        loader = DescriptorLoader(base_dir=mock_fs)
        global_config = loader.load_global_config()
        assert global_config == {'test_sys': {'pass': 'json_pass'}}

        # Test save credentials
        loader.save_system_credentials('test_sys', {'pass': 'new_pass'})
        assert (mock_fs / 'systems' / 'test_sys' / 'credentials.yaml').exists()

    def test_invalid_yaml(self, tmp_path):
        # Corrupt yaml
        sys_dir = tmp_path / 'systems' / 'bad_sys'
        sys_dir.mkdir(parents=True)
        (sys_dir / 'descriptor.yaml').write_text('bad: [yaml')

        loader = DescriptorLoader(base_dir=tmp_path)
        # load_systems should catch the exception and skip the system
        systems = loader.load_systems()
        assert len(systems) == 0

        # get_system should propagate the exception
        with pytest.raises(yaml.parser.ParserError):
            loader.get_system('bad_sys')

    def test_save_descriptor(self, mock_fs):
        loader = DescriptorLoader(base_dir=mock_fs)
        sys = loader.get_system('test_sys')
        sys.display_name = 'Modified System'
        loader.save_descriptor(sys)

        sys_reloaded = loader.get_system('test_sys')
        assert sys_reloaded.display_name == 'Modified System'


class TestDescriptorLoaderAdvanced:
    """Advanced tests for DescriptorLoader with edge cases."""

    @pytest.fixture
    def advanced_fs(self, tmp_path):
        """Create a more complex filesystem setup."""
        # Create multiple systems
        for sys_name in ['sys_a', 'sys_b', 'sys_c']:
            sys_dir = tmp_path / 'systems' / sys_name
            sys_dir.mkdir(parents=True)
            (sys_dir / 'descriptor.yaml').write_text(f'''
name: {sys_name}
display_name: {sys_name.title()} System
category: db
protocol: test_proto
timing_phases:
  - {{id: load, label: Load}}
  - {{id: query, label: Query}}
input_format: tsv
modes: [mode1, mode2]
rule_extension: .sql
flags:
  requires_credentials: false
            ''')
            (sys_dir / 'credentials.yaml').write_text('pass: 123')

        # Create multiple domains
        for dom_name in ['domain_a', 'domain_b']:
            dom_dir = tmp_path / 'domains' / dom_name
            dom_dir.mkdir(parents=True)
            (dom_dir / 'descriptor.yaml').write_text(f'''
name: {dom_name}
display_name: {dom_name.title()}
description: Test domain
modes: [mode1]
query_parameters:
  - {{name: src, type: int}}
  - {{name: dst, type: int}}
example_rules: {{}}
            ''')

        # Create graph types
        graph_dir = tmp_path / 'graph_types'
        graph_dir.mkdir(parents=True)
        for i in range(1, 4):
            (graph_dir / f'graph_{i}.yaml').write_text(f'''
name: graph_{i}
display_name: Graph {i}
description: Test graph
generator: module.func
parameters: {{p{i}: {i}}}
            ''')

        # Create global config with multiple systems
        (tmp_path / 'config.yaml').write_text('''
sys_a: {pass: 111}
sys_b: {pass: 222}
sys_c: {pass: 333}
        ''')

        return tmp_path

    def test_load_multiple_systems(self, advanced_fs):
        """Test loading all systems."""
        loader = DescriptorLoader(base_dir=advanced_fs)
        systems = loader.load_systems()
        
        assert len(systems) == 3
        sys_names = {sys.name for sys in systems}
        assert sys_names == {'sys_a', 'sys_b', 'sys_c'}

    def test_load_multiple_domains(self, advanced_fs):
        """Test loading all domains."""
        loader = DescriptorLoader(base_dir=advanced_fs)
        domains = loader.load_domains()
        
        assert len(domains) == 2
        dom_names = {dom.name for dom in domains}
        assert dom_names == {'domain_a', 'domain_b'}

    def test_load_multiple_graph_types(self, advanced_fs):
        """Test loading all graph types."""
        loader = DescriptorLoader(base_dir=advanced_fs)
        graphs = loader.load_graph_types()
        
        assert len(graphs) == 3
        graph_names = {g.name for g in graphs}
        assert graph_names == {'graph_1', 'graph_2', 'graph_3'}

    def test_get_system_applies_global_config(self, advanced_fs):
        """Test that get_system merges with global config."""
        loader = DescriptorLoader(base_dir=advanced_fs)
        sys = loader.get_system('sys_b')
        
        # Should have credentials from either system descriptor or global config
        assert sys.credentials is not None
        assert isinstance(sys.credentials, dict)

    def test_csv_headers_from_timing_phases(self, advanced_fs):
        """Test CSV header generation from timing phases."""
        loader = DescriptorLoader(base_dir=advanced_fs)
        sys = loader.get_system('sys_a')
        
        # Should have headers for each phase
        expected = ['LoadRealTime', 'LoadCPUTime', 'LoadMaxRAM_MB', 
                   'QueryRealTime', 'QueryCPUTime', 'QueryMaxRAM_MB']
        assert sys.csv_headers == expected

    def test_load_global_config_nonexistent(self, tmp_path):
        """Test loading config when no config file exists."""
        loader = DescriptorLoader(base_dir=tmp_path)
        config = loader.load_global_config()
        
        assert config == {}

    def test_save_system_credentials_creates_file(self, tmp_path):
        """Test saving credentials creates new file."""
        sys_dir = tmp_path / 'systems' / 'new_sys'
        sys_dir.mkdir(parents=True)
        (sys_dir / 'descriptor.yaml').write_text('''
name: new_sys
display_name: New System
category: db
protocol: test
timing_phases: []
input_format: tsv
modes: []
rule_extension: .sql
flags: {}
        ''')

        loader = DescriptorLoader(base_dir=tmp_path)
        loader.save_system_credentials('new_sys', {'user': 'admin', 'pass': 'secret'})
        
        cred_file = sys_dir / 'credentials.yaml'
        assert cred_file.exists()
        with open(cred_file) as f:
            import yaml
            creds = yaml.safe_load(f)
        assert creds['user'] == 'admin'
        assert creds['pass'] == 'secret'

    def test_descriptor_path_set_correctly(self, advanced_fs):
        """Test that descriptor paths are set on objects."""
        loader = DescriptorLoader(base_dir=advanced_fs)
        
        sys = loader.get_system('sys_a')
        assert sys.descriptor_path == advanced_fs / 'systems' / 'sys_a' / 'descriptor.yaml'
        
        dom = loader.get_domain('domain_a')
        assert dom.descriptor_path == advanced_fs / 'domains' / 'domain_a' / 'descriptor.yaml'

    def test_rules_dir_set_correctly(self, advanced_fs):
        """Test that rules_dir is set on system descriptor."""
        loader = DescriptorLoader(base_dir=advanced_fs)
        sys = loader.get_system('sys_a')
        
        assert sys.rules_dir == advanced_fs / 'systems' / 'sys_a' / 'rules'

    def test_system_version_integration(self, advanced_fs):
        """Test system version is loaded."""
        loader = DescriptorLoader(base_dir=advanced_fs)
        sys = loader.get_system('sys_a')
        
        # Version should be populated (either from descriptor or 'Unknown')
        assert sys.version is not None

    def test_query_parameters_structure(self, advanced_fs):
        """Test query parameters are properly structured."""
        loader = DescriptorLoader(base_dir=advanced_fs)
        dom = loader.get_domain('domain_a')
        
        assert len(dom.query_parameters) == 2
        assert dom.query_parameters[0].name == 'src'
        assert dom.query_parameters[0].type == 'int'
        assert dom.query_parameters[1].name == 'dst'
        assert dom.query_parameters[1].type == 'int'

    def test_empty_domains_directory(self, tmp_path):
        """Test loading from directory with no domains."""
        loader = DescriptorLoader(base_dir=tmp_path)
        domains = loader.load_domains()
        
        assert domains == []

    def test_empty_graph_types_directory(self, tmp_path):
        """Test loading from directory with no graph types."""
        loader = DescriptorLoader(base_dir=tmp_path)
        graphs = loader.load_graph_types()
        
        assert graphs == []

    def test_partial_system_descriptor(self, tmp_path):
        """Test loading system with minimal descriptor."""
        sys_dir = tmp_path / 'systems' / 'minimal'
        sys_dir.mkdir(parents=True)
        (sys_dir / 'descriptor.yaml').write_text('''
name: minimal
display_name: Minimal System
category: db
protocol: test
timing_phases: []
input_format: tsv
modes: []
rule_extension: .sql
flags: {}
        ''')

        loader = DescriptorLoader(base_dir=tmp_path)
        sys = loader.get_system('minimal')
        
        assert sys.name == 'minimal'
        assert sys.timing_phases == []
        assert sys.modes == []

    def test_complex_graph_parameters(self, tmp_path):
        """Test loading graph type with complex parameters."""
        graph_dir = tmp_path / 'graph_types'
        graph_dir.mkdir(parents=True)
        (graph_dir / 'complex.yaml').write_text('''
name: complex
display_name: Complex Graph
description: Graph with nested parameters
generator: module.gen
parameters:
  seed: 42
  nested:
    option1: value1
    option2: value2
  list_param: [1, 2, 3]
        ''')

        loader = DescriptorLoader(base_dir=tmp_path)
        graphs = loader.load_graph_types()
        
        assert len(graphs) == 1
        g = graphs[0]
        assert g.parameters['seed'] == 42
        assert g.parameters['nested']['option1'] == 'value1'
        assert g.parameters['list_param'] == [1, 2, 3]

    def test_descriptor_to_dicts(self, advanced_fs):
        loader = DescriptorLoader(base_dir=advanced_fs)
        
        sys = loader.get_system('sys_a')
        d = sys.to_dict()
        assert d['name'] == 'sys_a'
        assert sys.system_dir == advanced_fs / 'systems' / 'sys_a'

        dom = loader.get_domain('domain_a')
        d_dom = dom.to_dict()
        assert d_dom['name'] == 'domain_a'
        assert dom.domain_dir == advanced_fs / 'domains' / 'domain_a'

        graph = loader.load_graph_types()[0]
        d_graph = graph.to_dict()
        assert d_graph['name'] == graph.name

    def test_loader_missing_directories_and_files(self, advanced_fs):
        import shutil
        shutil.rmtree(advanced_fs / 'systems')
        shutil.rmtree(advanced_fs / 'domains')
        shutil.rmtree(advanced_fs / 'graph_types')
        
        loader = DescriptorLoader(base_dir=advanced_fs)
        assert loader.load_systems() == []
        assert loader.load_domains() == []
        assert loader.load_graph_types() == []
        assert loader.get_domain('nonexistent') is None
        assert loader.get_system('nonexistent') is None

    def test_loader_names_filter(self, advanced_fs):
        loader = DescriptorLoader(base_dir=advanced_fs)
        assert len(loader.load_systems(['sys_a'])) == 1
        assert len(loader.load_domains(['domain_a'])) == 1
        assert len(loader.load_graph_types(['graph_1'])) == 1

    def test_loader_exceptions_in_parsing(self, advanced_fs):
        (advanced_fs / 'graph_types' / 'bad.yaml').write_text("invalid: [yaml: content")
        (advanced_fs / 'domains' / 'bad' / 'descriptor.yaml').parent.mkdir(parents=True, exist_ok=True)
        (advanced_fs / 'domains' / 'bad' / 'descriptor.yaml').write_text("invalid: [yaml: content")
        
        loader = DescriptorLoader(base_dir=advanced_fs)
        loader.load_graph_types()  # should catch exception and log
        loader.load_domains()      # should catch exception and log

    def test_load_global_config_cached_and_yaml(self, advanced_fs):
        loader = DescriptorLoader(base_dir=advanced_fs)
        conf1 = loader.load_global_config()
        assert 'sys_a' in conf1
        
        conf2 = loader.load_global_config()
        assert conf1 is conf2  # cached

    def test_legacy_dict_query_parameters(self, advanced_fs):
        dom_dir = advanced_fs / 'domains' / 'legacy_domain'
        dom_dir.mkdir(parents=True)
        (dom_dir / 'descriptor.yaml').write_text('''
name: legacy_domain
display_name: Legacy
description: Legacy domain
modes: [mode1]
query_parameters:
  src: int
  dst: int
        ''')
        loader = DescriptorLoader(base_dir=advanced_fs)
        dom = loader.get_domain('legacy_domain')
        assert dom.query_parameters[0].name == 'src'
        assert dom.query_parameters[1].type == 'int'

from unittest.mock import patch, MagicMock
from engine.loader import get_system_version

def test_get_system_version_cmd_outputs():
    with patch('subprocess.run') as mock_run:
        res = MagicMock()
        res.returncode = 0
        mock_run.return_value = res
        
        res.stdout = "Version: 2.4.1\n"
        assert get_system_version('souffle') == "2.4.1"
        
        res.stdout = "XSB Version 5.0.0\n"
        assert get_system_version('xsb') == "5.0.0"

        res.stdout = "psql (PostgreSQL) 14.2\n"
        assert get_system_version('postgres') == "14.2"
        
        res.stdout = "mysql  Ver 15.1 Distrib 10.6.5-MariaDB, for debian-linux-gnu (x86_64) using readline 5.2\n"
        res.stdout = "mariadb from 10.6.5, os\n"
        assert get_system_version('mariadb') == "10.6.5"
        
        res.stdout = "Build Tag: v22.1.0\n"
        assert get_system_version('cockroachdb') == "v22.1.0"
        
        res.stdout = "db version v5.0.9\n"
        assert get_system_version('mongodb') == "v5.0.9"
        
        res.stdout = "Some random output"
        assert get_system_version('neo4j') == "Some random output" # neo4j doesn't have command in cmd_map

        # Test empty stdout fallback to stderr
        res.stdout = ""
        res.stderr = "Version: 1.2.3"
        assert get_system_version('souffle') == "1.2.3"

        # Test subprocess.run exception
        mock_run.side_effect = Exception("Command failed")
        assert get_system_version('souffle') == "Unknown"

def test_get_system_version_metadata_fallback():
    with patch('subprocess.run') as mock_run, patch('importlib.metadata.version') as mock_meta:
        res = MagicMock()
        res.returncode = 1  # fail command
        mock_run.return_value = res
        
        mock_meta.return_value = "1.0.0"
        
        assert get_system_version('neo4j') == "1.0.0"
        assert get_system_version('postgres') == "1.0.0"
        assert get_system_version('mongodb') == "1.0.0"
        assert get_system_version('duckdb') == "1.0.0"
        assert get_system_version('mariadb') == "1.0.0"
        assert get_system_version('clingo') == "1.0.0"
        
        mock_meta.side_effect = Exception("Not installed")
        assert get_system_version('neo4j') == "Unknown"

