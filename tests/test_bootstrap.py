import pytest
import yaml

from engine.bootstrap import BootstrapManager


class TestBootstrapManager:
    @pytest.fixture
    def mock_bootstrap_fs(self, tmp_path):
        # Create directories
        templates_dir = tmp_path / 'templates'
        templates_dir.mkdir(parents=True)
        
        # Create a mock system template
        (templates_dir / 'descriptor_sql_database.yaml').write_text('''
name: template_sys
display_name: Template Database
category: db
protocol: test_proto
timing_phases: []
input_format: tsv
modes: []
rule_extension: .sql
flags: {}
        ''')

        # Create a mock domain template
        (templates_dir / 'domain_shortest_path.yaml').write_text('''
name: template_domain
display_name: Template Domain
description: Desc
modes: []
query_parameters: []
example_rules: {}
        ''')

        # Create a mock rule template
        (templates_dir / 'rule_template_sql_right_recursion.sql').write_text('''
SELECT * FROM tc;
        ''')

        return tmp_path

    def test_bootstrap_system(self, mock_bootstrap_fs):
        manager = BootstrapManager(base_dir=mock_bootstrap_fs)
        
        # Test bootstrap new system
        sys_dir = manager.bootstrap_system('my_custom_db')
        assert sys_dir.exists()
        assert (sys_dir / 'rules').exists()
        
        desc_path = sys_dir / 'descriptor.yaml'
        assert desc_path.exists()
        with open(desc_path) as f:
            desc = yaml.safe_load(f)
        assert desc['name'] == 'my_custom_db'

        # Test bootstrap existing system raises error
        with pytest.raises(ValueError):
            manager.bootstrap_system('my_custom_db')

    def test_bootstrap_domain(self, mock_bootstrap_fs):
        manager = BootstrapManager(base_dir=mock_bootstrap_fs)
        
        # Test bootstrap new domain
        dom_dir = manager.bootstrap_domain('my_custom_domain')
        assert dom_dir.exists()
        
        desc_path = dom_dir / 'descriptor.yaml'
        assert desc_path.exists()
        with open(desc_path) as f:
            desc = yaml.safe_load(f)
        assert desc['name'] == 'my_custom_domain'

        # Test bootstrap existing domain raises error
        with pytest.raises(ValueError):
            manager.bootstrap_domain('my_custom_domain')

    def test_bootstrap_graph(self, mock_bootstrap_fs):
        manager = BootstrapManager(base_dir=mock_bootstrap_fs)
        
        # Test bootstrap new graph type
        manager.graph_types_dir.mkdir(parents=True, exist_ok=True)
        graph_file = manager.bootstrap_graph('my_graph', 'engine.gen.method', 'Hexagon grid')
        assert graph_file.exists()
        
        with open(graph_file) as f:
            desc = yaml.safe_load(f)
        assert desc['name'] == 'my_graph'
        assert desc['generator'] == 'engine.gen.method'
        assert desc['description'] == 'Hexagon grid'

        # Test bootstrap existing graph raises error
        with pytest.raises(ValueError):
            manager.bootstrap_graph('my_graph', 'engine.gen.method')

    def test_copy_rule_template(self, mock_bootstrap_fs):
        manager = BootstrapManager(base_dir=mock_bootstrap_fs)
        target_dir = mock_bootstrap_fs / 'target_rules'
        
        rule_file = manager.copy_rule_template(target_dir, 'rule_template_sql_right_recursion.sql')
        assert rule_file.exists()
        assert rule_file.name == 'sql_right_recursion.sql'
        assert rule_file.read_text().strip() == 'SELECT * FROM tc;'

        # Test nonexistent template raises error
        with pytest.raises(FileNotFoundError):
            manager.copy_rule_template(target_dir, 'nonexistent_template.sql')

    def test_bootstrap_missing_templates(self, mock_bootstrap_fs):
        manager = BootstrapManager(base_dir=mock_bootstrap_fs)
        with pytest.raises(FileNotFoundError):
            manager.bootstrap_system('sys2', 'nonexistent.yaml')
        with pytest.raises(FileNotFoundError):
            manager.bootstrap_domain('dom2', 'nonexistent.yaml')

    def test_list_templates(self, mock_bootstrap_fs):
        manager = BootstrapManager(base_dir=mock_bootstrap_fs)
        templates = manager.list_templates()
        assert 'descriptor_sql_database.yaml' in templates['system_descriptors']
        assert 'domain_shortest_path.yaml' in templates['domain_templates']
        assert 'rule_template_sql_right_recursion.sql' in templates['rule_templates']

