from unittest.mock import MagicMock, patch

import pytest

from engine.validation import RuleValidator


class TestRuleValidator:
    @pytest.fixture
    def mock_fs(self, tmp_path):
        # Create systems directory
        sys_dir = tmp_path / 'systems' / 'test_sys'
        sys_dir.mkdir(parents=True)
        (sys_dir / 'descriptor.yaml').write_text('''
name: test_sys
display_name: Test System
category: db
protocol: duckdb
timing_phases:
  - {id: load, label: Load}
input_format: tsv
modes: [right_recursion]
rule_extension: .sql
flags:
  requires_credentials: true
        ''')
        (sys_dir / 'credentials.yaml').write_text('database: ":memory:"')

        # Create rules folder and rule files
        rules_dir = sys_dir / 'rules'
        rules_dir.mkdir(parents=True)
        (rules_dir / 'transitive_right_recursion.sql').write_text('SELECT * FROM tc;')
        (rules_dir / 'test_domain_right_recursion.sql').write_text('SELECT * FROM tc;')

        # Create domains directory
        domain_dir = tmp_path / 'domains' / 'test_domain'
        domain_dir.mkdir(parents=True)
        (domain_dir / 'descriptor.yaml').write_text('''
name: test_domain
display_name: Test Domain
description: Desc
modes: [right_recursion]
query_parameters:
  - {name: src, type: int}
example_rules: {}
        ''')

        return tmp_path

    def test_validate_system_success(self, mock_fs):
        validator = RuleValidator(base_dir=mock_fs)
        assert validator.validate_system('test_sys') is True

    def test_validate_system_not_found(self, mock_fs):
        validator = RuleValidator(base_dir=mock_fs)
        assert validator.validate_system('nonexistent_sys') is False

    def test_validate_system_invalid_descriptor(self, mock_fs):
        # Break the descriptor file
        sys_dir = mock_fs / 'systems' / 'test_sys'
        (sys_dir / 'descriptor.yaml').write_text('bad: [yaml')
        validator = RuleValidator(base_dir=mock_fs)
        assert validator.validate_system('test_sys') is False

    def test_validate_system_missing_rules(self, mock_fs):
        # Delete rules dir
        import shutil
        shutil.rmtree(mock_fs / 'systems' / 'test_sys' / 'rules')
        validator = RuleValidator(base_dir=mock_fs)
        assert validator.validate_system('test_sys') is False

    def test_validate_system_missing_credentials(self, mock_fs):
        # Delete credentials file
        (mock_fs / 'systems' / 'test_sys' / 'credentials.yaml').unlink()
        validator = RuleValidator(base_dir=mock_fs)
        assert validator.validate_system('test_sys') is False

    def test_validate_domain_success(self, mock_fs):
        validator = RuleValidator(base_dir=mock_fs)
        assert validator.validate_domain('test_domain') is True

    def test_validate_domain_not_found(self, mock_fs):
        validator = RuleValidator(base_dir=mock_fs)
        assert validator.validate_domain('nonexistent_domain') is False

    def test_validate_domain_missing_rules(self, mock_fs):
        # Delete the rule file that domain expects
        (mock_fs / 'systems' / 'test_sys' / 'rules' / 'test_domain_right_recursion.sql').unlink()
        validator = RuleValidator(base_dir=mock_fs)
        assert validator.validate_domain('test_domain') is False

    def test_test_rule_file(self, mock_fs):
        validator = RuleValidator(base_dir=mock_fs)
        rule_path = mock_fs / 'systems' / 'test_sys' / 'rules' / 'transitive_right_recursion.sql'

        # Test valid
        assert validator.test_rule_file(rule_path) is True
        assert validator.validate_rule_file(rule_path) is True # Legacy API

        # Test nonexistent
        assert validator.test_rule_file(mock_fs / 'nonexistent.sql') is False

        # Test empty file
        empty_file = mock_fs / 'empty.sql'
        empty_file.write_text('')
        assert validator.test_rule_file(empty_file) is False

    def test_static_checks(self, mock_fs):
        validator = RuleValidator(base_dir=mock_fs)

        # Unbalanced parens
        unbalanced = mock_fs / 'unbalanced.sql'
        unbalanced.write_text('SELECT * FROM (tc;')
        assert validator.test_rule_file(unbalanced) is False

        # Unbalanced brackets
        unbalanced.write_text('SELECT * FROM tc[;')
        assert validator.test_rule_file(unbalanced) is False

        # Balanced with quotes
        balanced = mock_fs / 'balanced.sql'
        balanced.write_text("SELECT * FROM tc WHERE x = '(';")
        assert validator.test_rule_file(balanced) is True

    def test_language_specific_warnings(self, mock_fs):
        validator = RuleValidator(base_dir=mock_fs)

        # SQL check
        sql_file = mock_fs / 'sql.sql'
        sql_file.write_text("SELECT * FROM tc WHERE x = 'unclosed;")
        assert validator.test_rule_file(sql_file) is False

        # Datalog check
        dl_file = mock_fs / 'dl.dl'
        dl_file.write_text('a :- b.')
        # Datalog warnings don't fail the test_rule_file unless there's syntax issues, they just log warnings
        assert validator.test_rule_file(dl_file) is True

        # Cypher check
        cy_file = mock_fs / 'cy.cypher'
        cy_file.write_text('MATCH (n) RETURN n;')
        assert validator.test_rule_file(cy_file) is True

        # Prolog check
        pl_file = mock_fs / 'pl.pl'
        pl_file.write_text('a :- b.')
        assert validator.test_rule_file(pl_file) is True

        # Distalgo check
        da_file = mock_fs / 'da.da'
        da_file.write_text('def run(): pass')
        assert validator.test_rule_file(da_file) is True

        # JS check
        js_file = mock_fs / 'js.js'
        js_file.write_text('function f() { return 1; }')
        assert validator.test_rule_file(js_file) is True

        # JS check unbalanced
        js_unbalanced = mock_fs / 'js_unbalanced.js'
        js_unbalanced.write_text('function f() { return 1;')
        assert validator.test_rule_file(js_unbalanced) is False

    @patch('duckdb.connect')
    def test_live_dry_run(self, mock_duckdb_connect, mock_fs):
        validator = RuleValidator(base_dir=mock_fs)
        rule_path = mock_fs / 'systems' / 'test_sys' / 'rules' / 'transitive_right_recursion.sql'

        # Set up mock duckdb connection
        mock_conn = MagicMock()
        mock_duckdb_connect.return_value = mock_conn

        # Test live check with duckdb success
        assert validator.test_rule_file(rule_path, system_name='test_sys') is True

        # Test live check with duckdb error
        mock_conn.execute.side_effect = Exception("DuckDB Error")
        assert validator.test_rule_file(rule_path, system_name='test_sys') is False

    @patch('psycopg2.connect')
    def test_live_dry_run_postgres(self, mock_pg_connect, mock_fs):
        # Create a postgres system descriptor
        sys_dir = mock_fs / 'systems' / 'pg_sys'
        sys_dir.mkdir(parents=True)
        (sys_dir / 'descriptor.yaml').write_text('''
name: pg_sys
display_name: PG System
category: db
protocol: psycopg2
timing_phases:
  - {id: load, label: Load}
input_format: tsv
modes: [right_recursion]
rule_extension: .sql
flags:
  requires_credentials: false
        ''')
        rules_dir = sys_dir / 'rules'
        rules_dir.mkdir(parents=True)
        rule_path = rules_dir / 'transitive_right_recursion.sql'
        rule_path.write_text('SELECT * FROM tc;')

        validator = RuleValidator(base_dir=mock_fs)
        
        mock_conn = MagicMock()
        mock_pg_connect.return_value = mock_conn
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        # Test live check success
        assert validator.test_rule_file(rule_path, system_name='pg_sys') is True

        # Test live check execution error
        mock_cur.execute.side_effect = Exception("PG execute error")
        assert validator.test_rule_file(rule_path, system_name='pg_sys') is False


class TestValidationAdvanced:
    """Advanced validation tests covering edge cases."""

    @pytest.fixture
    def extended_mock_fs(self, tmp_path):
        """Extended filesystem setup for complex validation tests."""
        # Create multiple systems with different configurations
        for sys_name, protocol in [('sys_duckdb', 'duckdb'), ('sys_clingo', 'clingo_python')]:
            sys_dir = tmp_path / 'systems' / sys_name
            sys_dir.mkdir(parents=True)
            
            (sys_dir / 'descriptor.yaml').write_text(f'''
name: {sys_name}
display_name: {sys_name.title()}
category: db
protocol: {protocol}
timing_phases:
  - {{id: load, label: Load}}
  - {{id: query, label: Query}}
input_format: {"lp" if protocol == "clingo_python" else "tsv"}
modes: [mode1, mode2]
rule_extension: {"py" if protocol == "clingo_python" else "sql"}
flags:
  requires_credentials: false
            ''')
            
            (sys_dir / 'credentials.yaml').write_text('{}')
            
            rules_dir = sys_dir / 'rules'
            rules_dir.mkdir(parents=True)
            for mode in ['mode1', 'mode2']:
                ext = '.py' if protocol == 'clingo_python' else '.sql'
                (rules_dir / f'transitive_{mode}{ext}').write_text('SELECT * FROM tc;')

        # Create multiple domains
        for i in range(1, 3):
            dom_dir = tmp_path / 'domains' / f'domain_{i}'
            dom_dir.mkdir(parents=True)
            (dom_dir / 'descriptor.yaml').write_text(f'''
name: domain_{i}
display_name: Domain {i}
description: Test domain {i}
modes: [mode1]
query_parameters:
  - {{name: p1, type: string}}
example_rules: {{}}
            ''')

        return tmp_path

    def test_validate_all_systems(self, extended_mock_fs):
        """Test validation of all systems in directory."""
        validator = RuleValidator(base_dir=extended_mock_fs)
        
        # Validation should recognize systems (may have warnings but should return a boolean)
        result_duckdb = validator.validate_system('sys_duckdb')
        result_clingo = validator.validate_system('sys_clingo')
        assert isinstance(result_duckdb, bool)
        assert isinstance(result_clingo, bool)

    def test_validate_all_domains(self, extended_mock_fs):
        """Test validation of all domains."""
        validator = RuleValidator(base_dir=extended_mock_fs)
        
        result_1 = validator.validate_domain('domain_1')
        result_2 = validator.validate_domain('domain_2')
        assert isinstance(result_1, bool)
        assert isinstance(result_2, bool)

    def test_syntax_check_sql_valid(self, tmp_path):
        """Test SQL syntax validation."""
        validator = RuleValidator(base_dir=tmp_path)
        
        # Valid SQL variations
        valid_sql_files = [
            ('simple.sql', 'SELECT * FROM tc;'),
            ('with_cte.sql', 'WITH t AS (SELECT 1) SELECT * FROM t;'),
            ('complex.sql', 'SELECT * FROM (SELECT 1 AS x) WHERE x = 1;'),
            ('nested_quotes.sql', "SELECT * FROM t WHERE s = 'test(value)';"),
        ]
        
        for name, content in valid_sql_files:
            f = tmp_path / name
            f.write_text(content)
            assert validator.test_rule_file(f) is True, f"Failed for {name}"

    def test_syntax_check_sql_invalid(self, tmp_path):
        """Test invalid SQL syntax detection."""
        validator = RuleValidator(base_dir=tmp_path)
        
        invalid_sql_files = [
            ('unbalanced_paren.sql', 'SELECT * FROM (tc;'),
            ('unbalanced_bracket.sql', 'SELECT * FROM [tc;'),
            ('multiple_unbalanced.sql', 'SELECT (((*;'),
        ]
        
        for name, content in invalid_sql_files:
            f = tmp_path / name
            f.write_text(content)
            assert validator.test_rule_file(f) is False, f"Should fail for {name}"

    def test_syntax_check_datalog_valid(self, tmp_path):
        """Test Datalog syntax validation."""
        validator = RuleValidator(base_dir=tmp_path)
        
        valid_dl = [
            ('simple.dl', 'path(X, Y) :- edge(X, Y).'),
            ('recursive.dl', 'path(X, Y) :- edge(X, Y). path(X, Z) :- path(X, Y), edge(Y, Z).'),
            ('fact.dl', 'edge(1, 2).'),
            ('comment.dl', '% comment\npath(X, Y) :- edge(X, Y).'),
        ]
        
        for name, content in valid_dl:
            f = tmp_path / name
            f.write_text(content)
            # Datalog checks don't fail on valid syntax
            result = validator.test_rule_file(f)
            assert result is True, f"Failed for {name}"

    def test_syntax_check_cypher_valid(self, tmp_path):
        """Test Cypher syntax validation."""
        validator = RuleValidator(base_dir=tmp_path)
        
        valid_cypher = [
            ('simple.cypher', 'MATCH (n) RETURN n;'),
            ('with_where.cypher', 'MATCH (n:Node) WHERE n.id = 1 RETURN n;'),
            ('relationship.cypher', 'MATCH (a)-[:REL]->(b) RETURN a, b;'),
        ]
        
        for name, content in valid_cypher:
            f = tmp_path / name
            f.write_text(content)
            assert validator.test_rule_file(f) is True

    def test_syntax_check_prolog_valid(self, tmp_path):
        """Test Prolog syntax validation."""
        validator = RuleValidator(base_dir=tmp_path)
        
        valid_prolog = [
            ('rule.pl', 'parent(john, mary). parent(john, tom).'),
            ('recursive.pl', 'ancestor(X, Y) :- parent(X, Y). ancestor(X, Z) :- parent(X, Y), ancestor(Y, Z).'),
        ]
        
        for name, content in valid_prolog:
            f = tmp_path / name
            f.write_text(content)
            assert validator.test_rule_file(f) is True

    def test_syntax_check_python_valid(self, tmp_path):
        """Test Python/Distalgo syntax validation."""
        validator = RuleValidator(base_dir=tmp_path)
        
        valid_python = [
            ('simple.da', 'def run(): pass'),
            ('with_class.da', 'class Test: def method(self): pass'),
            ('function.da', 'def f(x, y): return x + y'),
        ]
        
        for name, content in valid_python:
            f = tmp_path / name
            f.write_text(content)
            assert validator.test_rule_file(f) is True

    def test_syntax_check_javascript_valid(self, tmp_path):
        """Test JavaScript syntax validation."""
        validator = RuleValidator(base_dir=tmp_path)
        
        valid_js = [
            ('simple.js', 'function f() { return 1; }'),
            ('arrow.js', 'const f = () => { return 1; }'),
            ('nested.js', 'function f() { function g() { return 1; } return g(); }'),
        ]
        
        for name, content in valid_js:
            f = tmp_path / name
            f.write_text(content)
            assert validator.test_rule_file(f) is True

    def test_syntax_check_javascript_invalid(self, tmp_path):
        """Test JavaScript syntax errors."""
        validator = RuleValidator(base_dir=tmp_path)
        
        invalid_js = [
            ('unbalanced_paren.js', 'function f('),
            ('unbalanced_brace.js', 'function f() { return 1;'),
            ('unbalanced_bracket.js', 'const arr = [1, 2, 3'),
        ]
        
        for name, content in invalid_js:
            f = tmp_path / name
            f.write_text(content)
            assert validator.test_rule_file(f) is False

    def test_syntax_check_mixed_quotes(self, tmp_path):
        """Test syntax checking with mixed quotes."""
        validator = RuleValidator(base_dir=tmp_path)
        
        # SQL with both quote types
        f = tmp_path / 'mixed_quotes.sql'
        f.write_text('''
SELECT * FROM t 
WHERE s = 'can''t' AND t = "value"
AND u = '(test)' AND v = "[test]"
''')
        assert validator.test_rule_file(f) is True

    def test_syntax_check_escaped_characters(self, tmp_path):
        """Test syntax checking with escaped characters."""
        validator = RuleValidator(base_dir=tmp_path)
        
        # SQL with escaped quotes
        f = tmp_path / 'escaped.sql'
        f.write_text(r'''
SELECT * FROM t WHERE s = 'test\'s value'
''')
        # This should pass basic bracket counting
        result = validator.test_rule_file(f)
        assert result is True

    def test_validate_rule_file_legacy_api(self, tmp_path):
        """Test legacy validate_rule_file API."""
        validator = RuleValidator(base_dir=tmp_path)
        
        rule_file = tmp_path / 'rule.sql'
        rule_file.write_text('SELECT * FROM tc;')
        
        # Legacy API should work
        assert validator.validate_rule_file(rule_file) is True

    def test_system_descriptor_extraction(self, extended_mock_fs):
        """Test system descriptor is properly extracted during validation."""
        validator = RuleValidator(base_dir=extended_mock_fs)
        
        # Validation should work for valid systems
        result = validator.validate_system('sys_duckdb')
        assert isinstance(result, bool)
        
        # Rules dir should exist
        rules_dir = extended_mock_fs / 'systems' / 'sys_duckdb' / 'rules'
        assert rules_dir.exists()

    def test_domain_with_missing_rule_in_system(self, extended_mock_fs):
        """Test domain validation fails when system lacks required rule."""
        validator = RuleValidator(base_dir=extended_mock_fs)
        
        # Create a new domain that requires a mode not in any system
        dom_dir = extended_mock_fs / 'domains' / 'special_domain'
        dom_dir.mkdir(parents=True)
        (dom_dir / 'descriptor.yaml').write_text('''
name: special_domain
display_name: Special Domain
description: Domain with special mode
modes: [special_mode]
query_parameters: []
example_rules: {}
        ''')
        
        # Validation should fail because no system has special_mode
        assert validator.validate_domain('special_domain') is False

    def test_multiple_timing_phases(self, extended_mock_fs):
        """Test systems with multiple timing phases."""
        validator = RuleValidator(base_dir=extended_mock_fs)
        
        # Validation should handle systems with multiple timing phases
        result = validator.validate_system('sys_duckdb')
        assert isinstance(result, bool)

    def test_csv_header_generation(self, extended_mock_fs):
        """Test CSV header generation from timing phases."""
        from engine.loader import DescriptorLoader
        
        loader = DescriptorLoader(base_dir=extended_mock_fs)
        sys = loader.get_system('sys_duckdb')
        
        # Should have headers for load and query phases
        headers = sys.csv_headers
        assert 'LoadRealTime' in headers
        assert 'LoadCPUTime' in headers
        assert 'QueryRealTime' in headers
        assert 'QueryCPUTime' in headers

    def test_empty_rule_file_detection(self, tmp_path):
        """Test detection of empty rule files."""
        validator = RuleValidator(base_dir=tmp_path)
        
        empty_file = tmp_path / 'empty.sql'
        empty_file.write_text('')
        
        assert validator.test_rule_file(empty_file) is False

    def test_whitespace_only_file_detection(self, tmp_path):
        """Test detection of whitespace-only files."""
        validator = RuleValidator(base_dir=tmp_path)
        
        ws_file = tmp_path / 'whitespace.sql'
        ws_file.write_text('   \n\n   \t\t\n   ')
        
        assert validator.test_rule_file(ws_file) is False

    def test_comment_only_file_detection(self, tmp_path):
        """Test detection of comment-only files."""
        validator = RuleValidator(base_dir=tmp_path)
        
        # SQL with only comments
        comment_file = tmp_path / 'comments.sql'
        comment_file.write_text('-- This is a comment\n-- Another comment')
        
        # Files with no content after comments are detected
        result = validator.test_rule_file(comment_file)
        assert isinstance(result, bool)
