from pathlib import Path
from unittest.mock import MagicMock, patch

from common import AnalyzeSystems, Base
from tests import BaseTest


class TestCommon(BaseTest):
    def setUp(self):
        super().setUp()
        self.config = {
            'duckdb': {'uri': 'duckdb_file.db'},
            'neo4j': {
                'uri': 'bolt://localhost:7687',
                'user': 'neo4j',
                'password': 'test',
            },
            'mongodb': {'uri': 'mongodb://localhost:27017', 'database': 'testdb'},
            'postgres': {'dbURL': 'postgresql://user:password@localhost:5432/testdb'},
            'mariadb': {
                'database': 'testdb',
                'user': 'user',
                'password': 'password',
                'host': 'localhost',
                'port': 3306,
            },
            'defaults': {
                'systems': {
                    'environmentExtensions': {
                        'duckdb': '.dl',
                        'neo4j': '.cypher',
                        'mongodb': '.json',
                        'postgres': '.sql',
                        'mariadb': '.sql',
                    },
                    'dbSystems': ['postgres', 'mariadb'],
                }
            },
        }
        self.env = 'xsb'
        self.base = Base(self.config)
        self.analyze_systems = AnalyzeSystems(self.config, self.env)

    @patch('duckdb.connect')
    def test_connect_duckdb(self, mock_connect):
        conn = MagicMock()
        mock_connect.return_value = conn

        result = self.base._connect_duckdb('duckdb')
        self.assertEqual(result, conn)
        mock_connect.assert_called_once()

    @patch('neo4j.GraphDatabase.driver')
    def test_connect_neo4j(self, mock_driver):
        driver = MagicMock()
        mock_driver.return_value = driver

        result = self.base._connect_neo4j('neo4j')
        self.assertEqual(result, driver)
        mock_driver.assert_called_once_with(
            'bolt://localhost:7687', auth=('neo4j', 'test')
        )

    @patch('pymongo.MongoClient', autospec=True)
    def test_connect_mongodb(self, mock_client):
        # Mock the MongoClient
        client_instance = MagicMock()
        mock_client.return_value = client_instance
        
        # Mock the database return
        db_instance = MagicMock()
        client_instance.__getitem__.return_value = db_instance
        
        # Test the connection method
        result = self.base._connect_mongodb('mongodb')
        
        # Assertions
        mock_client.assert_called_once_with('mongodb://localhost:27017')
        self.assertEqual(result, db_instance)


    @patch('psycopg2.connect')
    def test_connect_psycopg2(self, mock_connect):
        conn = MagicMock()
        mock_connect.return_value = conn

        result = self.base._connect_psycopg2('postgres')
        self.assertEqual(result, conn)
        mock_connect.assert_called_once_with(
            'postgresql://user:password@localhost:5432/testdb'
        )

    @patch('MySQLdb.connect')
    def test_connect_mariadb(self, mock_connect):
        conn = MagicMock()
        mock_connect.return_value = conn

        result = self.base._connect_mariadb('mariadb')
        self.assertEqual(result, conn)
        mock_connect.assert_called_once_with(
            db='testdb',
            user='user',
            passwd='password',
            host='localhost',
            port=3306,
            local_infile=1,
        )

    def test_close(self):
        self.base.driver = MagicMock()
        self.base.db_path = Path('test.db')
        self.base.db_path.touch()

        self.base.close()

        if self.base.driver is not None:
            self.base.driver.close.assert_called_once()
        self.assertFalse(self.base.db_path.exists())

    def test_discover_rules(self):
        rules_dir = self.test_dir
        (rules_dir / 'rule_test_1.dl').touch()
        (rules_dir / 'rule_test_2.dl').touch()

        expected = {
            'test_1': rules_dir / 'rule_test_1.dl',
            'test_2': rules_dir / 'rule_test_2.dl',
        }
        result = self.analyze_systems.discover_rules(rules_dir, '.dl')
        self.assertEqual(result, expected)

    def test_estimate_time_duration(self):
        t1 = (1, 2, 3, 4, 5)
        t2 = (6, 7, 8, 9, 10)
        real_time, cpu_time = self.analyze_systems.estimate_time_duration(t1, t2)
        self.assertEqual(real_time, 5)
        self.assertEqual(cpu_time, 20)

    @patch('subprocess.run')
    @patch('os.times')
    def test_run_souffle_command(self, mock_times, mock_run):
        mock_times.side_effect = [(1, 2, 3, 4, 5), (6, 7, 8, 9, 10)]
        mock_run.return_value = MagicMock(
            stdout='Real time: 1.0 seconds\nCPU time: 2.0 seconds',
            stderr='',
            returncode=0,
        )

        output, timing_data = self.analyze_systems.run_souffle_command('dummy_command')

        self.assertEqual(output, '5,20')
        self.assertEqual(timing_data, {'Real time': 1.0, 'CPU time': 2.0})

    def test_parse_souffle_timing_data(self):
        output = 'Real time: 1.0 seconds\nCPU time: 2.0 seconds'
        expected = {'Real time': 1.0, 'CPU time': 2.0}
        result = self.analyze_systems._AnalyzeSystems__parse_souffle_timing_data(output)
        self.assertEqual(result, expected)

    def test_parse_postgresql_timings(self):
        output = 'Time: 123.456 ms'
        expected = {
            'CreateTable': 0.123456,
        }
        result = self.analyze_systems.parse_postgresql_timings(output)
        self.assertEqual(result, expected)
