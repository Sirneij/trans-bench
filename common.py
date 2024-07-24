import logging
import os
import re
import subprocess
from pathlib import Path
from typing import Any, Optional, Union

import duckdb
import MySQLdb
import psycopg2
import singlestoredb as s2
from neo4j import GraphDatabase
from pymongo import MongoClient

# Set up logging with a specific format
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s'
)


class Base:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.driver = None
        self.db_path = None
        self.headers_rdbms = [
            'CreateTableRealTime',
            'CreateTableCPUTime',
            'LoadDataRealTime',
            'LoadDataCPUTime',
            'CreateIndexRealTime',
            'CreateIndexCPUTime',
            'AnalyzeRealTime',
            'AnalyzeCPUTime',
            'ExecuteQueryRealTime',
            'ExecuteQueryCPUTime',
            'WriteResultRealTime',
            'WriteResultCPUTime',
        ]
        self.headers_neo4j = [
            'DeleteDataRealTime',
            'DeleteDataCPUTime',
            'LoadDataRealTime',
            'LoadDataCPUTime',
            'CreateIndexXRealTime',
            'CreateIndexXCPUTime',
            'CreateIndexYRealTime',
            'CreateIndexYCPUTime',
            'QueryRealTime',
            'QueryCPUTime',
            'WriteResultRealTime',
            'WriteResultCPUTime',
        ]
        self.headers_mongodb = [
            'CreateTableRealTime',
            'CreateTableCPUTime',
            'LoadDataRealTime',
            'LoadDataCPUTime',
            'CreateIndexRealTime',
            'CreateIndexCPUTime',
            'ExecuteQueryRealTime',
            'ExecuteQueryCPUTime',
            'WriteResultRealTime',
            'WriteResultCPUTime',
        ]

    def connect_db(self, env_name: str, rule_path: Optional[str] = None) -> Union[
        duckdb.DuckDBPyConnection,
        GraphDatabase.driver,
        MongoClient,
        psycopg2.extensions.connection,
        MySQLdb.Connection,
    ]:
        env_name = env_name.lower()
        connection_methods = {
            'duckdb': self._connect_duckdb,
            'neo4j': self._connect_neo4j,
            'mongodb': self._connect_mongodb,
            'cockroachdb': self._connect_psycopg2,
            'postgres': self._connect_psycopg2,
            'mariadb': self._connect_mariadb,
        }

        if env_name not in connection_methods:
            raise ValueError(f"Unsupported database environment: {env_name}")

        return connection_methods[env_name](env_name, rule_path)

    def _connect_duckdb(
        self, env_name: str, rule_path: Optional[str] = None
    ) -> duckdb.DuckDBPyConnection:
        if rule_path is not None:
            self.db_path = Path(rule_path).parent / env_name / 'duckdb_file.db'
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(
            database=str(self.db_path) if self.db_path else ':memory:'
        )

    def _connect_neo4j(
        self, env_name: str, rule_path: Optional[str] = None
    ) -> GraphDatabase.driver:
        return GraphDatabase.driver(
            self.config[env_name]['uri'],
            auth=(self.config[env_name]['user'], self.config[env_name]['password']),
        )

    def _connect_mongodb(
        self, env_name: str, rule_path: Optional[str] = None
    ) -> MongoClient:
        self.driver = MongoClient(self.config[env_name]['uri'])
        return self.driver[self.config[env_name]['database']]

    def _connect_psycopg2(
        self, env_name: str, rule_path: Optional[str] = None
    ) -> psycopg2.extensions.connection:
        logging.info(f"Env: {env_name}")
        return psycopg2.connect(self.config[env_name]['dbURL'])

    def _connect_mariadb(
        self, env_name: str, rule_path: Optional[str] = None
    ) -> MySQLdb.Connection:
        return MySQLdb.connect(
            db=self.config[env_name]['database'],
            user=self.config[env_name]['user'],
            passwd=self.config[env_name]['password'],
            host=self.config[env_name]['host'],
            port=self.config[env_name]['port'],
            local_infile=1,  # Enable LOAD DATA LOCAL INFILE
        )

    def close(self) -> None:
        if self.driver:
            self.driver.close()
            self.driver = None
        if self.db_path and self.db_path.exists():
            os.remove(self.db_path)


class AnalyzeSystems(Base):
    def __init__(self, config: dict[str, Any], environment: str):
        super().__init__(config)
        self.environment = environment
        self.rule_path = None
        self.input_path = None
        self.timing_path = None
        self.output_folder = None

    def discover_rules(self, rules_dir: Path, extension: str) -> dict[str, Path]:
        """Discovers rule files in a directory and maps rule names to file paths."""
        return {
            rule_file.stem.split('_', 1)[-1]: rule_file
            for rule_file in rules_dir.glob(f'*{extension}')
        }

    def estimate_time_duration(self, t1: tuple, t2: tuple) -> tuple[float, float]:
        """Estimates the time duration between two time points."""
        u1, s1, cu1, cs1, elapsed1 = t1
        u2, s2, cu2, cs2, elapsed2 = t2
        return elapsed2 - elapsed1, u2 - u1 + s2 - s1 + cu2 - cu1 + cs2 - cs1

    def run_souffle_command(self, command: str) -> tuple[str, dict[str, float]]:
        """Executes a given Souffle command and logs the output, error, and timing data."""
        logging.info(f'Executing command: {command}')
        try:
            time_begin = os.times()
            result = subprocess.run(
                command,
                shell=True,
                check=True,
                stderr=subprocess.PIPE,
                stdout=subprocess.PIPE,
                text=True,
            )
            time_end = os.times()
            logging.info(f'Output: {result.stdout}')
            logging.info(f'Error: {result.stderr}')
            timing_data = self.__parse_souffle_timing_data(result.stdout)
            logging.info(f"Parsed timing data: {timing_data}")
        except subprocess.CalledProcessError as e:
            logging.error(f"Command failed: {e}")
            return 'Command failed', {}
        except Exception as e:
            logging.error(f"Error: {e}")
            return 'Error', {}

        if result.returncode == 0:
            real_time, cpu_time = self.estimate_time_duration(time_begin, time_end)
            return f"{real_time},{cpu_time}", timing_data
        else:
            return '0,0', {}

    def __parse_souffle_timing_data(self, output: str) -> dict[str, float]:
        """Parses the timing data from the output of a Souffle command."""
        pattern = r'(\w+ time): (\d+\.\d+) seconds'
        timings = re.findall(pattern, output)
        return {metric: float(time) for metric, time in timings}

    def parse_postgresql_timings(self, output: str) -> dict[str, float]:
        """Parses timing data from PostgreSQL output."""
        timing_regex = re.compile(r'Time:\s+([\d.]+)\s+ms')
        steps = [
            'CreateTable',
            'LoadData',
            'CreateIndex',
            'Analyze',
            'ExecuteQuery',
            'WriteResult',
        ]
        times = timing_regex.findall(output)
        times_in_seconds = [float(time) / 1000 for time in times]
        return dict(zip(steps, times_in_seconds))

    def set_output_folder(self) -> None:
        """Sets the output folder based on the timing path."""
        pattern = r'^timing_(.*?)_graph_(\d+)\.csv$'
        match = re.match(pattern, self.timing_path.name)
        mode, size = match.groups() if match else ('', '')

        output_file_mode = self.timing_path.parent / f'{mode}'
        output_file_mode.parent.mkdir(parents=True, exist_ok=True)

        output_folder = output_file_mode / f'{size}'
        output_folder.mkdir(parents=True, exist_ok=True)

        self.output_folder = output_folder
        logging.info(f'Output folder: {self.output_folder}')

    def set_file_paths(self, mode: str, graph_type: str, size: int) -> None:
        """Sets the paths for rule file, input file, and timing file."""
        config = self.config['defaults']['systems']
        project_root = Path(__file__).parent
        rules_dir = project_root / f'{self.environment}_rules'
        rule_files = self.discover_rules(
            rules_dir, config['environmentExtensions'][self.environment]
        )

        if mode not in rule_files:
            logging.error(f'Rule file not found for mode: {mode}')
            return

        input_dir = Path('input')
        if self.environment in config['dbSystems'] + ['souffle']:
            input_path = (
                input_dir / 'souffle' / graph_type / str(size) / 'edge.facts'
                if self.environment != 'souffle'
                else input_dir / 'souffle' / graph_type / str(size)
            )
        else:
            inputfile = f'graph_{size}.lp'
            input_path = input_dir / 'clingo_xsb' / graph_type / inputfile
        rule_path = rule_files[mode]

        timing_dir = Path('timing') / self.environment / graph_type
        timing_dir.mkdir(parents=True, exist_ok=True)
        output_file_name = (
            f'timing_{mode}_{input_path.stem}.csv'
            if self.environment not in config['dbSystems'] + ['souffle']
            else f'timing_{mode}_graph_{size}.csv'
        )
        timing_path = timing_dir / output_file_name

        self.rule_path = rule_path
        self.input_path = input_path
        self.timing_path = timing_path

        logging.info(
            f'Using rule file: {self.rule_path}, input file: {self.input_path}, timing file: {self.timing_path}'
        )

    def analyze(self) -> None:
        raise NotImplementedError
