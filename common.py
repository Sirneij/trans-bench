import json
import logging
import os
import re
import subprocess
from pathlib import Path
from typing import Any

import duckdb
import psycopg2
from neo4j import Driver, GraphDatabase
from pymongo import MongoClient
from pymongo.database import Database

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
            'CreateIndexRealTime',
            'CreateIndexCPUTime',
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

    def connect_db(
        self, env_name: str, rule_path: str = None
    ) -> duckdb.DuckDBPyConnection | Driver | Database:
        if env_name == 'duckdb':
            if rule_path is not None:
                self.db_path = Path(rule_path).parent / env_name / 'duckdb_file.db'
                self.db_path.parent.mkdir(parents=True, exist_ok=True)
            return duckdb.connect(database=str(self.db_path))

        elif env_name == 'neo4j':
            return GraphDatabase.driver(
                self.config[env_name]['uri'],
                auth=(self.config[env_name]['user'], self.config[env_name]['password']),
            )
        elif env_name == 'mongodb':
            self.driver = MongoClient(self.config[env_name]['uri'])
            return self.driver[self.config[env_name]['database']]
        elif env_name in ['cockroachdb', 'postgres']:
            return psycopg2.connect(self.config[env_name]['db_url'])

    def close(self):
        if self.driver:
            self.driver.close()
            self.driver = None
        if self.db_path and self.db_path.exists():
            os.remove(self.db_path)


class AnalyzeSystems(Base):
    def __init__(
        self,
        config: dict[str, Any],
        environment: str,
    ):
        super().__init__(config)
        self.environment = environment
        self.rule_path = None
        self.input_path = None
        self.timing_path = None
        self.output_folder = None

    def discover_rules(self, rules_dir: Path, extension: str) -> dict[str, Path]:
        """
        Discovers rule files in a directory and maps rule names to file paths.

        This function searches for files with a given extension in a directory. It then maps the rule name from each file name (the part after the first underscore)
        to the file path. The function returns this mapping as a dictionary.

        Args:
            `rules_dir (Path)`: The directory to search for rule files.
            `extension (str)`: The extension of the rule files.

        Returns:
            dict[str, Path]: A dictionary mapping rule names to file paths.
        """
        return {
            rule_file.stem.split('_', 1)[-1]: rule_file
            for rule_file in rules_dir.glob(f'*{extension}')
        }

    def estimate_time_duration(self, t1: tuple, t2: tuple) -> tuple[float, float]:
        """
        Estimates the time duration between two time points.

        This function calculates the elapsed time and the CPU time used between two time points. The time points are tuples containing user time, system time, child user time, child system time, and elapsed real time.

        Args:
            `t1 (tuple)`: The first time point. It is a tuple of the form (user time, system time, child user time, child system time, elapsed real time).
            `t2 (tuple)`: The second time point. It is a tuple of the form (user time, system time, child user time, child system time, elapsed real time).

        Returns:
            tuple: A tuple containing the difference in elapsed real time and the sum of the differences in user time, system time, child user time, and child system time.
        """
        u1, s1, cu1, cs1, elapsed1 = t1
        u2, s2, cu2, cs2, elapsed2 = t2
        return elapsed2 - elapsed1, u2 - u1 + s2 - s1 + cu2 - cu1 + cs2 - cs1

    def run_souffle_command(self, command: str) -> tuple[str, dict[str, float]]:
        """
        Executes a given Souffle command and logs the output, error, and timing data.

        This function takes in a Souffle command, executes it using the subprocess module, and logs the output, error, and timing data. If the command fails, it logs the error and returns a failure message. If the command succeeds, it calculates the real time and CPU time used and returns these values along with the parsed timing data.

        Args:
            `command (str)`: The Souffle command to be executed.

        Returns:
            `tuple`: A tuple containing the real time, CPU time, and parsed timing data if the command succeeds. If the command fails, it returns a failure message.
        """
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
            logging.info(f"Command failed: {e}")
            return 'Command failed'
        except Exception as e:
            logging.error(f"Error: {e}")
            return 'Error'

        if result.returncode == 0:
            real_time, cpu_time = self.estimate_time_duration(time_begin, time_end)
            return f"{real_time},{cpu_time}", timing_data
        else:
            return '0,0', {}

    def __parse_souffle_timing_data(self, output: str) -> dict[str, float]:
        """
        Parses the timing data from the output of a Souffle command.

        This function takes in the output of a Souffle command, extracts the timing data using a regular expression, and returns this data as a dictionary.

        Args:
            `output (str)`: The output of a Souffle command.

        Returns:
            `dict`: A dictionary where the keys are the names of the timing metrics (e.g., 'Loading time', 'Query time') and the values are the corresponding times in seconds.
        """
        pattern = r'(\w+ time): (\d+\.\d+) seconds'
        timings = re.findall(pattern, output)
        return dict(timings)

    def parse_postgresql_timings(self, output: str) -> dict[str, float]:
        # Regular expression to match timing lines
        timing_regex = re.compile(r'Time:\s+([\d.]+)\s+ms')

        # Define the steps in the order they appear
        steps = [
            'CreateTable',
            'LoadData',
            'CreateIndex',
            'Analyze',
            'ExecuteQuery',
            'WriteResult',
        ]

        # Find all timing values in the output
        times = timing_regex.findall(output)

        # Convert times from milliseconds to seconds
        times_in_seconds = [float(time) / 1000 for time in times]

        # Map steps to their corresponding timing values
        timings = dict(zip(steps, times_in_seconds))

        return timings

    def set_output_folder(self) -> None:
        pattern = r'^timing_(.*?)_graph_(\d+)\.csv$'
        match = re.match(pattern, self.timing_path.name)
        mode, size = '', ''
        if match:
            mode, size = match.groups()

        output_file_mode = self.timing_path.parent / f'{mode}'
        output_file_mode.parent.mkdir(parents=True, exist_ok=True)

        output_folder = output_file_mode / f'{size}'
        output_folder.mkdir(parents=True, exist_ok=True)

        self.output_folder = output_folder

        logging.info(f'Output folder: {self.output_folder}')

    def set_file_paths(self, mode: str, graph_type: str, size: int) -> None:
        config = self.config['defaults']['systems']
        project_root = Path(__file__).parent
        rules_dir = project_root / f'{self.environment}_rules'
        rule_files = self.discover_rules(
            rules_dir,
            config['environmentExtensions'][self.environment],
        )

        if mode not in rule_files:
            logging.error(f'Rule file not found for mode: {mode}')
            return

        input_dir = Path('input')
        if self.environment in config['dbSystems'] + ['souffle']:
            if self.environment == 'souffle':
                input_path = input_dir / 'souffle' / graph_type / str(size)
            else:
                input_path = (
                    input_dir / 'souffle' / graph_type / str(size) / 'edge.facts'
                )
        else:
            inputfile = f'graph_{size}.lp'
            input_path = input_dir / 'clingo_xsb' / graph_type / inputfile
        rule_path = rule_files[mode]

        timing_dir = Path('timing') / self.environment / graph_type
        timing_dir.mkdir(parents=True, exist_ok=True)
        if self.environment in config['dbSystems'] + ['souffle']:
            output_file_name = f'timing_{mode}_graph_{size}.csv'
        else:
            output_file_name = f'timing_{mode}_{input_path.stem}.csv'
        timing_path = timing_dir / output_file_name

        self.rule_path = rule_path
        self.input_path = input_path
        self.timing_path = timing_path

        logging.info(
            f'Using rule file: {self.rule_path}, input file: {self.input_path}, timing file: {self.timing_path}'
        )

    def analyze(self) -> None:
        raise NotImplementedError
