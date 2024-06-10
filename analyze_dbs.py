import argparse
import csv
import gc
import json
import logging
import os
import subprocess
from pathlib import Path

import duckdb
import pexpect
from neo4j import GraphDatabase

from common import AnalyzeSystems, get_files
from transitive import DB_SYSTEMS

# Set up logging with a specific format
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s'
)


class AnalyzeDBs(AnalyzeSystems):
    def __init__(
        self,
        environment: str,
        rule_path: Path,
        input_path: Path,
        timing_path: Path,
        config: dict,
    ):
        super().__init__(environment, rule_path, input_path, timing_path)
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
        self.headers_nosql = [
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
        self.conn = None
        self.db_path = None
        self.config = config

    def connect(self, rule_path: str = None):
        if self.environment == 'duckdb':
            if rule_path is not None:
                self.db_path = Path(rule_path).parent / 'duckdb' / 'duckdb_file.db'
                self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self.conn = duckdb.connect(database=str(self.db_path))

        elif self.environment == 'neo4j':
            self.driver = GraphDatabase.driver(
                self.config['neo4j']['uri'],
                auth=(self.config['neo4j']['user'], self.config['neo4j']['password']),
            )

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
        if self.db_path and self.db_path.exists():
            os.remove(self.db_path)
        if hasattr(self, 'driver'):
            self.driver.close()

    def solve_with_postgres(
        self,
        fact_file: Path,
        rule_file: Path,
        timing_path: Path,
        output_folder: Path,
    ) -> None:
        logging.info(f'Executing for PostgreSQL.')

        # Read the SQL script and substitute the placeholders
        with open(rule_file, 'r') as f:
            sql_script = f.read()

        # Substitute the placeholders with actual file paths
        results_path = output_folder / 'postgres_results.csv'
        sql_script = sql_script.replace('{data_file}', str(fact_file))
        sql_script = sql_script.replace('{output_file}', str(results_path))

        # Split the script into individual commands
        sql_commands = [
            f'{command.strip()};'
            for command in sql_script.split(';')
            if command.strip()
        ]

        timing_results = {header: 0 for header in self.headers_rdbms}

        for i, command in enumerate(sql_commands):
            exec_command = ['psql', '-c', command]
            start_time = os.times()
            try:
                subprocess.run(exec_command, text=True, capture_output=True, check=True)
            except Exception as e:
                logging.error(f'Error executing command: {command}. Error: {e}')
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results[self.headers_rdbms[2 * i]] = real_time
            timing_results[self.headers_rdbms[2 * i + 1]] = cpu_time

        # Write timing results to CSV file
        is_new_file = not timing_path.exists()
        with open(timing_path, 'a', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            if is_new_file:
                csv_writer.writerow(self.headers_rdbms)
            csv_writer.writerow(
                [timing_results[header] for header in self.headers_rdbms]
            )

        logging.info(f'(PostgreSQL) Experiment timing results saved to: {timing_path}')

        # Drop the tables created.
        try:
            subprocess.run(
                ['psql', '-c', 'DROP TABLE IF EXISTS tc_path, tc_result;'],
                text=True,
                capture_output=True,
                check=True,
            )
        except Exception as e:
            logging.error('Error droping tables: {e}')

    def solve_with_mariadb(
        self,
        fact_file: Path,
        rule_file: Path,
        timing_path: Path,
        output_folder: Path,
    ) -> None:
        logging.info('Executing for mariadb.')

        with open(rule_file, 'r') as f:
            sql_script = f.read()

        results_path = output_folder / 'mariadb_results.csv'
        sql_script = sql_script.replace('{data_file}', f'{fact_file}')

        # Split the script into individual commands
        sql_commands = [
            f'{command.strip()};'
            for command in sql_script.split(';')
            if command.strip()
        ]

        timing_results = {header: 0 for header in self.headers_rdbms}

        host = self.config['mariadb']['host']
        user = self.config['mariadb']['user']
        password = self.config['mariadb']['password']
        database = self.config['mariadb']['database']
        machine_user_password = self.config['machineUserPassword']

        for i, command in enumerate(sql_commands):
            cmd = (
                f'mysql -h {host} -u {user} -p\'{password}\' {database} -e "{command}"'
            )
            start_time = os.times()
            try:
                subprocess.run(
                    cmd, shell=True, capture_output=True, text=True, check=True
                )
            except Exception as e:
                logging.error(f'Error executing command: {command}. Error: {e}')
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results[self.headers_rdbms[2 * i]] = real_time
            timing_results[self.headers_rdbms[2 * i + 1]] = cpu_time

        # Write timing results to CSV file
        is_new_file = not timing_path.exists()
        with open(timing_path, 'a', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            if is_new_file:
                csv_writer.writerow(self.headers_rdbms)
            csv_writer.writerow(
                [timing_results[header] for header in self.headers_rdbms]
            )

        # Drop the tables created.
        try:
            subprocess.run(
                f'mysql -h {host} -u {user} -p{password} {database} -e "DROP TABLE IF EXISTS tc_path, tc_result;"',
                text=True,
                capture_output=True,
                shell=True,
                check=True,
            )
        except Exception as e:
            logging.error(f'Error droping tables: {e}')

        # NOTE: There is an issue with mariadb that prevents programs from writing into any
        #  directory of choice even after I set `secure_file_priv = ""` in `~/.my.cnf`,
        # `/etc/mysql/my.cnf` in my Ubuntu (`~/.my.ini` works for Windows)
        # but I could write into `/tmp/` so I did that temporarily and thereafter decided to move
        # it to the desired file location using the lines below.
        cp_cmd = f'cp /tmp/mariadb_results.csv {results_path}'
        rm_cmd = f'sudo rm -rf /tmp/mariadb_results.csv'
        # Run the command and pass the password
        try:
            subprocess.run(
                cp_cmd, text=True, capture_output=True, shell=True, check=True
            )
        except Exception as e:
            logging.error(f'Copy (cp) /tmp/{e}')

        try:
            child = pexpect.spawn(rm_cmd)
            child.expect('password')
            child.sendline(machine_user_password)
            child.expect(pexpect.EOF)
        except Exception as e:
            logging.error(f'Remove(rm) /tmp/{e}')

    def solve_with_duckdb(
        self,
        fact_file: Path,
        rule_file: Path,
        timing_path: Path,
        output_folder: Path,
    ) -> None:
        conn = self.conn
        with open(rule_file, 'r') as f:
            sql_script = f.read()

        results_path = output_folder / 'duckdb_results.csv'
        sql_script = sql_script.replace('{data_file}', f'{fact_file}')
        sql_script = sql_script.replace('{output_file}', f'{results_path}')

        # Split the script into individual commands
        sql_commands = [
            f'{command.strip()};'
            for command in sql_script.split(';')
            if command.strip()
        ]

        timing_results = {header: 0 for header in self.headers_rdbms}

        for i, command in enumerate(sql_commands):
            start_time = os.times()
            try:
                conn.execute(command)
            except Exception as e:
                logging.error(f'Error executing command: {command}. Error: {e}')
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results[self.headers_rdbms[2 * i]] = real_time
            timing_results[self.headers_rdbms[2 * i + 1]] = cpu_time

        # Write timing results to CSV file
        is_new_file = not timing_path.exists()
        with open(timing_path, 'a', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            if is_new_file:
                csv_writer.writerow(self.headers_rdbms)
            csv_writer.writerow(
                [timing_results[header] for header in self.headers_rdbms]
            )

    def solve_with_neo4j(
        self,
        fact_file: Path,
        rule_file: Path,
        timing_path: Path,
        output_folder: Path,
    ) -> None:
        results_path = output_folder / 'neo4j_results.csv'

        machine_user_password = self.config['machineUserPassword']
        neo4j_import_dir = self.config['neo4j']['import_directory']
        fact_file_name = os.path.basename(fact_file)

        cp_cmd = f'sudo cp {fact_file.resolve()} {neo4j_import_dir}/'

        try:
            child = pexpect.spawn(cp_cmd)
            child.expect('password')
            child.sendline(machine_user_password)
            child.expect(pexpect.EOF)
        except Exception as e:
            logging.error(f'Copy(cp) neo4j error: {e}')

        with open(rule_file, 'r') as f:
            cypher_script = f.read()

        cypher_script = cypher_script.replace('{data_file}', str(fact_file_name))

        commands = [
            command.strip() for command in cypher_script.split(';') if command.strip()
        ]

        timing_results = {header: 0 for header in self.headers_nosql}

        with self.driver.session() as session:
            for i, command in enumerate(
                commands[:-1]
            ):  # Execute all but the last command
                start_time = os.times()
                try:
                    session.run(command)
                except Exception as e:
                    logging.error(f'Error executing: {command} with error: {e}')

                end_time = os.times()
                real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
                timing_results[self.headers_nosql[2 * i]] = real_time
                timing_results[self.headers_nosql[2 * i + 1]] = cpu_time

            # Execute the last command (query) and write the results to a file
            query = commands[-1]
            start_time = os.times()
            result = session.run(query)
            records = [(record["startX"], record["endY"]) for record in result]
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results[self.headers_nosql[-4]] = real_time
            timing_results[self.headers_nosql[-3]] = cpu_time

            # Write results to the file
            start_time = os.times()
            with open(results_path, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(["startX", "endY"])
                writer.writerows(records)
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results[self.headers_nosql[-2]] = real_time
            timing_results[self.headers_nosql[-1]] = cpu_time

        # Write timing results to CSV file
        is_new_file = not timing_path.exists()
        with open(timing_path, 'a', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            if is_new_file:
                csv_writer.writerow(self.headers_nosql)
            csv_writer.writerow(
                [timing_results[header] for header in self.headers_nosql]
            )

        logging.info(f'(Neo4j) Experiment timing results saved to: {timing_path}')

        rm_cmd = f'sudo rm {neo4j_import_dir}/{fact_file_name}'

        try:
            child = pexpect.spawn(rm_cmd)
            child.expect('password')
            child.sendline(machine_user_password)
            child.expect(pexpect.EOF)
        except Exception as e:
            logging.error(f'Remove(rm) neo4j error: {e}')

    def analyze(self) -> None:
        output_folder = self.get_output_folder()
        logging.info(f'Output folder: {output_folder}')

        common_args = [self.input_path, self.rule_path, self.timing_path, output_folder]

        solve_method = getattr(self, f'solve_with_{self.environment}', None)

        if solve_method is None:
            logging.error(
                f"'{self.environment}' is not supported or method is missing."
            )
            return

        solve_method(*common_args)

        # Close the connection and delete the DuckDB file if applicable
        self.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--size', type=int, default=100, help='Size of the input graph. Default is 100.'
    )
    parser.add_argument(
        '--mode',
        type=str,
        default='right_recursion',
        help='Mode of the rule file to use. Default is right_recursion.',
    )
    parser.add_argument(
        '--graph-type', type=str, required=True, help='Type of graph to analyze'
    )
    parser.add_argument(
        '--environment',
        choices=DB_SYSTEMS,
        required=True,
        help='Database environment to use. Example is postgres or mariadb.',
    )
    args = parser.parse_args()

    config_path = Path('config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)

    rule_path, input_path, timing_path = get_files(
        args.environment, args.mode, args.graph_type, args.size
    )

    analyze_dbs = AnalyzeDBs(
        args.environment, rule_path, input_path, timing_path, config
    )
    analyze_dbs.connect(rule_path)
    analyze_dbs.analyze()


if __name__ == '__main__':
    gc.disable()
    main()
