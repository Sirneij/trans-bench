import argparse
import csv
import gc
import importlib
import json
import logging
import os
import subprocess
from typing import Any, Dict

import pexpect

from common import AnalyzeSystems

# Set up logging with a specific format
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s'
)


class AnalyzeDBs(AnalyzeSystems):
    def __init__(self, config: Dict[str, Any], environment: str):
        super().__init__(config, environment)

    def execute_with_timing(self, operation: callable, *args, **kwargs) -> tuple:
        """Execute an operation and measure its real and CPU time."""
        start_time = os.times()
        operation(*args, **kwargs)
        end_time = os.times()
        return self.estimate_time_duration(start_time, end_time)

    def dynamic_import(self, module_name: str, class_name: str) -> Any:
        """Dynamically import a class from a module."""
        module = importlib.import_module(module_name)
        return getattr(module, class_name)

    def write_timing_results(
        self, timing_results: Dict[str, float], headers: list[str]
    ) -> None:
        """Write timing results to CSV."""
        is_new_file = not self.timing_path.exists()
        with open(self.timing_path, 'a', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            if is_new_file:
                csv_writer.writerow(headers)
            csv_writer.writerow([timing_results[header] for header in headers])
        logging.info(f'Timing results saved to: {self.timing_path}')

    def solve_with_postgres(self) -> None:
        conn = self.connect_db(self.environment)
        module_name = self.rule_path.stem
        class_name = f'PostgreSQL{module_name.split("_")[1].capitalize()}Recursion'
        logging.info(
            f'Executing for PostgreSQL. Module: {module_name}, class: {class_name}'
        )

        PostgreSQLRecursionClass = self.dynamic_import(
            f'postgres_rules.{module_name}', class_name
        )
        postgres_operations = PostgreSQLRecursionClass(self.config, conn)

        results_path = self.output_folder / 'postgres_results.csv'
        timing_results = {header: 0 for header in self.headers_rdbms}

        try:
            postgres_operations.drop_tc_path_tc_result_tables()
            (
                timing_results['CreateTableRealTime'],
                timing_results['CreateTableCPUTime'],
            ) = self.execute_with_timing(postgres_operations.create_tc_path_table)
            timing_results['LoadDataRealTime'], timing_results['LoadDataCPUTime'] = (
                self.execute_with_timing(
                    postgres_operations.import_data_from_tsv,
                    'edge',
                    f'{self.input_path}',
                )
            )
            (
                timing_results['CreateIndexRealTime'],
                timing_results['CreateIndexCPUTime'],
            ) = self.execute_with_timing(postgres_operations.create_tc_path_index)
            timing_results['AnalyzeRealTime'], timing_results['AnalyzeCPUTime'] = (
                self.execute_with_timing(postgres_operations.analyze_tc_path_table)
            )
            (
                timing_results['ExecuteQueryRealTime'],
                timing_results['ExecuteQueryCPUTime'],
            ) = self.execute_with_timing(postgres_operations.run_recursive_query)
            (
                timing_results['WriteResultRealTime'],
                timing_results['WriteResultCPUTime'],
            ) = self.execute_with_timing(
                postgres_operations.export_transitive_closure_results, results_path
            )
            postgres_operations.drop_tc_path_tc_result_tables()
        except Exception as e:
            logging.error(f'PostgreSQL error: {e}')
        finally:
            conn.close()

        self.write_timing_results(timing_results, self.headers_rdbms)

    def solve_with_mariadb(self) -> None:
        conn = self.connect_db(self.environment)
        module_name = self.rule_path.stem
        class_name = f'MariaDB{module_name.split("_")[1].capitalize()}Recursion'
        logging.info(
            f'Executing for MariaDB. Module: {module_name}, class: {class_name}'
        )

        MariaDBRecursionClass = self.dynamic_import(
            f'mariadb_rules.{module_name}', class_name
        )
        mariadb_operations = MariaDBRecursionClass(self.config, conn)

        results_path = self.output_folder / 'mariadb_results.csv'
        timing_results = {header: 0 for header in self.headers_rdbms}

        try:
            mariadb_operations.drop_tc_path_tc_result_tables()
            (
                timing_results['CreateTableRealTime'],
                timing_results['CreateTableCPUTime'],
            ) = self.execute_with_timing(mariadb_operations.create_tc_path_table)
            timing_results['LoadDataRealTime'], timing_results['LoadDataCPUTime'] = (
                self.execute_with_timing(
                    mariadb_operations.import_data_from_file,
                    'edge',
                    f'{self.input_path}',
                )
            )
            (
                timing_results['CreateIndexRealTime'],
                timing_results['CreateIndexCPUTime'],
            ) = self.execute_with_timing(mariadb_operations.create_tc_path_index)
            timing_results['AnalyzeRealTime'], timing_results['AnalyzeCPUTime'] = (
                self.execute_with_timing(mariadb_operations.analyze_tc_path_table)
            )
            (
                timing_results['ExecuteQueryRealTime'],
                timing_results['ExecuteQueryCPUTime'],
            ) = self.execute_with_timing(mariadb_operations.run_recursive_query)
            (
                timing_results['WriteResultRealTime'],
                timing_results['WriteResultCPUTime'],
            ) = self.execute_with_timing(mariadb_operations.export_data_to_file)
            mariadb_operations.drop_tc_path_tc_result_tables()
        except Exception as e:
            logging.error(f'MariaDB error: {e}')
        finally:
            conn.close()

        self.write_timing_results(timing_results, self.headers_rdbms)

        self.copy_file('/tmp/mariadb_results.csv', results_path)
        self.remove_file('/tmp/mariadb_results.csv')

    def solve_with_duckdb(self) -> None:
        conn = self.connect_db(self.environment, self.rule_path)
        with open(self.rule_path, 'r') as f:
            sql_script = f.read()

        results_path = self.output_folder / 'duckdb_results.csv'
        sql_script = sql_script.replace('{data_file}', f'{self.input_path}')
        sql_script = sql_script.replace('{output_file}', f'{results_path}')

        sql_commands = [
            f'{command.strip()};'
            for command in sql_script.split(';')
            if command.strip()
        ]
        timing_results = {header: 0 for header in self.headers_rdbms}

        for i, command in enumerate(sql_commands):
            try:
                (
                    timing_results[self.headers_rdbms[2 * i]],
                    timing_results[self.headers_rdbms[2 * i + 1]],
                ) = self.execute_with_timing(conn.execute, command)
            except Exception as e:
                logging.error(f'Error executing command: {command}. Error: {e}')

        self.write_timing_results(timing_results, self.headers_rdbms)

    def solve_with_neo4j(self) -> None:
        self.driver = self.connect_db(self.environment)
        results_path = self.output_folder / 'neo4j_results.csv'

        machine_user_password = self.config['machineUserPassword']
        neo4j_import_dir = self.config['neo4j']['import_directory']
        fact_file_name = os.path.basename(self.input_path)

        self.run_pexpect_command(
            f'sudo cp {self.input_path.resolve()} {neo4j_import_dir}/',
            machine_user_password,
        )

        with open(self.rule_path, 'r') as f:
            cypher_script = f.read()

        cypher_script = cypher_script.replace('{data_file}', str(fact_file_name))
        commands = [
            f'{command.strip()};'
            for command in cypher_script.split(';')
            if command.strip()
        ]
        timing_results = {header: 0 for header in self.headers_neo4j}

        try:

            with self.driver.session() as session:
                for i, command in enumerate(commands[:-1]):
                    (
                        timing_results[self.headers_neo4j[2 * i]],
                        timing_results[self.headers_neo4j[2 * i + 1]],
                    ) = self.execute_with_timing(session.run, command)

                    logging.info(
                        f'Command: {command}. Time: {self.headers_neo4j[2 * i]}, {self.headers_neo4j[2 * i + 1]}'
                    )

                query = commands[-1]
                start_time = os.times()
                result = session.run(query)
                end_time = os.times()
                (
                    timing_results[self.headers_neo4j[-4]],
                    timing_results[self.headers_neo4j[-3]],
                ) = self.estimate_time_duration(start_time, end_time)
                logging.info(
                    f'Command: {query}, Times: {self.headers_neo4j[-4]}, {self.headers_neo4j[-3]}'
                )

                start_time = os.times()
                records = [(record['startX'], record['endY']) for record in result]
                with open(results_path, 'w', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(['startX', 'endY'])
                    writer.writerows(records)
                end_time = os.times()
                (
                    timing_results[self.headers_neo4j[-2]],
                    timing_results[self.headers_neo4j[-1]],
                ) = self.estimate_time_duration(start_time, end_time)
                logging.info(
                    f'Times: {self.headers_neo4j[-2]}, {self.headers_neo4j[-1]}'
                )

        except Exception as e:
            logging.error(f'Neo4J error: {e}')

        self.write_timing_results(timing_results, self.headers_neo4j)

        self.run_pexpect_command(
            f'sudo rm {neo4j_import_dir}/{fact_file_name}', machine_user_password
        )

    def solve_with_mongodb(self) -> None:
        db = self.connect_db(self.environment)
        module_name = self.rule_path.stem
        class_name = f'MongoDB{module_name.split("_")[1].capitalize()}Recursion'
        logging.info(
            f'Executing for MongoDB. Module: {module_name}, class: {class_name}'
        )

        MongoDBRecursionClass = self.dynamic_import(
            f'mongodb_rules.{module_name}', class_name
        )
        mongo_operations = MongoDBRecursionClass(self.config, db)

        results_path = self.output_folder / 'mongodb_results.csv'
        timing_results = {header: 0 for header in self.headers_mongodb}

        try:
            (
                timing_results['CreateTableRealTime'],
                timing_results['CreateTableCPUTime'],
            ) = self.execute_with_timing(
                mongo_operations.create_collection, 'edge', 'tc_result'
            )
            timing_results['LoadDataRealTime'], timing_results['LoadDataCPUTime'] = (
                self.execute_with_timing(
                    mongo_operations.insert_data, 'edge', self.input_path
                )
            )
            (
                timing_results['CreateIndexRealTime'],
                timing_results['CreateIndexCPUTime'],
            ) = self.execute_with_timing(mongo_operations.create_index, 'edge')
            (
                timing_results['ExecuteQueryRealTime'],
                timing_results['ExecuteQueryCPUTime'],
            ) = self.execute_with_timing(
                mongo_operations.recursive_query, 'edge', 'tc_result'
            )
            (
                timing_results['WriteResultRealTime'],
                timing_results['WriteResultCPUTime'],
            ) = self.execute_with_timing(
                mongo_operations.export_to_csv, 'tc_result', results_path
            )
        except Exception as e:
            logging.error(f'MongoDB error: {e}')

        self.write_timing_results(timing_results, self.headers_mongodb)
        logging.info(
            f'(MongoDB) Experiment timing results saved to: {self.timing_path}'
        )

    def solve_with_cockroachdb(self) -> None:
        conn = self.connect_db(self.environment)
        module_name = self.rule_path.stem
        class_name = f'CockroachDB{module_name.split("_")[1].capitalize()}Recursion'
        logging.info(
            f'Executing for CockroachDB. Module: {module_name}, class: {class_name}'
        )

        CockroachDBRecursionClass = self.dynamic_import(
            f'cockroachdb_rules.{module_name}', class_name
        )
        cockroachdb_operations = CockroachDBRecursionClass(self.config, conn)

        results_path = self.output_folder / 'cockroachdb_results.csv'
        timing_results = {header: 0 for header in self.headers_rdbms}

        try:
            cockroachdb_operations.drop_tc_path_tc_result_tables()
            external_directory = self.config[self.environment]["externalDirectory"]

            (
                timing_results['CreateTableRealTime'],
                timing_results['CreateTableCPUTime'],
            ) = self.execute_with_timing(cockroachdb_operations.create_tc_path_table)

            cmd = f'mkdir -p {external_directory} && cp {self.input_path} {external_directory}'
            subprocess.run(cmd, shell=True, text=True, capture_output=True, check=True)

            filename = f'{self.input_path.stem + self.input_path.suffix}'
            timing_results['LoadDataRealTime'], timing_results['LoadDataCPUTime'] = (
                self.execute_with_timing(
                    cockroachdb_operations.import_data_from_tsv, 'edge', filename
                )
            )

            cmd = f'rm {external_directory}{filename}'
            subprocess.run(cmd, shell=True, text=True, capture_output=True, check=True)

            (
                timing_results['CreateIndexRealTime'],
                timing_results['CreateIndexCPUTime'],
            ) = self.execute_with_timing(cockroachdb_operations.create_tc_path_index)
            timing_results['AnalyzeRealTime'], timing_results['AnalyzeCPUTime'] = (
                self.execute_with_timing(cockroachdb_operations.analyze_tc_path_table)
            )
            (
                timing_results['ExecuteQueryRealTime'],
                timing_results['ExecuteQueryCPUTime'],
            ) = self.execute_with_timing(cockroachdb_operations.run_recursive_query)
            (
                timing_results['WriteResultRealTime'],
                timing_results['WriteResultCPUTime'],
            ) = self.execute_with_timing(
                cockroachdb_operations.export_transitive_closure_results, results_path
            )

            cockroachdb_operations.drop_tc_path_tc_result_tables()

            external_dir = f'{external_directory}tmp/*.csv'

            logging.info(f'External directory: {external_dir}')
            cp_cmd = f'cp {external_dir} {results_path}'
            rm_cmd = f'rm -r {external_directory}tmp'
            subprocess.run(
                cp_cmd, shell=True, text=True, capture_output=True, check=True
            )
            subprocess.run(
                rm_cmd, shell=True, text=True, capture_output=True, check=True
            )
        except Exception as e:
            logging.error(f'CockroachDB error: {e}')
        finally:
            conn.close()

        self.write_timing_results(timing_results, self.headers_rdbms)
        logging.info(
            f'(CockroachDB) Experiment timing results saved to: {self.timing_path}'
        )

    def solve_with_singlestoredb(self) -> None:
        conn = self.connect_db(self.environment)
        module_name = self.rule_path.stem
        class_name = f'SingleStore{module_name.split("_")[1].capitalize()}Recursion'
        logging.info(
            f'Executing for SingleStore. Module: {module_name}, class: {class_name}'
        )

        SingleStoreRecursionClass = self.dynamic_import(
            f'singlestoredb_rules.{module_name}', class_name
        )
        singlestoredb_operations = SingleStoreRecursionClass(self.config, conn)

        results_path = self.output_folder / 'singlestore_results.csv'
        timing_results = {header: 0 for header in self.headers_rdbms}

        try:
            singlestoredb_operations.drop_tc_path_tc_result_tables()
            (
                timing_results['CreateTableRealTime'],
                timing_results['CreateTableCPUTime'],
            ) = self.execute_with_timing(singlestoredb_operations.create_tc_path_table)
            timing_results['LoadDataRealTime'], timing_results['LoadDataCPUTime'] = (
                self.execute_with_timing(
                    singlestoredb_operations.import_data_from_file,
                    'edge',
                    f'{self.input_path}',
                )
            )
            (
                timing_results['CreateIndexRealTime'],
                timing_results['CreateIndexCPUTime'],
            ) = self.execute_with_timing(singlestoredb_operations.create_tc_path_index)
            timing_results['AnalyzeRealTime'], timing_results['AnalyzeCPUTime'] = (
                self.execute_with_timing(singlestoredb_operations.analyze_tc_path_table)
            )
            (
                timing_results['ExecuteQueryRealTime'],
                timing_results['ExecuteQueryCPUTime'],
            ) = self.execute_with_timing(singlestoredb_operations.run_recursive_query)
            (
                timing_results['WriteResultRealTime'],
                timing_results['WriteResultCPUTime'],
            ) = self.execute_with_timing(singlestoredb_operations.export_data_to_file)
            singlestoredb_operations.drop_tc_path_tc_result_tables()
        except Exception as e:
            logging.error(f'SingleStoreDB error: {e}')
        finally:
            conn.close()

        self.write_timing_results(timing_results, self.headers_rdbms)
        self.copy_file('/tmp/singlestore_results.csv', results_path)
        self.remove_file('/tmp/singlestore_results.csv')

    def run_pexpect_command(self, command: str, password: str) -> None:
        """Run a shell command using pexpect with password input."""
        try:
            child = pexpect.spawn(command)
            child.expect('password')
            child.sendline(password)
            child.expect(pexpect.EOF)
        except Exception as e:
            logging.error(f'Command `{command}` error: {e}')

    def copy_file(self, source: str, destination: str) -> None:
        """Copy file from source to destination."""
        try:
            subprocess.run(
                f'cp {source} {destination}',
                text=True,
                capture_output=True,
                shell=True,
                check=True,
            )
        except Exception as e:
            logging.error(f'Copy (cp) error: {e}')

    def remove_file(self, path: str) -> None:
        """Remove file at specified path."""
        try:
            subprocess.run(
                f'sudo rm -rf {path}',
                text=True,
                capture_output=True,
                shell=True,
                check=True,
            )
        except Exception as e:
            logging.error(f'Remove (rm) error: {e}')

    def analyze(self) -> None:
        solve_method = getattr(self, f'solve_with_{self.environment}', None)
        if solve_method is None:
            logging.error(
                f"'{self.environment}' is not supported or method is missing."
            )
            return
        solve_method()
        self.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--config', type=str, required=True, help='JSON string of the config'
    )
    parser.add_argument(
        '--size', type=int, default=100, help='Size of the input graph. Default is 100.'
    )
    parser.add_argument(
        '--mode',
        type=str,
        required=True,
        help='Mode of the rule file to use. Default is right_recursion.',
    )
    parser.add_argument(
        '--graph-type', type=str, required=True, help='Type of graph to analyze'
    )
    parser.add_argument(
        '--environment',
        required=True,
        help='Database environment to use. Example is postgres or mariadb.',
    )
    args = parser.parse_args()

    config = json.loads(args.config)

    analyze_dbs = AnalyzeDBs(config, args.environment)
    analyze_dbs.set_file_paths(args.mode, args.graph_type, args.size)
    analyze_dbs.set_output_folder()
    analyze_dbs.analyze()


if __name__ == '__main__':
    gc.disable()
    main()
