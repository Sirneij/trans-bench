import argparse
import csv
import gc
import importlib
import json
import logging
import os
import subprocess
from typing import Any

import pexpect

from common import AnalyzeSystems

# Set up logging with a specific format
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s'
)


class AnalyzeDBs(AnalyzeSystems):
    def __init__(
        self,
        config: dict[str, Any],
        environment: str,
    ):
        super().__init__(config, environment)

    def solve_with_postgres(self) -> None:
        conn = self.connect_db(self.environment)
        # Extract the class name based on the rule file name
        module_name = self.rule_path.stem
        class_name = f'PostgreSQL{module_name.split("_")[1].capitalize()}Recursion'

        logging.info(
            f'Executing for PostgreSQL. Module: {module_name}, class: {class_name}'
        )

        # Dynamically import the appropriate module and class
        module = importlib.import_module(f'postgres_rules.{module_name}')
        PostgreSQLRecursionClass = getattr(module, class_name)
        postgres_operations = PostgreSQLRecursionClass(self.config, conn)

        results_path = self.output_folder / 'postgres_results.csv'
        timing_results = {header: 0 for header in self.headers_rdbms}

        try:
            # Drop tables in case they exist
            postgres_operations.drop_tc_path_tc_result_tables()

            # Create Table
            start_time = os.times()
            postgres_operations.create_tc_path_table()
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['CreateTableRealTime'] = real_time
            timing_results['CreateTableCPUTime'] = cpu_time

            # Insert Data
            start_time = os.times()
            postgres_operations.import_data_from_tsv('tc_path', f'{self.input_path}')
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['LoadDataRealTime'] = real_time
            timing_results['LoadDataCPUTime'] = cpu_time

            # Create Index
            start_time = os.times()
            postgres_operations.create_tc_path_index()
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['CreateIndexRealTime'] = real_time
            timing_results['CreateIndexCPUTime'] = cpu_time

            # Analyze table for query improvement
            start_time = os.times()
            postgres_operations.analyze_tc_path_table()
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['AnalyzeRealTime'] = real_time
            timing_results['AnalyzeCPUTime'] = cpu_time

            # Recursive Query
            start_time = os.times()
            postgres_operations.run_recursive_query()
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['ExecuteQueryRealTime'] = real_time
            timing_results['ExecuteQueryCPUTime'] = cpu_time

            # Export to CSV
            start_time = os.times()
            postgres_operations.export_transitive_closure_results(results_path)
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['WriteResultRealTime'] = real_time
            timing_results['WriteResultCPUTime'] = cpu_time

            # Drop used tables
            postgres_operations.drop_tc_path_tc_result_tables()

        except Exception as e:
            logging.error(f'PostgreSQL error: {e}')

        finally:
            conn.close()

        # Write timing results to CSV file
        is_new_file = not self.timing_path.exists()
        with open(self.timing_path, 'a', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            if is_new_file:
                csv_writer.writerow(self.headers_rdbms)
            csv_writer.writerow(
                [timing_results[header] for header in self.headers_rdbms]
            )

        logging.info(
            f'(PostgreSQL) Experiment timing results saved to: {self.timing_path}'
        )

    def solve_with_mariadb(self) -> None:
        conn = self.connect_db(self.environment)
        # Extract the class name based on the rule file name
        module_name = self.rule_path.stem
        class_name = f'MariaDB{module_name.split("_")[1].capitalize()}Recursion'

        logging.info(
            f'Executing for MariaDB. Module: {module_name}, class: {class_name}'
        )

        # Dynamically import the appropriate module and class
        module = importlib.import_module(f'mariadb_rules.{module_name}')
        MariaDBRecursionClass = getattr(module, class_name)
        mariadb_operations = MariaDBRecursionClass(self.config, conn)

        results_path = self.output_folder / 'mariadb_results.csv'
        timing_results = {header: 0 for header in self.headers_rdbms}

        try:
            # Drop tables in case they exist
            mariadb_operations.drop_tc_path_tc_result_tables()

            # Create Table
            start_time = os.times()
            mariadb_operations.create_tc_path_table()
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['CreateTableRealTime'] = real_time
            timing_results['CreateTableCPUTime'] = cpu_time

            # Insert Data
            start_time = os.times()
            mariadb_operations.import_data_from_file('tc_path', f'{self.input_path}')
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['LoadDataRealTime'] = real_time
            timing_results['LoadDataCPUTime'] = cpu_time

            # Create Index
            start_time = os.times()
            mariadb_operations.create_tc_path_index()
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['CreateIndexRealTime'] = real_time
            timing_results['CreateIndexCPUTime'] = cpu_time

            # Analyze table for query improvement
            start_time = os.times()
            mariadb_operations.analyze_tc_path_table()
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['AnalyzeRealTime'] = real_time
            timing_results['AnalyzeCPUTime'] = cpu_time

            # Recursive Query
            start_time = os.times()
            mariadb_operations.run_recursive_query()
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['ExecuteQueryRealTime'] = real_time
            timing_results['ExecuteQueryCPUTime'] = cpu_time

            # Export to CSV
            start_time = os.times()
            mariadb_operations.export_data_to_file()
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['WriteResultRealTime'] = real_time
            timing_results['WriteResultCPUTime'] = cpu_time

            # Drop used tables
            mariadb_operations.drop_tc_path_tc_result_tables()

        except Exception as e:
            logging.error(f'MariaDB error: {e}')

        finally:
            conn.close()

        # Write timing results to CSV file
        is_new_file = not self.timing_path.exists()
        with open(self.timing_path, 'a', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            if is_new_file:
                csv_writer.writerow(self.headers_rdbms)
            csv_writer.writerow(
                [timing_results[header] for header in self.headers_rdbms]
            )

        # NOTE: There is an issue with mariadb that prevents programs from writing into any
        #  directory of choice even after I set `secure_file_priv = ""` in `~/.my.cnf`,
        # `/etc/mysql/my.cnf` in my Ubuntu (`~/.my.ini` for Windows)
        # but I could write into `/tmp/` so I did that temporarily and thereafter move
        # it to the desired file location using the lines below.
        machine_user_password = self.config['machineUserPassword']
        cp_cmd = f'cp /tmp/mariadb_results.csv {results_path}'
        rm_cmd = f'sudo rm -rf /tmp/mariadb_results.csv'
        # Run the command and pass the password
        try:
            subprocess.run(
                cp_cmd, text=True, capture_output=True, shell=True, check=True
            )
        except Exception as e:
            logging.error(f'Copy (cp) `/tmp/` error: {e}')

        try:
            child = pexpect.spawn(rm_cmd)
            child.expect('password')
            child.sendline(machine_user_password)
            child.expect(pexpect.EOF)
        except Exception as e:
            logging.error(f'Remove(rm) `/tmp/` error: {e}')

    def solve_with_duckdb(self) -> None:
        conn = self.connect_db(self.environment, self.rule_path)
        with open(self.rule_path, 'r') as f:
            sql_script = f.read()

        results_path = self.output_folder / 'duckdb_results.csv'
        sql_script = sql_script.replace('{data_file}', f'{self.input_path}')
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
        is_new_file = not self.timing_path.exists()
        with open(self.timing_path, 'a', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            if is_new_file:
                csv_writer.writerow(self.headers_rdbms)
            csv_writer.writerow(
                [timing_results[header] for header in self.headers_rdbms]
            )

    def solve_with_neo4j(self) -> None:
        self.driver = self.connect_db(self.environment)
        results_path = self.output_folder / 'neo4j_results.csv'

        machine_user_password = self.config['machineUserPassword']
        neo4j_import_dir = self.config['neo4j']['import_directory']
        fact_file_name = os.path.basename(self.input_path)

        cp_cmd = f'sudo cp {self.input_path.resolve()} {neo4j_import_dir}/'

        try:
            child = pexpect.spawn(cp_cmd)
            child.expect('password')
            child.sendline(machine_user_password)
            child.expect(pexpect.EOF)
        except Exception as e:
            logging.error(f'Copy(cp) neo4j error: {e}')

        with open(self.rule_path, 'r') as f:
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
        is_new_file = not self.timing_path.exists()
        with open(self.timing_path, 'a', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            if is_new_file:
                csv_writer.writerow(self.headers_nosql)
            csv_writer.writerow(
                [timing_results[header] for header in self.headers_nosql]
            )

        logging.info(f'(Neo4j) Experiment timing results saved to: {self.timing_path}')

        rm_cmd = f'sudo rm {neo4j_import_dir}/{fact_file_name}'

        try:
            child = pexpect.spawn(rm_cmd)
            child.expect('password')
            child.sendline(machine_user_password)
            child.expect(pexpect.EOF)
        except Exception as e:
            logging.error(f'Remove(rm) neo4j error: {e}')

    def solve_with_mongodb(self) -> None:
        db = self.connect_db(self.environment)
        # Extract the class name based on the rule file name
        module_name = self.rule_path.stem
        class_name = f'MongoDB{module_name.split("_")[1].capitalize()}Recursion'

        logging.info(
            f'Executing for MongoDB. Module: {module_name}, class: {class_name}'
        )

        # Dynamically import the appropriate module and class
        module = importlib.import_module(f'mongodb_rules.{module_name}')
        MongoDBRecursionClass = getattr(module, class_name)
        mongo_operations = MongoDBRecursionClass(self.config, db)

        results_path = self.output_folder / 'mongodb_results.csv'
        timing_results = {header: 0 for header in self.headers_mongodb}

        try:
            # Create Collection (equivalent to Create Table)
            start_time = os.times()
            mongo_operations.create_collection('tc_path', 'tc_result')
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['CreateTableRealTime'] = real_time
            timing_results['CreateTableCPUTime'] = cpu_time

            # Insert Data (equivalent to COPY)
            start_time = os.times()
            mongo_operations.insert_data('tc_path', self.input_path)
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['LoadDataRealTime'] = real_time
            timing_results['LoadDataCPUTime'] = cpu_time

            # Create Index
            start_time = os.times()
            mongo_operations.create_index('tc_path', 'y')
            mongo_operations.create_index('tc_path', 'x')
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['CreateIndexRealTime'] = real_time
            timing_results['CreateIndexCPUTime'] = cpu_time

            # Recursive Query
            start_time = os.times()
            mongo_operations.recursive_query('tc_path', 'tc_result')
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['ExecuteQueryRealTime'] = real_time
            timing_results['ExecuteQueryCPUTime'] = cpu_time

            # Export to CSV
            start_time = os.times()
            mongo_operations.export_to_csv('tc_result', results_path)
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['WriteResultRealTime'] = real_time
            timing_results['WriteResultCPUTime'] = cpu_time

        except Exception as e:
            logging.error(f'MongoDB error: {e}')

        # Write timing results to CSV file
        is_new_file = not self.timing_path.exists()
        with open(self.timing_path, 'a', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            if is_new_file:
                csv_writer.writerow(self.headers_mongodb)
            csv_writer.writerow(
                [timing_results[header] for header in self.headers_mongodb]
            )

        logging.info(
            f'(MongoDB) Experiment timing results saved to: {self.timing_path}'
        )

    def solve_with_cockroachdb(self) -> None:
        conn = self.connect_db(self.environment)
        # Extract the class name based on the rule file name
        module_name = self.rule_path.stem
        class_name = f'CockroachDB{module_name.split("_")[1].capitalize()}Recursion'

        logging.info(
            f'Executing for CockroachDB. Module: {module_name}, class: {class_name}'
        )

        # Dynamically import the appropriate module and class
        module = importlib.import_module(f'cockroachdb_rules.{module_name}')
        CockroachDBRecursionClass = getattr(module, class_name)
        cockroachdb_operations = CockroachDBRecursionClass(self.config, conn)

        results_path = self.output_folder / 'cockroachdb_results.csv'
        timing_results = {header: 0 for header in self.headers_rdbms}

        try:
            # Drop tables in case they exist
            cockroachdb_operations.drop_tc_path_tc_result_tables()

            external_directory = self.config[self.environment]["externalDirectory"]

            # Create Table
            start_time = os.times()
            cockroachdb_operations.create_tc_path_table()
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['CreateTableRealTime'] = real_time
            timing_results['CreateTableCPUTime'] = cpu_time

            # Create directory and copy input file
            cmd = f'mkdir -p {external_directory} && cp {self.input_path} {external_directory}'
            subprocess.run(cmd, shell=True, text=True, capture_output=True, check=True)

            filename = f'{self.input_path.stem + self.input_path.suffix}'

            # Insert Data
            start_time = os.times()
            cockroachdb_operations.import_data_from_tsv('tc_path', filename)
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['LoadDataRealTime'] = real_time
            timing_results['LoadDataCPUTime'] = cpu_time

            # Delete input file
            cmd = f'rm {external_directory}{filename}'
            subprocess.run(cmd, shell=True, text=True, capture_output=True, check=True)

            # Create Index
            start_time = os.times()
            cockroachdb_operations.create_tc_path_index()
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['CreateIndexRealTime'] = real_time
            timing_results['CreateIndexCPUTime'] = cpu_time

            # Analyze table for query improvement
            start_time = os.times()
            cockroachdb_operations.analyze_tc_path_table()
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['AnalyzeRealTime'] = real_time
            timing_results['AnalyzeCPUTime'] = cpu_time

            # Recursive Query
            start_time = os.times()
            cockroachdb_operations.run_recursive_query()
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['ExecuteQueryRealTime'] = real_time
            timing_results['ExecuteQueryCPUTime'] = cpu_time

            # Export to CSV
            start_time = os.times()
            cockroachdb_operations.export_transitive_closure_results(results_path)
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['WriteResultRealTime'] = real_time
            timing_results['WriteResultCPUTime'] = cpu_time

            # Drop used tables
            cockroachdb_operations.drop_tc_path_tc_result_tables()

            # Move output file to the desired location
            cp_cmd = f'cp {external_directory}tmp/*.csv {results_path}'
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

        # Write timing results to CSV file
        is_new_file = not self.timing_path.exists()
        with open(self.timing_path, 'a', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            if is_new_file:
                csv_writer.writerow(self.headers_rdbms)
            csv_writer.writerow(
                [timing_results[header] for header in self.headers_rdbms]
            )

        logging.info(
            f'(CockroachDB) Experiment timing results saved to: {self.timing_path}'
        )

    def solve_with_singlestoredb(self) -> None:
        conn = self.connect_db(self.environment)
        # Extract the class name based on the rule file name
        module_name = self.rule_path.stem
        class_name = f'SingleStore{module_name.split("_")[1].capitalize()}Recursion'

        logging.info(
            f'Executing for SingleStore. Module: {module_name}, class: {class_name}'
        )

        # Dynamically import the appropriate module and class
        module = importlib.import_module(f'singlestoredb_rules.{module_name}')
        SingleStoreRecursionClass = getattr(module, class_name)
        singlestoredb_operations = SingleStoreRecursionClass(self.config, conn)

        results_path = self.output_folder / 'singlestore_results.csv'
        timing_results = {header: 0 for header in self.headers_rdbms}

        try:
            # Drop tables in case they exist
            singlestoredb_operations.drop_tc_path_tc_result_tables()

            # Create Table
            start_time = os.times()
            singlestoredb_operations.create_tc_path_table()
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['CreateTableRealTime'] = real_time
            timing_results['CreateTableCPUTime'] = cpu_time

            # Insert Data
            start_time = os.times()
            singlestoredb_operations.import_data_from_file(
                'tc_path', f'{self.input_path}'
            )
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['LoadDataRealTime'] = real_time
            timing_results['LoadDataCPUTime'] = cpu_time

            # Create Index
            start_time = os.times()
            singlestoredb_operations.create_tc_path_index()
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['CreateIndexRealTime'] = real_time
            timing_results['CreateIndexCPUTime'] = cpu_time

            # Analyze table for query improvement
            start_time = os.times()
            singlestoredb_operations.analyze_tc_path_table()
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['AnalyzeRealTime'] = real_time
            timing_results['AnalyzeCPUTime'] = cpu_time

            # Recursive Query
            start_time = os.times()
            singlestoredb_operations.run_recursive_query()
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['ExecuteQueryRealTime'] = real_time
            timing_results['ExecuteQueryCPUTime'] = cpu_time

            # Export to CSV
            start_time = os.times()
            singlestoredb_operations.export_data_to_file()
            end_time = os.times()
            real_time, cpu_time = self.estimate_time_duration(start_time, end_time)
            timing_results['WriteResultRealTime'] = real_time
            timing_results['WriteResultCPUTime'] = cpu_time

            # Drop used tables
            singlestoredb_operations.drop_tc_path_tc_result_tables()

        except Exception as e:
            logging.error(f'SingleStoreDB error: {e}')

        finally:
            conn.close()

        # Write timing results to CSV file
        is_new_file = not self.timing_path.exists()
        with open(self.timing_path, 'a', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            if is_new_file:
                csv_writer.writerow(self.headers_rdbms)
            csv_writer.writerow(
                [timing_results[header] for header in self.headers_rdbms]
            )

        # NOTE: There is an issue with mariadb that prevents programs from writing into any
        #  directory of choice even after I set `secure_file_priv = ""` in `~/.my.cnf`,
        # `/etc/mysql/my.cnf` in my Ubuntu (`~/.my.ini` for Windows)
        # but I could write into `/tmp/` so I did that temporarily and thereafter move
        # it to the desired file location using the lines below.
        machine_user_password = self.config['machineUserPassword']
        cp_cmd = f'cp /tmp/singlestore_results.csv {results_path}'
        rm_cmd = f'sudo rm -rf /tmp/singlestore_results.csv'
        # Run the command and pass the password
        try:
            subprocess.run(
                cp_cmd, text=True, capture_output=True, shell=True, check=True
            )
        except Exception as e:
            logging.error(f'Copy (cp) `/tmp/` error: {e}')

        try:
            child = pexpect.spawn(rm_cmd)
            child.expect('password')
            child.sendline(machine_user_password)
            child.expect(pexpect.EOF)
        except Exception as e:
            logging.error(f'Remove(rm) `/tmp/` error: {e}')

    def analyze(self) -> None:
        solve_method = getattr(self, f'solve_with_{self.environment}', None)

        if solve_method is None:
            logging.error(
                f"'{self.environment}' is not supported or method is missing."
            )
            return

        solve_method()

        # Close the connection and delete the DuckDB file if applicable
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
