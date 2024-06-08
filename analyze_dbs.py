import argparse
import csv
import gc
import logging
import subprocess
from pathlib import Path

from common import AnalyzeSystems, get_files

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
    ):
        super().__init__(environment, rule_path, input_path, timing_path)

    def solve_with_postgres(
        self,
        fact_file: Path,
        rule_file: Path,
        timing_path: Path,
        output_folder: Path,
    ) -> None:

        # Read the SQL script and substitute the placeholders
        with open(rule_file, 'r') as f:
            sql_script = f.read()

        # Substitute the placeholders with actual file paths
        results_path = output_folder / 'pg_results.csv'
        sql_script = sql_script.replace('{data_file}', f'{fact_file}')
        sql_script = sql_script.replace('{output_file}', f'{results_path}')

        # Write the modified script to a temporary file
        temp_file = rule_file.parent / f'{rule_file.stem}_temp.sql'
        with open(temp_file, 'w') as f:
            f.write(sql_script)

        # Execute the script using psql
        command = ['psql', '-f', temp_file]

        result = subprocess.run(command, capture_output=True, text=True)

        # Parse the timing information
        timings = self.parse_postgresql_timings(result.stdout)

        # Write the timing data to the output CSV file
        is_new_file = not timing_path.exists()
        with open(timing_path, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            if is_new_file:
                writer.writerow(
                    [
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
                        'WriteResultRealTIme',
                        'WriteResultCPUTIme',
                    ]
                )
            writer.writerow(
                [
                    timings['CreateTable'],
                    0.0,
                    timings['LoadData'],
                    0.0,
                    timings['CreateIndex'],
                    0.0,
                    timings['Analyze'],
                    0.0,
                    timings['ExecuteQuery'],
                    0.0,
                    timings['WriteResult'],
                    0.0,
                ]
            )

        logging.info(f'(PostgreSQL) Experiment timing results saved to: {timing_path}')

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
        choices=['clingo', 'xsb', 'souffle', 'postgres'],
        required=True,
        help='Logic programming environment to use. Choose from clingo, xsb, postgres, or souffle.',
    )
    args = parser.parse_args()

    rule_path, input_path, timing_path = get_files(
        args.environment, args.mode, args.graph_type, args.size
    )

    analyze_dbs = AnalyzeDBs(
        args.environment, rule_path, input_path, timing_path
    )
    analyze_dbs.analyze()


if __name__ == '__main__':
    gc.disable()
    main()
