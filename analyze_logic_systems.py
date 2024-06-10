import argparse
import csv
import gc
import json
import logging
import os
import re
import subprocess
from pathlib import Path
from typing import Any

import clingo

from common import AnalyzeSystems

# Set up logging with a specific format
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s'
)


class AnalyzeLogicSystems(AnalyzeSystems):
    def __init__(
        self,
        config: dict[str, Any],
        environment: str,
        souffle_include_dir: str,
    ):
        super().__init__(config, environment)
        self.souffle_include_dir = souffle_include_dir
        self.queries = (
            "[[query1, path(X, Y)]]"  # Each query is a list: [Identifier, Query]
        )

    def analyze(self):
        """
        Executes a query using the specified environment and logs the time taken for various stages.

        This function checks the environment and calls the appropriate function to execute a query and log the time taken for various stages.
        """
        solve_method = getattr(self, f'solve_with_{self.environment}', None)

        if solve_method is None:
            logging.error(
                f"'{self.environment}' is not supported or method is missing."
            )
            return

        solve_method()

    def solve_with_xsb(self) -> None:
        """
        Executes a query using XSB Prolog and logs the time taken for loading and querying.

        This function prepares and runs a command to execute a query using XSB Prolog. It captures the output, extracts the loading and querying times,
        and writes these times to a CSV file.
        """
        logging.info(
            f"Running XSB with fact file {self.input_path}, rule file {self.rule_path}, and queries {self.queries}"
        )

        xsb_export_path = self.rule_path.parent / 'xsb_export'
        results_path = self.output_folder / 'xsb_results.txt'

        # Prepare the XSB command
        xsb_query_1 = f"extfilequery:external_file_query_only('{self.rule_path}','{self.input_path}',{self.queries},'{results_path}')."
        xsb_query_2 = f"extfilequery:external_file_query('{self.rule_path}','{self.input_path}',{self.queries},'{results_path}')."
        xsb_command_1 = [
            'xsb',
            '--nobanner',
            '--quietload',
            '--noprompt',
            '-e',
            f"add_lib_dir('{xsb_export_path}').",
            '-e',
            xsb_query_1,
        ]

        xsb_command_2 = [
            'xsb',
            '--nobanner',
            '--quietload',
            '--noprompt',
            '-e',
            f"add_lib_dir('{xsb_export_path}').",
            '-e',
            xsb_query_2,
        ]

        # Run the command and capture the output
        output = subprocess.run(xsb_command_1, stdout=subprocess.PIPE, text=True)
        output_2 = subprocess.run(xsb_command_2, stdout=subprocess.PIPE, text=True)

        logging.info(f'(XSB) Command output: {output.stdout}')
        logging.info(f'(XSB) Error output: {output.stderr}')
        logging.info(f'(XSB) Command output 2: {output_2.stdout}')
        logging.info(f'(XSB) Error output 2: {output_2.stderr}')

        load_rule_time = re.search(
            r'LoadRuleTime:\s+(-?\d+\.\d+(?:e[+-]?\d+)?)', output.stdout
        )
        cpu_load_rule_time = re.search(
            r'CPULoadRuleTime:\s+(-?\d+\.\d+(?:e[+-]?\d+)?)', output.stdout
        )
        load_facts_time = re.search(
            r'LoadFactsTime:\s+(-?\d+\.\d+(?:e[+-]?\d+)?)', output.stdout
        )
        cpu_load_facts_time = re.search(
            r'CPULoadFactsTime:\s+(-?\d+\.\d+(?:e[+-]?\d+)?)', output.stdout
        )
        query_time = re.search(
            r'QueryOnlyTime:\s+(-?\d+\.\d+(?:e[+-]?\d+)?)', output.stdout
        )
        cpu_query_time = re.search(
            r'CPUQueryOnlyTime:\s+(-?\d+\.\d+(?:e[+-]?\d+)?)', output.stdout
        )
        query_write_time = re.search(
            r'QueryAndWriteTime:\s+(-?\d+\.\d+(?:e[+-]?\d+)?)', output_2.stdout
        )
        query_cpu_write_time = re.search(
            r'CPUTimeQueryAndWriteTime:\s+(-?\d+\.\d+(?:e[+-]?\d+)?)', output_2.stdout
        )

        write_time = float(query_write_time.group(1)) - float(query_time.group(1))
        cpu_write_time = float(query_cpu_write_time.group(1)) - float(
            cpu_query_time.group(1)
        )

        is_new_file = not self.timing_path.exists()
        with open(self.timing_path, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            if is_new_file:
                writer.writerow(
                    [
                        'LoadRulesRealTime',
                        'LoadRulesCPUTime',
                        'LoadFactsRealTime',
                        'LoadFactsCPUTime',
                        'QueryRealTime',
                        'QueryCPUTime',
                        'WriteRealTime',
                        'WriteCPUTime',
                    ]
                )
            writer.writerow(
                [
                    abs(float(load_rule_time.group(1))),
                    abs(float(cpu_load_rule_time.group(1))),
                    abs(float(load_facts_time.group(1))),
                    abs(float(cpu_load_facts_time.group(1))),
                    abs(float(query_time.group(1))),
                    abs(float(cpu_query_time.group(1))),
                    write_time,
                    cpu_write_time,
                ]
            )
        logging.info(f'(XSB) Experiment timing results saved to: {self.timing_path}')

        # Delete all .xwam files in the rule file directory
        for f in self.rule_path.parent.glob('*.xwam'):
            f.unlink()

        # Delete all .xwam files in the xsb_export directory
        for f in xsb_export_path.glob('*.xwam'):
            f.unlink()

        gc.collect()

    def solve_with_clingo(self) -> None:
        """
        Executes a query using Clingo and logs the time taken for loading and querying.

        This function creates a Clingo control object, loads the facts and rules, executes a query, and measures the time taken for loading and querying.
        It then writes these times to a CSV file.
        """
        try:
            ctl = clingo.Control()
            # Load facts and rules
            t_load_rule_begin = os.times()
            ctl.load(str(self.rule_path))
            t_load_rule_end = os.times()

            t_load_facts_begin = os.times()
            ctl.load(str(self.input_path))
            t_load_facts_end = os.times()

            t_ground_begin = os.times()
            ctl.ground([('base', [])])
            t_ground_end = os.times()

            t_query_begin = os.times()
            ctl.configuration.solve.models = '0'
            ctl.solve()
            t_query_end = os.times()

            # Measure time to solve and write the results
            output_file = self.output_folder / f'clingo_results.txt'

            t_query_w_begin = os.times()
            ctl.configuration.solve.models = '0'
            with ctl.solve(yield_=True) as handle:
                with open(output_file, 'w') as f:
                    for model in handle:
                        for atom in model.symbols(shown=True):
                            f.write(f'{atom}\n')
            t_query_w_end = os.times()

            logging.info(f'(Clingo) Results written to: {output_file}')

            load_rule_times = self.estimate_time_duration(
                t_load_rule_begin, t_load_rule_end
            )
            load_facts_times = self.estimate_time_duration(
                t_load_facts_begin, t_load_facts_end
            )
            ground_times = self.estimate_time_duration(t_ground_begin, t_ground_end)
            query_times = self.estimate_time_duration(t_query_begin, t_query_end)
            query_times_w = self.estimate_time_duration(t_query_w_begin, t_query_w_end)

            write_time = abs(float(query_times_w[0] - query_times[0]))
            write_cpu_time = abs(float(query_times_w[1] - query_times[1]))

            is_new_file = not self.timing_path.exists()
            with open(self.timing_path, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                if is_new_file:
                    writer.writerow(
                        [
                            'LoadRulesRealTime',
                            'LoadRulesCPUTime',
                            'LoadFactsRealTime',
                            'LoadFactsCPUTime',
                            'GroundRealTime',
                            'GroundCPUTime',
                            'QueryRealTime',
                            'QueryCPUTime',
                            'WriteRealTime',
                            'WriteCPUTime',
                        ]
                    )
                writer.writerow(
                    [
                        load_rule_times[0],
                        load_rule_times[1],
                        load_facts_times[0],
                        load_facts_times[1],
                        ground_times[0],
                        ground_times[1],
                        query_times[0],
                        query_times[1],
                        write_time,
                        write_cpu_time,
                    ]
                )
            logging.info(f'(Clingo) Experiment results saved to: {self.timing_path}')
        except Exception as e:
            logging.error(f'Error: {e}')
        finally:
            gc.collect()

    def solve_with_souffle(self) -> None:
        """
        Executes a Souffle program, compiles the generated C++ code, and runs the compiled program.

        This function takes in a Datalog program (self.input_path and self.rule_path), generates C++ code from it using Souffle, compiles the generated C++ code using g++, and runs the compiled program. It also logs the timing data for each of these steps and writes it to a CSV file.
        """
        souffle_export_path = Path('souffle_rules') / 'souffle_export'
        souffle_export_file = souffle_export_path / 'main'
        generated_cpp_filename = souffle_export_path / 'souffle_generated.cpp'

        # Run the command to generate C++ code from Datalog
        datalog_to_cpp_cmd = f'souffle {self.rule_path} -F {self.input_path} -w -g {generated_cpp_filename} -D {self.output_folder}'
        datalog_to_cpp_result = self.run_souffle_command(datalog_to_cpp_cmd)

        # Compile the generated C++ code with the main.cpp file using g++ with C++17 standard
        # and the include directory for Souffle C++ headers.
        # Preprocessor macro __EMBEDDED_SOUFFLE__ is defined to prevent main() from being generated.
        compile_cmd = f'g++ {souffle_export_file}.cpp {generated_cpp_filename} -std=c++17 -I {self.souffle_include_dir} -o {souffle_export_file} -D__EMBEDDED_SOUFFLE__'
        compile_result = self.run_souffle_command(compile_cmd)

        # Run the compiled program with the fact file
        run_cmd = f'./{souffle_export_file} {self.input_path}'
        run_result = self.run_souffle_command(run_cmd)
        logging.info(
            f'Results: DTC: {datalog_to_cpp_result}, CR: {compile_result}, RR: {run_result}'
        )

        # Write the timing data to the output CSV file
        is_new_file = not self.timing_path.exists()
        with open(self.timing_path, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            if is_new_file:
                writer.writerow(
                    [
                        'DatalogToCPPRealTime',
                        'DatalogToCPPCPUTime',
                        'CompileRealTime',
                        'CompileCPUTime',
                        'InstanceLoadingRealTime',
                        'InstanceLoadingCPUTime',
                        'LoadingFactRealTime',
                        'LoadingFactCPUTime',
                        'QueryRealTime',
                        'QueryCPUTime',
                        'WritingResultRealTime',
                        'WritingResultCPUTime',
                    ]
                )
            writer.writerow(
                [
                    datalog_to_cpp_result[0].split(',')[0],
                    datalog_to_cpp_result[0].split(',')[1],
                    compile_result[0].split(',')[0],
                    compile_result[0].split(',')[1],
                    float(run_result[1].get('Instance time', 0.0)),
                    float(run_result[1].get('InstanceCPU time', 0.0)),
                    float(run_result[1].get('LoadingFacts time', 0.0)),
                    float(run_result[1].get('LoadingFactsCPU time', 0.0)),
                    float(run_result[1].get('Query time', 0.0)),
                    float(run_result[1].get('QueryCPU time', 0.0)),
                    float(run_result[1].get('Writing time', 0.0)),
                    float(run_result[1].get('WritingCPU time', 0.0)),
                ]
            )

        logging.info(
            f'(Souffle) Experiment timing results saved to: {self.timing_path}'
        )

        # Delete executable file
        souffle_export_file.unlink()

        # Delete the generated C++ file
        generated_cpp_filename.unlink()

        gc.collect()


def main() -> None:
    """
    Parses command-line arguments and runs an experiment.

    This function parses command-line arguments for the size of the input graph, the mode of the rule file to use, the type of graph to analyze, and the logic
    programming environment to use. It then runs an experiment using these parameters and logs the time taken for various stages.
    """
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
        default='right_recursion',
        help='Mode of the rule file to use. Default is right_recursion.',
    )
    parser.add_argument(
        '--graph-type', type=str, required=True, help='Type of graph to analyze'
    )
    parser.add_argument(
        '--environment',
        required=True,
        help='Logic programming environment to use. Example is clingo or xsb.',
    )
    parser.add_argument(
        '--souffle-include-dir',
        type=str,
        help='Include directory for souffle C++ headers after installation',
    )
    args = parser.parse_args()

    logging.info(
        f'Running experiment for {args.environment} environment with mode: {args.mode} and size: {args.size}'
    )

    config = json.loads(args.config)

    analyze_logic_systems = AnalyzeLogicSystems(
        config, args.environment, args.souffle_include_dir
    )
    analyze_logic_systems.set_file_paths(args.mode, args.graph_type, args.size)
    analyze_logic_systems.set_output_folder()
    analyze_logic_systems.analyze()


if __name__ == '__main__':
    gc.disable()
    main()
