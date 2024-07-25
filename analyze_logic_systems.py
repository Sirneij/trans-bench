import argparse
import csv
import gc
import json
import logging
import os
import re
import subprocess
from pathlib import Path
from typing import Any, List, Optional

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
        queries: str,
    ):
        super().__init__(config, environment)
        self.souffle_include_dir = souffle_include_dir
        self.queries = queries  # Each query is a list: [Identifier, Query]

    def analyze(self) -> None:
        """Executes the appropriate solve method based on the environment."""
        solve_method = getattr(self, f'solve_with_{self.environment}', None)
        if solve_method is None:
            logging.error(
                f"'{self.environment}' is not supported or method is missing."
            )
            return
        solve_method()

    def run_subprocess(self, command: List[str]) -> subprocess.CompletedProcess:
        """Run a subprocess command and return the completed process."""
        logging.info(f'Executing command: {" ".join(command)}')
        return subprocess.run(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

    def extract_timing(self, pattern: str, text: str) -> Optional[float]:
        """Extract timing information from text using a regex pattern."""
        match = re.search(pattern, text)
        return float(match.group(1)) if match else None

    def write_to_csv(
        self, headers: List[str], data: List[Any], file_path: Path
    ) -> None:
        """Write data to a CSV file."""
        is_new_file = not file_path.exists()
        with open(file_path, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            if is_new_file:
                writer.writerow(headers)
            writer.writerow(data)

    def solve_with_xsb(self) -> None:
        """Executes a query using XSB Prolog and logs the time taken for various stages."""
        logging.info(
            f"Running XSB with fact file {self.input_path}, rule file {self.rule_path}, and queries {self.queries}"
        )

        xsb_export_path = self.rule_path.parent / 'xsb_export'
        results_path = self.output_folder / 'xsb_results.txt'

        xsb_commands = [
            [
                'xsb',
                '--nobanner',
                '--quietload',
                '--noprompt',
                '-e',
                f"add_lib_dir('{xsb_export_path}').",
                '-e',
                f"extfilequery:external_file_query_only('{self.rule_path}','{self.input_path}',{self.queries},'{results_path}').",
            ],
            [
                'xsb',
                '--nobanner',
                '--quietload',
                '--noprompt',
                '-e',
                f"add_lib_dir('{xsb_export_path}').",
                '-e',
                f"extfilequery:external_file_query('{self.rule_path}','{self.input_path}',{self.queries},'{results_path}').",
            ],
        ]

        outputs = [self.run_subprocess(cmd) for cmd in xsb_commands]

        logging.info(f'(XSB) Command output: {outputs[0].stdout}')
        logging.info(f'(XSB) Error output: {outputs[0].stderr}')
        logging.info(f'(XSB) Command output 2: {outputs[1].stdout}')
        logging.info(f'(XSB) Error output 2: {outputs[1].stderr}')

        timings = [
            ('LoadRuleTime', outputs[0].stdout),
            ('CPULoadRuleTime', outputs[0].stdout),
            ('LoadFactsTime', outputs[0].stdout),
            ('CPULoadFactsTime', outputs[0].stdout),
            ('QueryOnlyTime', outputs[0].stdout),
            ('CPUQueryOnlyTime', outputs[0].stdout),
            ('QueryAndWriteTime', outputs[1].stdout),
            ('CPUTimeQueryAndWriteTime', outputs[1].stdout),
        ]

        timing_results = [
            self.extract_timing(rf'{name}:\s+(-?\d+\.\d+(?:e[+-]?\d+)?)', text)
            for name, text in timings
        ]
        write_time = float(timing_results[6]) - float(timing_results[4])
        cpu_write_time = float(timing_results[7]) - float(timing_results[5])

        self.write_to_csv(
            [
                'LoadRulesRealTime',
                'LoadRulesCPUTime',
                'LoadFactsRealTime',
                'LoadFactsCPUTime',
                'QueryRealTime',
                'QueryCPUTime',
                'WriteRealTime',
                'WriteCPUTime',
            ],
            [
                float(timing_results[0]),
                float(timing_results[1]),
                float(timing_results[2]),
                float(timing_results[3]),
                float(timing_results[4]),
                float(timing_results[5]),
                write_time,
                cpu_write_time,
            ],
            self.timing_path,
        )

        logging.info(f'(XSB) Experiment timing results saved to: {self.timing_path}')

        # Clean up .xwam files
        for f in self.rule_path.parent.glob('*.xwam'):
            f.unlink()
        for f in xsb_export_path.glob('*.xwam'):
            f.unlink()

        gc.collect()

    def solve_with_clingo(self) -> None:
        """Executes a query using Clingo and logs the time taken for loading and querying."""
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
            with ctl.solve(yield_=True) as handle:
                # Collect results first
                results = [model.symbols(shown=True) for model in handle]
            t_query_end = os.times()

            # Now write the results to a file
            output_file = self.output_folder / 'clingo_results.txt'
            t_query_w_begin = os.times()
            with open(output_file, 'w') as f:
                f.writelines([f'{atom}\n' for result in results for atom in result])
            t_query_w_end = os.times()

            logging.info(f'(Clingo) Results written to: {output_file}')

            timings = {
                'LoadRules': self.estimate_time_duration(
                    t_load_rule_begin, t_load_rule_end
                ),
                'LoadFacts': self.estimate_time_duration(
                    t_load_facts_begin, t_load_facts_end
                ),
                'Ground': self.estimate_time_duration(t_ground_begin, t_ground_end),
                'Query': self.estimate_time_duration(t_query_begin, t_query_end),
                'QueryWrite': self.estimate_time_duration(
                    t_query_w_begin, t_query_w_end
                ),
            }

            write_time = float(timings['QueryWrite'][0] - timings['Query'][0])
            write_cpu_time = float(timings['QueryWrite'][1] - timings['Query'][1])

            self.write_to_csv(
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
                ],
                [
                    timings['LoadRules'][0],
                    timings['LoadRules'][1],
                    timings['LoadFacts'][0],
                    timings['LoadFacts'][1],
                    timings['Ground'][0],
                    timings['Ground'][1],
                    timings['Query'][0],
                    timings['Query'][1],
                    write_time,
                    write_cpu_time,
                ],
                self.timing_path,
            )

            logging.info(f'(Clingo) Experiment results saved to: {self.timing_path}')
        except Exception as e:
            logging.error(f'Error: {e}')
        finally:
            gc.collect()

    def solve_with_souffle(self) -> None:
        """Executes a Souffle program, compiles the generated C++ code, and runs the compiled program."""
        souffle_export_path = Path('souffle_rules') / 'souffle_export'
        souffle_export_file = souffle_export_path / 'main'
        generated_cpp_filename = souffle_export_path / 'souffle_generated.cpp'

        # Generate C++ code from Datalog
        datalog_to_cpp_cmd = f'souffle {self.rule_path} -F {self.input_path} -w -g {generated_cpp_filename} -D {self.output_folder}'
        datalog_to_cpp_result = self.run_souffle_command(datalog_to_cpp_cmd)

        # Compile the generated C++ code
        compile_cmd = f'g++ {souffle_export_file}.cpp {generated_cpp_filename} -std=c++17 -I {self.souffle_include_dir} -o {souffle_export_file} -D__EMBEDDED_SOUFFLE__'
        compile_result = self.run_souffle_command(compile_cmd)

        # Run the compiled program
        run_cmd = f'./{souffle_export_file} {self.input_path}'
        run_result = self.run_souffle_command(run_cmd)
        logging.info(
            f'Results: DTC: {datalog_to_cpp_result}, CR: {compile_result}, RR: {run_result}'
        )

        # Write the timing data to the output CSV file
        self.write_to_csv(
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
            ],
            [
                *datalog_to_cpp_result[0].split(','),
                *compile_result[0].split(','),
                float(run_result[1].get('Instance time', 0.0)),
                float(run_result[1].get('InstanceCPU time', 0.0)),
                float(run_result[1].get('LoadingFacts time', 0.0)),
                float(run_result[1].get('LoadingFactsCPU time', 0.0)),
                float(run_result[1].get('Query time', 0.0)),
                float(run_result[1].get('QueryCPU time', 0.0)),
                float(run_result[1].get('Writing time', 0.0)),
                float(run_result[1].get('WritingCPU time', 0.0)),
            ],
            self.timing_path,
        )

        logging.info(
            f'(Souffle) Experiment timing results saved to: {self.timing_path}'
        )

        # Clean up generated files
        souffle_export_file.unlink(missing_ok=True)
        generated_cpp_filename.unlink(missing_ok=True)
        gc.collect()


def main() -> None:
    """Parses command-line arguments and runs an experiment."""
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
        config, args.environment, args.souffle_include_dir, config.get('queries', '[]')
    )
    analyze_logic_systems.set_file_paths(args.mode, args.graph_type, args.size)
    analyze_logic_systems.set_output_folder()
    analyze_logic_systems.analyze()


if __name__ == '__main__':
    gc.disable()
    main()
