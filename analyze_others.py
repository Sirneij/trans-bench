import argparse
import csv
import gc
import logging
import os
import re
import subprocess
from pathlib import Path

import clingo

# Set up logging with a specific format
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s'
)

# Define the file extensions for different logic programming environments
ENVIRONMENT_EXTENSIONS = {
    'clingo': '.lp',
    'xsb': '.P',
    'souffle': '.dl',
}


class AnalyzeOthers:
    def __init__(
        self,
        environment: str,
        rule_path: Path,
        input_path: Path,
        timing_path: Path,
        souffle_include_dir: str,
    ):
        self.environment = environment
        self.rule_path = rule_path
        self.input_path = input_path
        self.timing_path = timing_path
        self.souffle_include_dir = souffle_include_dir
        self.queries = (
            "[[query1, path(X, Y)]]"  # Each query is a list: [Identifier, Query]
        )

    def __estimate_time_duration(self, t1: tuple, t2: tuple) -> tuple[float, float]:
        """
        Estimates the time duration between two time points.

        This function calculates the elapsed time and the CPU time used between two time points. The time points are tuples containing user time, system time,
        child user time, child system time, and elapsed real time.

        Args:
            `t1 (tuple)`: The first time point. It is a tuple of the form (user time, system time, child user time, child system time, elapsed real time).
            `t2 (tuple)`: The second time point. It is a tuple of the form (user time, system time, child user time, child system time, elapsed real time).

        Step-by-step logic:
        1. Unpack the tuples t1 and t2 into their respective components.
        2. Calculate the difference in elapsed real time between t2 and t1.
        3. Calculate the difference in user time, system time, child user time, and child system time between t2 and t1, and sum these differences.

        Returns:
            tuple: A tuple containing the difference in elapsed real time and the sum of the differences in user time, system time, child user time, and child system time.
        """
        u1, s1, cu1, cs1, elapsed1 = t1
        u2, s2, cu2, cs2, elapsed2 = t2
        return elapsed2 - elapsed1, u2 - u1 + s2 - s1 + cu2 - cu1 + cs2 - cs1

    def analyze(self):
        """
        Executes a query using the specified environment and logs the time taken for various stages.

        This function checks the environment and calls the appropriate function to execute a query and log the time taken for various stages.
        The functions for Clingo, XSB, and Souffle are assumed to be defined elsewhere in the same module.

        Step-by-step logic:
        1. Check the environment.
        2. If the environment is 'clingo', call the function solve_with_clingo with the input file, rule file, and output file.
        3. If the environment is 'xsb', call the function solve_with_xsb with the input file, rule file, the query 'printPath(X, Y)', and the output file.
        4. If the environment is 'souffle', call the function solve_with_souffle with the input file, rule file, and output file.
        5. If the environment is not one of the above, log an error message.

        """
        pattern = r'^timing_(.*?)_graph_(\d+)\.csv$'
        match = re.match(pattern, self.timing_path.name)
        mode, size = '', ''
        if match:
            mode, size = match.groups()

        output_file_mode = self.timing_path.parent / f'{mode}'
        output_file_mode.parent.mkdir(parents=True, exist_ok=True)

        output_folder = output_file_mode / f'{size}'
        output_folder.mkdir(parents=True, exist_ok=True)
        logging.info(f'Output folder: {output_folder}')
        if self.environment == 'clingo':
            self.solve_with_clingo(
                self.input_path, self.rule_path, self.timing_path, output_folder
            )
        elif self.environment == 'xsb':
            self.solve_with_xsb(
                self.input_path,
                self.rule_path,
                self.queries,
                self.timing_path,
                output_folder,
            )
        elif self.environment == 'souffle':
            self.solve_with_souffle(
                self.input_path,
                self.rule_path,
                self.timing_path,
                output_folder,
                self.souffle_include_dir,
            )

    def solve_with_xsb(
        self,
        fact_file: Path,
        rule_file: Path,
        queries: str,
        timing_path: Path,
        output_folder: Path,
    ) -> None:
        """
        Executes a query using XSB Prolog and logs the time taken for loading and querying.

        This function prepares and runs a command to execute a query using XSB Prolog. It captures the output, extracts the loading and querying times,
        and writes these times to a CSV file.

        Args:
            `fact_file (Path)`: The path to the fact file.
            `rule_file (Path)`: The path to the rule file.
            `queries (str)`: The queries to run.
            `timing_path (Path)`: The path to the timing CSV file.
            `output_folder (Path)`: The path to the output folder.

        Step-by-step logic:
        1. Log the fact file, rule file, and query.
        2. Prepare the XSB command with the fact file, rule file, and query.
        3. Run the XSB command and capture the output.
        4. Extract the loading and querying times from the output.
        5. Write the loading and querying times to the output CSV file.
        6. Log the path to the output CSV file.
        7. Delete all .xwam files in the rule file directory to clean up.

        """
        logging.info(
            f"Running XSB with fact file {fact_file}, rule file {rule_file}, and queries {queries}"
        )

        xsb_export_path = rule_file.parent / 'xsb_export'
        results_path = output_folder / 'xsb_results.txt'

        # Prepare the XSB command
        xsb_query_1 = f"extfilequery:external_file_query_only('{rule_file}','{fact_file}',{queries},'{results_path}')."
        xsb_query_2 = f"extfilequery:external_file_query('{rule_file}','{fact_file}',{queries},'{results_path}')."
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
        cpu_write_time = float(query_cpu_write_time.group(1)) - float(cpu_query_time.group(1))

        is_new_file = not timing_path.exists()
        with open(timing_path, 'a', newline='') as csvfile:
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
        logging.info(f'(XSB) Experiment timing results saved to: {timing_path}')

        # Delete all .xwam files in the rule file directory
        for f in rule_file.parent.glob('*.xwam'):
            f.unlink()

        # Delete all .xwam files in the xsb_export directory
        for f in xsb_export_path.glob('*.xwam'):
            f.unlink()

        gc.collect()

    def solve_with_clingo(
        self, fact_file: Path, rule_file: Path, timing_path: Path, output_folder: Path
    ) -> None:
        """
        Executes a query using Clingo and logs the time taken for loading and querying.

        This function creates a Clingo control object, loads the facts and rules, executes a query, and measures the time taken for loading and querying.
        It then writes these times to a CSV file.

        Args:
            `fact_file (Path)`: The path to the fact file.
            `rule_file (Path)`: The path to the rule file.
            `timing_path (Path)`: The path to the timing CSV file.
            `output_folder (Path)`: The path to the output folder.

        Step-by-step logic:
        1. Create a Clingo control object.
        2. Measure the time before loading the facts and rules.
        3. Load the facts and rules.
        4. Measure the time after loading the facts and rules.
        5. Measure the time before executing the query.
        6. Execute the query.
        7. Measure the time after executing the query.
        8. Calculate the loading and querying times.
        9. Write the loading and querying times to the output CSV file.
        10. Log the path to the output CSV file.

        """
        try:
            ctl = clingo.Control()
            # Load facts and rules
            t_load_rule_begin = os.times()
            ctl.load(str(rule_file))
            t_load_rule_end = os.times()

            t_load_facts_begin = os.times()
            ctl.load(str(fact_file))
            t_load_facts_end = os.times()

            t_ground_begin = os.times()
            ctl.ground([('base', [])])
            t_ground_end = os.times()

            t_query_begin = os.times()
            ctl.configuration.solve.models = '0'
            ctl.solve()
            t_query_end = os.times()

            # Measure time to solve and write the results
            output_file = output_folder / f'clingo_results.txt'

            t_query_w_begin = os.times()
            ctl.configuration.solve.models = '0'
            with ctl.solve(yield_=True) as handle:
                with open(output_file, 'w') as f:
                    for model in handle:
                        for atom in model.symbols(shown=True):
                            f.write(f'{atom}\n')
            t_query_w_end = os.times()

            logging.info(f'(Clingo) Results written to: {output_file}')

            load_rule_times = self.__estimate_time_duration(
                t_load_rule_begin, t_load_rule_end
            )
            load_facts_times = self.__estimate_time_duration(
                t_load_facts_begin, t_load_facts_end
            )
            ground_times = self.__estimate_time_duration(t_ground_begin, t_ground_end)
            query_times = self.__estimate_time_duration(t_query_begin, t_query_end)
            query_times_w = self.__estimate_time_duration(
                t_query_w_begin, t_query_w_end
            )

            write_time = abs(float(query_times_w[0] - query_times[0]))
            write_cpu_time = abs(float(query_times_w[1] - query_times[1]))

            is_new_file = not timing_path.exists()
            with open(timing_path, 'a', newline='') as csvfile:
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
            logging.info(f'(Clingo) Experiment results saved to: {timing_path}')
        except Exception as e:
            logging.error(f'Error: {e}')
        finally:
            gc.collect()

    def solve_with_souffle(
        self,
        fact_file: Path,
        rule_file: Path,
        timing_path: Path,
        output_folder: Path,
        include_dir: str,
    ) -> None:
        """
        Executes a Souffle program, compiles the generated C++ code, and runs the compiled program.

        This function takes in a Datalog program (fact_file and rule_file), generates C++ code from it using Souffle, compiles the generated C++ code using g++, and runs the compiled program. It also logs the timing data for each of these steps and writes it to a CSV file.

        Args:
            `fact_file (str)`: The path to the fact file for the Datalog program.
            `rule_file (str)`: The path to the rule file for the Datalog program.
            `timing_path (str)`: The path to the CSV file where the timing data will be written.
            `output_folder (str)`: The path to the folder where the output of the Datalog program will be written.
            `include_dir (str)`: The path to the directory containing the Souffle C++ headers.

        Step-by-step logic:
        1. Generate C++ code from the Datalog program using Souffle.
        2. Compile the generated C++ code using g++.
        3. Run the compiled program.
        4. Log the timing data for each of these steps.
        5. Write the timing data to a CSV file.
        6. Delete the executable file and the generated C++ file to clean up.
        """
        souffle_export_path = Path('souffle_rules') / 'souffle_export'
        souffle_export_file = souffle_export_path / 'main'
        generated_cpp_filename = souffle_export_path / 'souffle_generated.cpp'

        # Run the command to generate C++ code from Datalog
        datalog_to_cpp_cmd = f'souffle {rule_file} -F {fact_file} -w -g {generated_cpp_filename} -D {output_folder}'
        datalog_to_cpp_result = self.__run_souffle_command(datalog_to_cpp_cmd)

        # Compile the generated C++ code with the main.cpp file using g++ with C++17 standard
        # and the include directory for Souffle C++ headers.
        # Preprocessor macro __EMBEDDED_SOUFFLE__ is defined to prevent main() from being generated.
        compile_cmd = f'g++ {souffle_export_file}.cpp {generated_cpp_filename} -std=c++17 -I {include_dir} -o {souffle_export_file} -D__EMBEDDED_SOUFFLE__'
        compile_result = self.__run_souffle_command(compile_cmd)

        # Run the compiled program with the fact file
        run_cmd = f'./{souffle_export_file} {fact_file}'
        run_result = self.__run_souffle_command(run_cmd)
        logging.info(
            f'Results: DTC: {datalog_to_cpp_result}, CR: {compile_result}, RR: {run_result}'
        )

        # Write the timing data to the output CSV file
        is_new_file = not timing_path.exists()
        with open(timing_path, 'a', newline='') as csvfile:
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

        logging.info(f'(Souffle) Experiment timing results saved to: {timing_path}')

        # Delete executable file
        souffle_export_file.unlink()

        # Delete the generated C++ file
        generated_cpp_filename.unlink()

        gc.collect()

    def __run_souffle_command(self, command: str) -> tuple[str, dict[str, float]]:
        """
        Executes a given Souffle command and logs the output, error, and timing data.

        This function takes in a Souffle command, executes it using the subprocess module, and logs the output, error, and timing data. If the command fails, it logs the error and returns a failure message. If the command succeeds, it calculates the real time and CPU time used and returns these values along with the parsed timing data.

        Args:
            `command (str)`: The Souffle command to be executed.

        Step-by-step logic:
        1. Log the command to be executed.
        2. Execute the command using the subprocess module.
        3. If the command fails, log the error and return a failure message.
        4. If the command succeeds, log the output, error, and parsed timing data.
        5. Calculate the real time and CPU time used.
        6. Return the real time, CPU time, and parsed timing data.

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
            real_time, cpu_time = self.__estimate_time_duration(time_begin, time_end)
            return f"{real_time},{cpu_time}", timing_data
        else:
            return '0,0', {}

    def __parse_souffle_timing_data(self, output: str) -> dict[str, float]:
        """
        Parses the timing data from the output of a Souffle command.

        This function takes in the output of a Souffle command, extracts the timing data using a regular expression, and returns this data as a dictionary.

        Args:
            `output (str)`: The output of a Souffle command.

        Step-by-step logic:
        1. Define a regular expression pattern that matches the timing data in the output.
        2. Use the re.findall function to find all matches of the pattern in the output.
        3. Convert the list of matches into a dictionary and return it.

        Returns:
            `dict`: A dictionary where the keys are the names of the timing metrics (e.g., 'Loading time', 'Query time') and the values are the corresponding times in seconds.
        """
        pattern = r'(\w+ time): (\d+\.\d+) seconds'
        timings = re.findall(pattern, output)
        return dict(timings)


def discover_rules(rules_dir: Path, extension: str) -> dict[str, Path]:
    """
    Discovers rule files in a directory and maps rule names to file paths.

    This function searches for files with a given extension in a directory. It then maps the rule name from each file name (the part after the first underscore)
    to the file path. The function returns this mapping as a dictionary.

    Args:
        `rules_dir (Path)`: The directory to search for rule files.
        `extension (str)`: The extension of the rule files.

    Step-by-step logic:
    1. Use a dictionary comprehension to create a dictionary.
    2. For each file in the directory that matches the given extension:
        a. Extract the rule name from the file name by splitting the name at the first underscore and taking the second part.
        b. Map the rule name to the file path.

    Returns:
        dict[str, Path]: A dictionary mapping rule names to file paths.
    """
    return {
        rule_file.stem.split('_', 1)[-1]: rule_file
        for rule_file in rules_dir.glob(f'*{extension}')
    }


def main() -> None:
    """
    Parses command-line arguments and runs an experiment.

    This function parses command-line arguments for the size of the input graph, the mode of the rule file to use, the type of graph to analyze, and the logic
    programming environment to use. It then runs an experiment using these parameters and logs the time taken for various stages.

    Step-by-step logic:
    1. Parse command-line arguments.
    2. Log the experiment parameters.
    3. Determine the paths to the rule file and the input file.
    4. Log the paths to the rule file and the input file.
    5. Determine the path to the output CSV file.
    6. Run the experiment using the specified parameters.

    """
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
        choices=['clingo', 'xsb', 'souffle'],
        required=True,
        help='Logic programming environment to use. Choose from clingo, xsb, or souffle.',
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

    project_root = Path(__file__).parent
    rules_dir = project_root / f'{args.environment}_rules'
    RULES_FILES = discover_rules(rules_dir, ENVIRONMENT_EXTENSIONS[args.environment])

    if args.mode not in RULES_FILES:
        logging.error(f'Rule file not found for mode: {args.mode}')
        return

    input_dir = Path('input')
    if args.environment == 'souffle':
        input_path = input_dir / 'souffle' / args.graph_type / str(args.size)
    else:
        inputfile = f'graph_{args.size}.lp'
        input_path = input_dir / 'clingo_xsb' / args.graph_type / inputfile
    rule_path = RULES_FILES[args.mode]

    logging.info(f'Using rule file: {rule_path} and input file: {input_path}')

    timing_dir = Path('timing') / args.environment / args.graph_type
    timing_dir.mkdir(parents=True, exist_ok=True)
    if args.environment == 'souffle':
        output_file_name = f'timing_{args.mode}_graph_{args.size}.csv'
    else:
        output_file_name = f'timing_{args.mode}_{input_path.stem}.csv'
    timing_path = timing_dir / output_file_name

    analyze_others = AnalyzeOthers(
        args.environment, rule_path, input_path, timing_path, args.souffle_include_dir
    )
    analyze_others.analyze()


if __name__ == '__main__':
    gc.disable()
    main()
