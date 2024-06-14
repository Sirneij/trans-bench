import argparse
import csv
import gc
import json
import logging
import shutil
import subprocess
from pathlib import Path
from typing import Any

from common import Base

# TODO: Add support for specifying the base file name

# Set up logging with a specific format and include the file name and line number
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s'
)


class Experiment(Base):
    def __init__(
        self,
        config: dict[str, Any],
        graph_types: list[str],
        size_range: list[int],
        num_runs: int,
        modes: list[str],
        environments: list[str],
        souffle_include_dir: str,
    ):
        super().__init__(config)
        self.graph_types = graph_types
        self.size_range = size_range
        self.num_runs = num_runs
        self.modes = modes
        self.environments = environments
        self.souffle_include_dir = souffle_include_dir
        self.timing_dir = Path('timing')

    def __str__(self):
        return f'Experiment(graph_types={self.graph_types}, size_range={self.size_range}, num_runs={self.num_runs}, modes={self.modes}, environments={self.environments}, souffle_include_dir={self.souffle_include_dir})'

    def __delete_existing_timing_data(self):
        """
        Deletes existing timing data if it exists and contains no .csv files.

        This function checks if the timing directory exists and contains no .csv files. If the directory exists and contains no .csv files, it is deleted.
        """
        if self.timing_dir.exists():
            # Iterate over all subdirectories of the timing directory
            for subdir in self.timing_dir.iterdir():
                if subdir.is_dir():
                    # Check if the subdirectory contains any .csv files
                    csv_files = list(subdir.glob('**/*.csv'))
                    if not csv_files:
                        # If no .csv files are found, delete the subdirectory
                        logging.info(f'Removing existing directory: {subdir}...')
                        shutil.rmtree(subdir)

    def __generate_input_data(
        self, env_name: str, size_range: list[int], graph_types: list[str]
    ) -> None:
        """
        Generates input data for a given environment, size range, and set of graph types.

        This function checks if the necessary data already exists in the input directory for the given environment.
        If any data is missing, it generates the necessary data.

        Args:
            `env_name (str)`: The name of the environment for which to generate data.
            `size_range (list[int])`: A list of three integers representing the start, stop, and step of the range of sizes for which to generate data.
            `graph_types (list[str])`: A list of the types of graphs for which to generate data.
        """
        if env_name in ['clingo', 'xsb']:
            input_dir = Path('input') / 'clingo_xsb'
        elif env_name in self.config['defaults']['systems']['dbSystems'] + ['souffle']:
            input_dir = Path('input') / 'souffle'
        else:
            input_dir = Path('input') / env_name
        input_dir.mkdir(parents=True, exist_ok=True)

        # List to hold graph types that need data generation
        missing_graph_types = []

        # Check each graph type directory for missing sizes
        for graph_type in graph_types:
            graph_type_dir = input_dir / graph_type
            if not graph_type_dir.exists():
                # If the graph type directory doesn't exist, mark it for data generation
                missing_graph_types.append(graph_type)
            else:
                # Check for missing size files in the existing graph type directory
                existing_sizes = {
                    int(f.stem.split('_')[-1]) for f in graph_type_dir.glob('*.*')
                }
                required_sizes = set(range(size_range[0], size_range[1], size_range[2]))
                missing_sizes = required_sizes - existing_sizes
                if missing_sizes:
                    missing_graph_types.append(graph_type)

        # Generate data only if there are missing graph types or sizes
        if missing_graph_types:
            logging.info(
                f'Missing data for {missing_graph_types}. Generating data with size range: {size_range}...'
            )
            config_str = json.dumps(self.config)
            command = [
                'python',
                'generate_db.py',
                '--config',
                config_str,
                '--sizes',
                str(size_range[0]),
                str(size_range[1]),
                str(size_range[2]),
                '--graph-types',
                *missing_graph_types,
            ]
            subprocess.run(command)
        else:
            logging.info(
                f'All data up to date for {env_name}. Skipping data generation...'
            )

    def __start_analysis(
        self,
        graph_type: str,
        size: int,
        step: int,
        mode: str,
        env_name: str,
        souffle_include_dir: str,
    ) -> None:
        """
        Starts the analysis for the given environment, size, mode, and graph type.

        This function runs the analysis for the given environment, size, mode, and graph type using the appropriate command.

        Args:
            `graph_type (str)`: The type of graph to analyze.
            `size (int)`: The size of the graph.
            `step (int)`: The step size for the graph.
            `mode (str)`: The mode in which to run the analysis.
            `env_name (str)`: The name of the environment in which to run the analysis.
            `souffle_include_dir (str)`: The include directory for souffle.
        """
        output_dir = self.timing_dir / env_name / graph_type
        output_dir.mkdir(parents=True, exist_ok=True)

        output_path = output_dir / f'timing_{mode}_graph_{size}.csv'

        config_str = json.dumps(self.config)

        if env_name in self.config['defaults']['systems']['alda']:
            command = [
                'python',
                '-m',
                'da',
                '-r',
                f'--message-buffer-size={size//step}409600',
                '--rules',
                f'analyze_alda.da',
                '--size',
                str(size),
                '--mode',
                mode,
                '--graph-type',
                graph_type,
            ]
        elif env_name in self.config['defaults']['systems']['otherLogicSystems']:
            logging.info(f'Analyzing {env_name} among the logic systems')
            command = [
                'python',
                'analyze_logic_systems.py',
                '--config',
                config_str,
                '--environment',
                env_name,
                '--size',
                str(size),
                '--mode',
                mode,
                '--graph-type',
                graph_type,
                '--souffle-include-dir',
                souffle_include_dir,
            ]
        elif env_name in self.config['defaults']['systems']['dbSystems']:
            logging.info(f'Analyzing {env_name} among the DBs')
            command = [
                'python',
                'analyze_dbs.py',
                '--config',
                config_str,
                '--environment',
                env_name,
                '--size',
                str(size),
                '--mode',
                mode,
                '--graph-type',
                graph_type,
            ]

        # Run the program 10 times
        for _ in range(self.num_runs):
            subprocess.run(command)

        # Calculate and append the average to the CSV file
        self.__calculate_and_append_average(output_path)

    def __calculate_and_append_average(self, output_path: Path) -> None:
        """
        Calculates and appends the averages of all columns to a CSV file.

        This function reads a CSV file, calculates the average for each column from the data in the file, and appends these averages to the end of the file. It can handle any number of columns dynamically.

        Args:
            output_path (Path): The path to the CSV file.
        """
        with open(output_path, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader, None)  # Skip header and get header names
            if not headers:
                logging.info(f'No headers found in {output_path}')
                return

            # Initialize sum and count for each column
            sums = [0.0] * len(headers)
            counts = [0] * len(headers)

            for row in reader:
                if row:  # Ensure row is not empty
                    for i, value in enumerate(row):
                        if i < len(headers):  # Ensure index is within range
                            try:
                                sums[i] += float(value)
                                counts[i] += 1
                            except ValueError:
                                logging.warning(
                                    f"Non-numeric data '{value}' in column {i} skipped"
                                )
                        else:
                            logging.warning(f"Row has more columns than headers: {row}")

        # Calculate averages and prepare the row to append
        averages = []
        for sum_value, count_value in zip(sums, counts):
            average = sum_value / count_value if count_value > 0 else 0
            averages.append(average)

        # Append the averages to the CSV file
        if any(counts):  # Check if there is any data
            with open(output_path, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['Average'] + averages)
        else:
            logging.info(f'No data to calculate averages for in {output_path}')

    def run(self):
        """
        Runs the experiment for each combination of size, mode, environment, and graph type.

        This function generates the necessary input data, runs the experiment for each combination of size, mode, environment, and graph type, and calculates and appends the average elapsed time and CPU time to a CSV file.
        """
        self.__delete_existing_timing_data()

        config_str = json.dumps(self.config)

        for env in self.environments:
            self.__generate_input_data(env, self.size_range, self.graph_types)
            for graph_type in self.graph_types:
                for size in range(*self.size_range):
                    # Check if experiment data already exists for this size and skip if it does
                    skip_experiment = False
                    for mode in self.modes:
                        csv_path = (
                            self.timing_dir
                            / env
                            / graph_type
                            / f'timing_{mode}_graph_{size}.csv'
                        )
                        if csv_path.exists():
                            logging.info(f'Skipping existing experiment: {csv_path}')
                            skip_experiment = True
                            break
                    if skip_experiment:
                        continue

                    # If no existing data, run the experiment
                    for mode in self.modes:
                        self.__start_analysis(
                            graph_type,
                            size,
                            self.size_range[-1],
                            mode,
                            env,
                            self.souffle_include_dir,
                        )

        # Generate latex plots and tables of the
        subprocess.run(['python', 'generate_plot_table.py', '--config', config_str])


def load_config(file_path: str) -> dict[str, Any]:
    with open(file_path, 'r') as file:
        return json.load(file)


def main() -> None:
    """
    The main function to run the transitive closure experiment.

    This function parses command-line arguments for the range of sizes, number of runs, modes, environments, and graph types for the experiment. It then generates the necessary input data, runs the experiment for each combination of size, mode, environment, and graph type, and finally generates a plot of the results.
    """
    modes = ['right_recursion', 'left_recursion', 'double_recursion']
    graph_types = [
        'complete',
        'cycle',
        'cycle_with_shortcuts',
        'star',
        'max_acyclic',
        'path',
        'multi_path',
        'binary_tree',
        'grid',
        'reverse_binary_tree',
        'w',
        'y',
        'x',
    ]

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--config-file', type=str, default='config.json', help='Path to the config file'
    )
    parser.add_argument(
        '--size-range',
        type=int,
        nargs=3,
        metavar=('START', 'STOP', 'STEP'),
        default=[10, 101, 10],
        help='The range of sizes as a start stop and step. Default is 10 101 10.',
    )

    parser.add_argument(
        '--num-runs',
        type=int,
        default=10,
        help='Number of times to run each experiment for each size. Default is 10.',
    )

    parser.add_argument(
        '--modes',
        nargs='+',
        default=modes,
        choices=modes,
        help='The modes to run the experiment in. Default is right_recursion left_recursion double_recursion.',
    )

    parser.add_argument(
        '--environments',
        nargs='+',
        help='The environments to run the experiment in. Default is all the systems.',
    )

    parser.add_argument(
        '--graph-types',
        nargs='+',
        default=graph_types,
        choices=graph_types,
        help='The types of graphs to run the experiment on. Default is complete cycle cycle_with_shortcuts star max_acyclic path multi_path binary_tree reverse_binary_tree w y x.',
    )
    parser.add_argument(
        '--souffle-include-dir',
        type=str,
        default='/opt/homebrew/Cellar/souffle/HEAD-c7ce229/include',
        help='The include directory for souffle. Default is /opt/homebrew/Cellar/souffle/HEAD-c7ce229/include.',
    )
    args = parser.parse_args()

    config = load_config(args.config_file)

    environments = set(args.environments)
    defaults = config['defaults']['systems']
    all_environments = set(
        defaults['alda'] + defaults['otherLogicSystems'] + defaults['dbSystems']
    )

    invalid_environments = environments - all_environments

    if invalid_environments:
        parser.error(
            f"The following environments are not valid: {', '.join(invalid_environments)}. "
            f"Valid environments are: {', '.join(all_environments)}."
        )

    experiment = Experiment(
        config,
        args.graph_types,
        args.size_range,
        args.num_runs,
        args.modes,
        args.environments,
        args.souffle_include_dir,
    )

    logging.info(f'Starting experiment: {experiment}')

    experiment.run()


if __name__ == '__main__':
    gc.disable()
    main()
