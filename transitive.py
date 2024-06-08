import argparse
import csv
import gc
import logging
import shutil
import subprocess
from pathlib import Path

# TODO: Add support for specifying the base file name

# Set up logging with a specific format and include the file name and line number
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s'
)


class Experiment:
    def __init__(
        self,
        graph_types: list[str],
        size_range: list[int],
        num_runs: int,
        modes: list[str],
        environments: list[str],
        souffle_include_dir: str,
    ):
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

        Step-by-step logic:
        1. Check if the timing directory exists.
        2. If the timing directory exists, iterate over all subdirectories of the timing directory.
        3. For each subdirectory:
            a. Check if the subdirectory contains any .csv files.
            b. If no .csv files are found, delete the subdirectory.
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

        Step-by-step logic:
        1. Create the input directory for the given environment if it doesn't exist.
        2. For each graph type:
            a. Check if a directory for that graph type exists in the input directory.
            b. If the directory doesn't exist, mark the graph type for data generation.
            c. If the directory exists, check if files for all sizes in the given size range exist in the directory.
            d. If any size file is missing, mark the graph type for data generation.
        3. If any graph type is marked for data generation, generate the necessary data using the 'generate_db.py' script.
        4. If no graph type is marked for data generation, log a message indicating that all data is up to date.
        """
        if env_name in ['clingo', 'xsb']:
            input_dir = Path('input') / 'clingo_xsb'
        elif env_name in ['souffle', 'postgres']:
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
            command = [
                'python',
                'generate_db.py',
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

        Step-by-step
        1. Create the output directory for the timing data if it doesn't exist.
        2. Create the output path for the timing data.
        3. Run the analysis program self.num_runs times.
        4. Calculate and append the average elapsed time and CPU time to the CSV file.
        """
        output_dir = self.timing_dir / env_name / graph_type
        output_dir.mkdir(parents=True, exist_ok=True)

        output_path = output_dir / f'timing_{mode}_graph_{size}.csv'

        if env_name == 'alda':
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
        elif env_name in ['souffle', 'xsb', 'clingo']:
            logging.info(f'Analyzing {env_name} among the logic systems')
            command = [
                'python',
                'analyze_logic_systems.py',
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
        elif env_name in ['postgres', 'mariadb', 'duckdb']:
            logging.info(f'Analyzing {env_name} among the DBs')
            command = [
                'python',
                'analyze_dbs.py',
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

        Step-by-step logic:
        1. Delete existing timing data if it exists and contains no .csv files.
        2. Generate the necessary input data for the environment, size range, and graph types.
        3. Run the experiment for each combination of size, mode, environment, and graph type.
        """
        self.__delete_existing_timing_data()

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
        subprocess.run(['python', 'generate_plot_table.py'])


def main() -> None:
    """
    The main function to run the transitive closure experiment.

    This function parses command-line arguments for the range of sizes, number of runs, modes, environments, and graph types for the experiment. It then generates the necessary input data, runs the experiment for each combination of size, mode, environment, and graph type, and finally generates a plot of the results.

    Step-by-step logic:
    1. Parse command-line arguments.
    2. Create an instance of the Experiment class with the parsed arguments.
    3. Run the experiment.
    """
    parser = argparse.ArgumentParser()
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

    modes = ['right_recursion', 'left_recursion', 'double_recursion']
    environments = [
        'xsb',
        'clingo',
        'souffle',
        'alda',
        'postgres',
    ]
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
        default=environments,
        choices=environments,
        help='The environments to run the experiment in. Default is clingo xsb souffle alda.',
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

    experiment = Experiment(
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
