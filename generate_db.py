import argparse
import gc
import logging
import math
import pickle
from pathlib import Path
from typing import Generator

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s'
)


class DataGenerator:
    """
    The implementations here follow what wass described in the paper: Performance Analysis and Comparison of Deductive Systems and SQL Databases (https://ceur-ws.org/Vol-2368/paper3.pdf) with some modifications and additional graph types.
    """

    def __init__(self):
        self.k = 10

    def generate_complete_graph(self, n) -> Generator[tuple[int, int], None, None]:
        # self.E = {(i, j) for i in range(1, n + 1) for j in range(1, n + 1)}
        logging.info(f'Generating complete graph for n={n}')
        for i in range(1, n + 1):
            for j in range(1, n + 1):
                yield (i, j)

    def generate_max_acyclic_graph(self, n) -> Generator[tuple[int, int], None, None]:
        # self.E = {(a, b) for a in range(1, n + 1) for b in range(1, a) if a > b}
        logging.info(f'Generating max acyclic graph for n={n}')
        for a in range(1, n + 1):
            for b in range(1, a):
                if a > b:
                    yield (a, b)

    def generate_cycle_graph(self, n) -> Generator[tuple[int, int], None, None]:
        # E = {(i, i + 1) for i in range(1, n)} | {(n, 1)}
        logging.info(f'Generating cycle graph for n={n}')
        for i in range(1, n):
            yield (i, i + 1)
        yield (n, 1)

    def generate_cycle_with_shortcuts_graph(
        self, n
    ) -> Generator[tuple[int, int], None, None]:
        logging.info(f'Generating cycle with shortcuts graph for n={n}')

        skip = n // (self.k + 1)  # Number of vertices to skip for shortcuts
        for i in range(1, n):
            yield (i, i + 1)
        yield (n, 1)
        for i in range(1, n + 1):
            for t in range(1, self.k + 1):
                yield (i, 1 + (i - 1 + skip * t) % n)

    def generate_path_graph(self, n) -> Generator[tuple[int, int], None, None]:
        logging.info(f'Generating path graph for n={n}')
        for i in range(1, n):
            yield (i, i + 1)

    def generate_multi_path_graph(self, n) -> Generator[tuple[int, int], None, None]:
        logging.info(f'Generating multi path graph for n={n}')
        for i in range(1, (n - 1) * self.k + 1):
            yield (i, i + self.k)

    def generate_binary_tree_graph(self, n) -> Generator[tuple[int, int], None, None]:
        h = math.floor(math.log2(n))
        logging.info(f'Generating binary tree graph for n={n} and h={h}')
        parent_count = 2 ** (h - 1) - 1
        for i in range(1, parent_count + 1):
            yield (i, 2 * i)
            yield (i, 2 * i + 1)

    def generate_reverse_binary_tree_graph(
        self, n
    ) -> Generator[tuple[int, int], None, None]:
        h = math.floor(math.log2(n))
        logging.info(f'Generating reverse binary tree graph for n={n} and h={h}')
        parent_count = 2 ** (h - 1) - 1
        for i in range(1, parent_count + 1):
            yield (2 * i, i)
            yield (2 * i + 1, i)

    def generate_y_graph(self, n) -> Generator[tuple[int, int], None, None]:
        logging.info(f'Generating Y graph for n={n}')
        for i in range(1, n + 1):
            yield (i, n + 1)
        for i in range(n + 2, n + self.k + 1):
            yield (i - 1, i)

    def generate_w_graph(self, n) -> Generator[tuple[int, int], None, None]:
        logging.info(f'Generating W graph for n={n}')
        for i in range(1, n + 1):
            for j in range(1, self.k + 1):
                yield (i, n + 1 + (i + j - 1) % n)

    def generate_x_graph(self, n) -> Generator[tuple[int, int], None, None]:
        logging.info(f'Generating X graph for n={n}')
        for i in range(1, n + 1):
            yield (i, n + 1)
        for j in range(1, self.k + 1):
            yield (n + 1, n + 1 + j)

    def generate_star_graph(self, n) -> Generator[tuple[int, int], None, None]:
        logging.info(f'Generating star graph for n={n}')
        for i in range(2, n + 1):
            yield (i, 1)

    def generate_grid_graph(self, n) -> Generator[tuple[int, int], None, None]:
        logging.info(f'Generating grid graph for n={n}')

        n = int(math.sqrt(n))

        # Generate edges (j, j+1) for rows
        for i in range(1, n + 1):
            for j in range((i - 1) * n + 1, (i - 1) * n + n):
                yield (j, j + 1)

        # Generate edges (j, j+n) for columns
        for i in range(1, n):
            for j in range((i - 1) * n + 1, (i - 1) * n + n + 1):
                yield (j, j + n)


class GraphGenerator:
    """
    This class is responsible for generating and saving graphs in different formats.

    Attributes:
        `base_dir (Path)`: The base directory where the generated graphs will be saved.
        `env_extensions (dict)`: A dictionary mapping environments to their file extensions.
    """

    def __init__(self, base_dir: str, env_extensions: dict[str, str]):
        """
        The constructor for the GraphGenerator class.

        Args:
            `base_dir (str)`: The base directory where the generated graphs will be saved.
            `env_extensions (dict)`: A dictionary mapping environments to their file extensions.
        """
        self.base_dir = Path(base_dir)
        self.env_extensions = env_extensions

    def save_for_alda(self, graph_generator_func, size, filename: Path):
        graph_generator = graph_generator_func(size)
        data_set_of_tuples = set(graph_generator)
        with open(filename, 'wb') as f:
            pickle.dump(data_set_of_tuples, f)

    def save_for_souffle(
        self, graph_generator_func, size, filename: Path, fact_name: str = 'edge'
    ):
        graph_generator = graph_generator_func(size)
        with open(filename, 'w') as file:
            for value in graph_generator:
                first, second = value
                file.write(f'{first}\t{second}\n')

    def save_for_clingo_xsb(
        self, graph_generator_func, size: int, filename: Path, fact_name: str = 'edge'
    ):
        graph_generator = graph_generator_func(size)
        with open(filename, 'w') as file:
            for value in graph_generator:
                file.write(f'{fact_name}' + str(value) + '.\n')

    def generate_and_save_graphs(self, graph_type: str, size: int):
        data_gen = DataGenerator()
        generate_graph_method = getattr(data_gen, f'generate_{graph_type}_graph', None)

        if generate_graph_method is None:
            logging.error(
                f"Graph type '{graph_type}' is not supported or method is missing."
            )
            return

        for env, ext in self.env_extensions.items():
            # Use a combined folder for 'clingo' and 'xsb'
            if env in ['clingo', 'xsb']:
                combined_env = 'clingo_xsb'
                filename = (
                    self.base_dir / combined_env / graph_type / f'graph_{size}.lp'
                )
            else:
                filename = self.base_dir / env / graph_type / f'graph_{size}{ext}'

            # Ensure the directory exists
            filename.parent.mkdir(parents=True, exist_ok=True)

            # Handling different environments
            if env == 'alda':
                self.save_for_alda(generate_graph_method, size, filename)
            elif env == 'souffle':
                fact_name = 'edge'
                filename = (
                    self.base_dir / env / graph_type / f'{size}' / f'{fact_name}.facts'
                )
                filename.parent.mkdir(parents=True, exist_ok=True)
                self.save_for_souffle(generate_graph_method, size, filename, fact_name)
            elif env in ['clingo', 'xsb']:
                # For combined 'clingo' and 'xsb', we use the filename calculated with the combined_env
                self.save_for_clingo_xsb(
                    generate_graph_method, size, filename, fact_name='edge'
                )

    def generate_graphs(self, size_ranges: list[int], graph_types: list[str]) -> None:
        """
        Generates graphs of the specified types and sizes, and saves them in all supported formats.

        Args:
            `size_ranges (list)`: The sizes of the graphs to be generated.
            `graph_types (list)`: The types of the graphs to be generated.
        """
        for size in size_ranges:
            logging.info(f'Generating graphs for size {size}.')
            for graph_type in graph_types:
                logging.info(f'Generating graphs for type {graph_type} of size {size}.')
                self.generate_and_save_graphs(graph_type, size)


def main():
    parser = argparse.ArgumentParser()
    # Specify the start, stop, and step sizes for graph generation
    parser.add_argument(
        '--sizes',
        type=int,
        nargs=3,
        metavar=('START', 'STOP', 'STEP'),
        default=[10, 101, 10],
        help='The range of sizes as a start stop and step. Default is 10 101 10.',
    )
    # Specify the graph types to generate
    parser.add_argument(
        '--graph-types',
        nargs='+',
        default=[
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
        ],
        help='The types of graphs to run the experiment on. Default is complete cycle cycle_with_shortcuts star max_acyclic path multi_path binary_tree reverse_binary_tree w y x.',
    )
    args = parser.parse_args()

    env_extensions = {'alda': '.pickle', 'xsb': '.P', 'clingo': '.lp', 'souffle': '.dl'}
    # env_extensions = {'xsb': '.P', 'clingo': '.lp', 'souffle': '.dl'}
    logging.info(
        f'Generating graphs for sizes {args.sizes} and types {args.graph_types}.'
    )
    generator = GraphGenerator('input', env_extensions)
    generator.generate_graphs(list(range(*args.sizes)), args.graph_types)


if __name__ == '__main__':
    gc.disable()
    main()
