"""
Generate the benchmark's input graphs and write them in every system's input format.

Each graph type is a `generate_<type>_graph` method of DataGenerator. GraphGenerator writes one
graph of one size as a tab-separated edge file (input/souffle/, read by Souffle and the database
systems), as Prolog facts (input/clingo_xsb/, read by XSB and Clingo) and as a pickled edge set
(input/alda/), each with a queries_<n>.csv file for demand-driven runs. engine/campaign.py calls
this script for the inputs a campaign lacks:

    python generate_db.py --graph-types cycle path --size-list 100 200 300
"""

import argparse
import csv
import gc
import json
import logging
import math
import os
import pickle
import random
from pathlib import Path
from typing import Any, Callable, Generator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# Built-in defaults, used when the config has no 'defaults' key the 'defaults' key (e.g. new config.yaml)
_DEFAULT_SYSTEMS: dict[str, Any] = {
    'environmentExtensions': {
        'clingo': '.lp',
        'xsb': '.P',
        'souffle': '.dl',
        'postgres': '.py',
        'mariadb': '.py',
        'duckdb': '.sql',
        'neo4j': '.cypher',
        'mongodb': '.py',
        'cockroachdb': '.py',
        'alda': '.da',
    },
    'dbSystems': ['postgres', 'mariadb', 'duckdb', 'mongodb', 'neo4j', 'cockroachdb'],
    'otherLogicSystems': ['xsb', 'clingo', 'souffle'],
    'alda': ['alda'],
}


class DataGenerator:
    """
    Generators of the benchmark graphs, one method per graph type.

    The graphs follow the paper "Performance Analysis and Comparison of Deductive Systems and SQL
    Databases" (https://ceur-ws.org/Vol-2368/paper3.pdf), with some changes and further graph types.
    Every method yields (source, target) edges; GraphGenerator adds a weight in the shortest_path
    domain.
    """

    def __init__(self, domain: str = 'transitive_closure'):
        """Use k = 10 for the graph types that take a second parameter."""
        self.k = 10
        self.domain = domain

    def generate_complete_graph(self, n: int) -> Generator[tuple[int, int], None, None]:
        """Generate a complete graph with n nodes."""
        # self.E = {(i, j) for i in range(1, n + 1) for j in range(1, n + 1)}
        logging.info(f'Generating complete graph for n={n}')
        for i in range(1, n + 1):
            for j in range(1, n + 1):
                yield (i, j)

    def generate_max_acyclic_graph(self, n: int) -> Generator[tuple[int, int], None, None]:
        """Generate a max acyclic graph with n nodes."""
        # self.E = {(a, b) for a in range(1, n + 1) for b in range(1, a) if a > b}
        logging.info(f'Generating max acyclic graph for n={n}')
        for a in range(1, n + 1):
            for b in range(1, a):
                if a > b:
                    yield (a, b)

    def generate_cycle_graph(self, n: int) -> Generator[tuple[int, int], None, None]:
        """Generate a cycle graph with n nodes."""
        # E = {(i, i + 1) for i in range(1, n)} | {(n, 1)}
        logging.info(f'Generating cycle graph for n={n}')
        for i in range(1, n):
            yield (i, i + 1)
        yield (n, 1)

    def generate_cycle_with_shortcuts_graph(self, n: int) -> Generator[tuple[int, int], None, None]:
        """Generate a cycle with shortcuts graph with n nodes."""
        logging.info(f'Generating cycle with shortcuts graph for n={n}')

        skip = n // (self.k + 1)  # Number of vertices to skip for shortcuts
        for i in range(1, n):
            yield (i, i + 1)
        yield (n, 1)
        for i in range(1, n + 1):
            for t in range(1, self.k + 1):
                yield (i, 1 + (i - 1 + skip * t) % n)

    def generate_path_graph(self, n: int) -> Generator[tuple[int, int], None, None]:
        """Generate a path graph with n nodes."""
        logging.info(f'Generating path graph for n={n}')
        for i in range(1, n):
            yield (i, i + 1)

    def generate_multi_path_graph(self, n: int) -> Generator[tuple[int, int], None, None]:
        """Generate a multi path graph with n nodes."""
        logging.info(f'Generating multi path graph for n={n}')
        for i in range(1, (n - 1) * self.k + 1):
            yield (i, i + self.k)

    def generate_binary_tree_graph(self, n: int) -> Generator[tuple[int, int], None, None]:
        """Generate a binary tree graph with n nodes."""
        h = math.floor(math.log2(n))
        logging.info(f'Generating binary tree graph for n={n} and h={h}')
        parent_count = 2 ** (h - 1) - 1
        for i in range(1, parent_count + 1):
            yield (i, 2 * i)
            yield (i, 2 * i + 1)

    def generate_reverse_binary_tree_graph(self, n: int) -> Generator[tuple[int, int], None, None]:
        """Generate a reverse binary tree graph with n nodes."""
        h = math.floor(math.log2(n))
        logging.info(f'Generating reverse binary tree graph for n={n} and h={h}')
        parent_count = 2 ** (h - 1) - 1
        for i in range(1, parent_count + 1):
            yield (2 * i, i)
            yield (2 * i + 1, i)

    def generate_y_graph(self, n: int) -> Generator[tuple[int, int], None, None]:
        """Generate a Y graph with n nodes."""
        logging.info(f'Generating Y graph for n={n}')
        for i in range(1, n + 1):
            yield (i, n + 1)
        for i in range(n + 2, n + self.k + 1):
            yield (i - 1, i)

    def generate_w_graph(self, n: int) -> Generator[tuple[int, int], None, None]:
        """Generate a W graph with n nodes."""
        logging.info(f'Generating W graph for n={n}')
        for i in range(1, n + 1):
            for j in range(1, self.k + 1):
                yield (i, n + 1 + (i + j - 1) % n)

    def generate_x_graph(self, n: int) -> Generator[tuple[int, int], None, None]:
        """Generate a X graph with n nodes."""
        logging.info(f'Generating X graph for n={n}')
        for i in range(1, n + 1):
            yield (i, n + 1)
        for j in range(1, self.k + 1):
            yield (n + 1, n + 1 + j)

    def generate_star_graph(self, n: int) -> Generator[tuple[int, int], None, None]:
        """Generate a star graph with n nodes."""
        logging.info(f'Generating star graph for n={n}')
        for i in range(2, n + 1):
            yield (i, 1)

    def generate_grid_graph(self, n: int) -> Generator[tuple[int, int], None, None]:
        """Generate a grid graph with n nodes."""
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

    def generate_barabasi_albert_graph(self, n: int, m: int = 2) -> Generator[tuple[int, int], None, None]:
        """
        Generate a Barabási-Albert graph (the scale-free model of real-world networks).

        With the fixed seed, the graph of n nodes is a subgraph of the graph of any larger n.
        """
        import networkx as nx

        logging.info(f'Generating Barabási-Albert graph for n={n} (m={m})')

        if n <= m:
            for i in range(1, n + 1):
                for j in range(i + 1, n + 1):
                    yield (i, j)
        else:
            graph = nx.barabasi_albert_graph(n, m, seed=42)
            for u, v in graph.edges():
                yield (u + 1, v + 1)

    def generate_scale_free_graph(self, n: int) -> Generator[tuple[int, int], None, None]:
        """Generate a scale-free directed graph."""
        import networkx as nx

        logging.info(f'Generating scale-free graph for n={n}')
        graph = nx.scale_free_graph(n, seed=42)
        # a MultiDiGraph: parallel edges are yielded once per edge (see save_for_clingo_xsb)
        for u, v in graph.edges():
            yield (u + 1, v + 1)


class GraphGenerator:
    """
    Write generated graphs in every input format (weighted edges in the shortest_path domain).

    Parameters
    ----------
    base_dir : str
        Directory that receives the inputs (usually input/).
    config : dict
        Global configuration; its defaults.systems section can change the list of formats.
    domain : str
        transitive_closure, shortest_path, and so on.

    """

    def __init__(self, base_dir: str, config: dict[str, Any], domain: str = 'transitive_closure'):
        """Keep the output directory, the configuration and the domain."""
        self.base_dir = Path(base_dir)
        self.config = config
        self.domain = domain

    def save_for_alda(
        self, graph_generator_func: Callable[[int], Generator[tuple[int, int], None, None]], size: int, filename: Path
    ):
        """Pickle the set of edges for ALDA."""
        graph_generator = graph_generator_func(size)
        data_set_of_tuples = set(graph_generator)
        with open(filename, 'wb') as f:
            pickle.dump(data_set_of_tuples, f)

    def save_for_souffle(
        self,
        graph_generator_func: Callable[[int], Generator[tuple, None, None]],
        size: int,
        filename: Path,
        fact_name: str = 'edge',
    ):
        """Write the edges as tab-separated values (Souffle's .facts format, also read by the databases)."""
        graph_generator = graph_generator_func(size)
        with open(filename, 'w', encoding='utf-8') as file:
            for value in graph_generator:
                file.write('\t'.join(map(str, value)) + '\n')

    def save_for_clingo_xsb(
        self,
        graph_generator_func: Callable[[int], Generator[tuple[int, int], None, None]],
        size: int,
        filename: Path,
        fact_name: str = 'edge',
    ):
        """Write the edges as Prolog facts, edge(a,b). (read by XSB and Clingo)."""
        values = list(graph_generator_func(size))
        # Multigraph generators (networkx.scale_free_graph) yield parallel edges. The database
        # systems load the TSV file as generated (a table with duplicate rows; the closure is the
        # same), but for the logic systems every fact is written once, sorted, so that they do not
        # re-derive identical facts. Duplicate-free generators are written in generation order.
        if len(values) != len(set(values)) and all(len(v) == 2 for v in values):
            values = sorted(set(values))
        with open(filename, 'w', encoding='utf-8') as file:
            for value in values:
                if isinstance(value, tuple) and len(value) == 3:
                    # Weighted edge: (src, dst, weight)
                    file.write(f'{fact_name}({value[0]},{value[1]},{value[2]}).\n')
                else:
                    # Standard edge: (src, dst)
                    file.write(f'{fact_name}' + str(value) + '.\n')

    @staticmethod
    def _write_queries(directory: Path, graph_type: str, size: int) -> None:
        """Write queries_<n>.csv (one random start node for demand-driven runs) unless it exists."""
        queries_file = directory / f'queries_{size}.csv'
        if queries_file.exists():
            return
        directory.mkdir(parents=True, exist_ok=True)
        max_node = int(math.sqrt(size)) ** 2 if 'grid' in graph_type else size
        with open(queries_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['X'])
            writer.writerow([random.randint(1, max(1, max_node))])  # nosec B311  # not for security

    def generate_and_save_graphs(self, graph_type: str, size: int):
        """Write one graph of one size in every input format, each with its queries_<n>.csv."""
        data_gen = DataGenerator(domain=self.domain)
        base_method = getattr(data_gen, f'generate_{graph_type}_graph', None)
        if base_method is None:
            logging.error(f"Graph type '{graph_type}' is not supported.")
            return

        def generate_graph_method(s):
            """Yield the edges, with a weight from a seeded generator in the shortest_path domain."""
            random.seed(42)  # the same weights for every format of a graph
            for edge in base_method(s):
                if self.domain == 'shortest_path':
                    yield (*edge, random.randint(1, 100))  # nosec B311  # benchmark data, not security
                else:
                    yield edge

        config = self.config.get('defaults', {}).get('systems', _DEFAULT_SYSTEMS)
        db_systems = config.get('dbSystems', [])
        for env, ext in config.get('environmentExtensions', {}).items():
            if env in ('clingo', 'xsb'):  # one file for both, in input/clingo_xsb/
                filename = self.base_dir / 'clingo_xsb' / graph_type / f'graph_{size}.lp'
                self._write_queries(filename.parent, graph_type, size)
                self.save_for_clingo_xsb(generate_graph_method, size, filename, fact_name='edge')
            elif env == 'souffle':
                fact_name = 'edge' if self.domain != 'shortest_path' else 'edge_weighted'
                filename = self.base_dir / env / graph_type / f'{size}' / f'{fact_name}.facts'
                filename.parent.mkdir(parents=True, exist_ok=True)
                self.save_for_souffle(generate_graph_method, size, filename, fact_name)
                # written after the edges, as before: in the shortest_path domain the edges take
                # numbers from the same random generator, and the start node must not change
                self._write_queries(filename.parent, graph_type, size)
            elif env not in db_systems:  # the databases read Souffle's tab-separated file
                filename = self.base_dir / env / graph_type / f'graph_{size}{ext}'
                self._write_queries(filename.parent, graph_type, size)
                if env == 'alda':
                    self.save_for_alda(generate_graph_method, size, filename)

    def generate_graphs(self, size_ranges: list[int], graph_types: list[str]) -> None:
        """Write every graph type at every size (`size_ranges` is a list of sizes) in all formats."""
        for size in size_ranges:
            logging.info(f'Generating graphs for size {size}.')
            for graph_type in graph_types:
                logging.info(f'Generating graphs for type {graph_type} of size {size}.')
                self.generate_and_save_graphs(graph_type, size)


def main():
    """Parse the options and write the requested graphs into input/."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--config', type=str, required=False, help='JSON string of the config')
    # Specify domain for domain-specific graph generation
    parser.add_argument(
        '--domain',
        type=str,
        default='transitive_closure',
        help='Domain for experiment (transitive_closure, shortest_path, etc.). Default is transitive_closure.',
    )
    # Specify the start, stop, and step sizes for graph generation
    parser.add_argument(
        '--sizes',
        type=int,
        nargs=3,
        metavar=('START', 'STOP', 'STEP'),
        default=[10, 101, 10],
        help='The range of sizes as a start stop and step. Default is 10 101 10.',
    )
    # Or an explicit list of sizes (used by engine/campaign.py for the sizes that are missing)
    parser.add_argument(
        '--size-list',
        type=int,
        nargs='+',
        metavar='N',
        help='Explicit graph sizes; overrides --sizes.',
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
            'barabasi_albert',
            'scale_free',
        ],
        help='The graph types to generate. Default: all of them.',
    )
    args = parser.parse_args()

    if args.config and os.path.isfile(args.config):
        with open(args.config, 'r', encoding='utf-8') as file:
            config_content = file.read()
        config = json.loads(config_content)
    else:
        config = json.loads(args.config if args.config else '{}')

    sizes = args.size_list or args.sizes
    logging.info(f'Generating graphs for domain={args.domain}, sizes {sizes} and types {args.graph_types}.')
    generator = GraphGenerator('input', config, domain=args.domain)
    sizes = args.size_list if args.size_list else list(range(*args.sizes))
    generator.generate_graphs(sizes, args.graph_types)


if __name__ == '__main__':
    gc.disable()
    main()
