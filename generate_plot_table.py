import argparse
import json
import logging
import math
import re
import csv
import subprocess
from pathlib import Path
from typing import Any, Callable, Union

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s'
)


class BaseTableAndPlotGenerator:
    def __init__(self, timing_base_dir: Path, pattern: str, latex_file_dir: Path):
        self.timing_base_dir = timing_base_dir
        self.pattern = pattern
        self.latex_file_dir = latex_file_dir
        self.data = None
        self.components = self.__initialize_components()
        self.component_colors = self.__initialize_component_colors()
        self.component_legend = self.__initialize_component_legends()
        self.graphs_k_values = self.__initialize_graphs_k_values()

    @staticmethod
    def __initialize_components() -> dict[str, list[str]]:
        common_components = [
            'CreateTable',
            'LoadData',
            'CreateIndex',
            'Analyze',
            'ExecuteQuery',
            'WriteRes',
        ]
        mongodb_components = [comp for comp in common_components if comp != 'Analyze']
        neo4j_components = ['DeleteData'] + mongodb_components[1:]
        return {
            'alda': ['Overall'],
            'xsb': ['LoadRules', 'LoadFacts', 'Querying', 'Writing'],
            'clingo': ['LoadRules', 'LoadFacts', 'Ground', 'Querying', 'Writing'],
            'souffle': [
                'DatalogToCpp',
                'CppToO',
                'InstanceLoading',
                'LoadFacts',
                'Querying',
                'Writing',
            ],
            'postgres': common_components,
            'mariadb': common_components,
            'duckdb': common_components,
            'cockroachdb': common_components,
            'mongodb': mongodb_components,
            'neo4j': neo4j_components,
        }

    @staticmethod
    def __initialize_component_colors() -> dict[str, dict[str, str]]:
        common_colors = {
            'CreateTable': 'LightGoldenrod',
            'LoadData': 'DarkOrange',
            'CreateIndex': 'LightPink',
            'Analyze': 'LimeGreen',
            'ExecuteQuery': 'Crimson',
            'WriteRes': 'DodgerBlue',
        }
        mongodb_colors = {
            key: value for key, value in common_colors.items() if key != 'Analyze'
        }
        neo4j_colors = mongodb_colors.copy()
        neo4j_colors['DeleteData'] = neo4j_colors.pop('CreateTable')
        return {
            'alda': {'Overall': 'LimeGreen'},
            'xsb': {
                'LoadRules': 'LightPink',
                'LoadFacts': 'LimeGreen',
                'Querying': 'Crimson',
                'Writing': 'DodgerBlue',
            },
            'clingo': {
                'LoadRules': 'LightPink',
                'LoadFacts': 'LimeGreen',
                'Ground': 'OrangeRed',
                'Querying': 'FireBrick',
                'Writing': 'DodgerBlue',
            },
            'souffle': {
                'DatalogToCpp': 'LightGoldenrod',
                'CppToO': 'DarkOrange',
                'InstanceLoading': 'LightPink',
                'LoadFacts': 'LimeGreen',
                'Querying': 'Crimson',
                'Writing': 'DodgerBlue',
            },
            'postgres': common_colors,
            'mariadb': common_colors,
            'duckdb': common_colors,
            'cockroachdb': common_colors,
            'mongodb': mongodb_colors,
            'neo4j': neo4j_colors,
        }

    @staticmethod
    def __initialize_component_legends() -> dict[str, dict[str, str]]:
        common_legends = {
            'CreateTable': 'CreateTable',
            'LoadData': 'LoadData',
            'CreateIndex': 'CreateIndex',
            'Analyze': 'Analyze',
            'ExecuteQuery': 'Query',
            'WriteRes': 'WriteRes',
        }
        mongodb_legends = {
            key: value for key, value in common_legends.items() if key != 'Analyze'
        }
        neo4j_legends = mongodb_legends.copy()
        neo4j_legends['DeleteData'] = neo4j_legends.pop('CreateTable')
        return {
            'alda': {'Overall': 'Overall'},
            'xsb': {
                'LoadRules': 'LoadRules',
                'LoadFacts': 'ReadData',
                'Querying': 'Query',
                'Writing': 'WriteRes',
            },
            'clingo': {
                'LoadRules': 'LoadRules',
                'LoadFacts': 'ReadData',
                'Ground': 'Ground',
                'Querying': 'Solve',
                'Writing': 'WriteRes',
            },
            'souffle': {
                'DatalogToCpp': 'RulesToC++',
                'CppToO': 'C++ToExec',
                'InstanceLoading': 'LoadRules',
                'LoadFacts': 'ReadData',
                'Querying': 'Query',
                'Writing': 'WriteRes',
            },
            'postgres': common_legends,
            'mariadb': common_legends,
            'duckdb': common_legends,
            'cockroachdb': common_legends,
            'mongodb': mongodb_legends,
            'neo4j': neo4j_legends,
        }

    @staticmethod
    def __initialize_graphs_k_values() -> dict[str, Union[int, str]]:
        return {
            'cycle_with_shortcuts': 10,
            'multi_path': 10,
            'w': 10,
            'y': 10,
            'x': 'n',
        }

    def __collect_data(self) -> None:
        data = {}
        for csv_file in self.timing_base_dir.glob('**/*_graph_*.csv'):
            try:
                parts = csv_file.parts
                env_name, graph_type = parts[-3], parts[-2]
                mode, graph_size = re.match(self.pattern, csv_file.name).groups()
                graph_size = int(graph_size)

                key = (env_name, graph_type, mode)
                if key not in data:
                    data[key] = []

                with csv_file.open('r') as file:
                    lines = file.readlines()
                    last_line = lines[-1].strip().split(',')

                    if env_name == 'clingo':
                        data[key].append(
                            (graph_size, self.__process_clingo_data(last_line))
                        )
                    elif env_name == 'xsb':
                        data[key].append(
                            (graph_size, self.__process_xsb_data(last_line))
                        )
                    elif env_name == 'souffle':
                        data[key].append(
                            (graph_size, self.__process_souffle_data(last_line))
                        )
                    elif env_name in [
                        'postgres',
                        'mariadb',
                        'duckdb',
                        'cockroachdb',
                    ]:
                        data[key].append(
                            (graph_size, self.__process_sql_data(last_line))
                        )
                    elif env_name == 'mongodb':
                        data[key].append(
                            (graph_size, self.__process_mongo_data(last_line))
                        )
                    elif env_name == 'neo4j':
                        data[key].append(
                            (graph_size, self.__process_neo4j_data(last_line))
                        )
                    elif env_name == 'alda':
                        data[key].append(
                            (graph_size, self.__process_alda_data(last_line))
                        )
            except Exception as e:
                logging.error(f"Error processing file {csv_file}: {e}")
        self.data = data

    @staticmethod
    def __process_clingo_data(last_line: list[str]) -> dict[str, tuple[float, float]]:
        return {
            'LoadRules': (float(last_line[1]), float(last_line[2])),
            'LoadFacts': (float(last_line[3]), float(last_line[4])),
            'Ground': (float(last_line[5]), float(last_line[6])),
            'Querying': (float(last_line[7]), float(last_line[8])),
            'Writing': (float(last_line[9]), float(last_line[10])),
        }

    @staticmethod
    def __process_xsb_data(last_line: list[str]) -> dict[str, tuple[float, float]]:
        return {
            'LoadRules': (float(last_line[1]), float(last_line[2])),
            'LoadFacts': (float(last_line[3]), float(last_line[4])),
            'Querying': (float(last_line[5]), float(last_line[6])),
            'Writing': (float(last_line[7]), float(last_line[8])),
        }

    @staticmethod
    def __process_souffle_data(last_line: list[str]) -> dict[str, tuple[float, float]]:
        return {
            'DatalogToCpp': (float(last_line[1]), float(last_line[2])),
            'CppToO': (float(last_line[3]), float(last_line[4])),
            'InstanceLoading': (float(last_line[5]), float(last_line[6])),
            'LoadFacts': (float(last_line[7]), float(last_line[8])),
            'Querying': (float(last_line[9]), float(last_line[10])),
            'Writing': (float(last_line[11]), float(last_line[12])),
        }

    @staticmethod
    def __process_sql_data(last_line: list[str]) -> dict[str, tuple[float, float]]:
        return {
            'CreateTable': (float(last_line[1]), float(last_line[2])),
            'LoadData': (float(last_line[3]), float(last_line[4])),
            'CreateIndex': (float(last_line[5]), float(last_line[6])),
            'Analyze': (float(last_line[7]), float(last_line[8])),
            'ExecuteQuery': (float(last_line[9]), float(last_line[10])),
            'WriteRes': (float(last_line[11]), float(last_line[12])),
        }

    @staticmethod
    def __process_mongo_data(last_line: list[str]) -> dict[str, tuple[float, float]]:
        return {
            'CreateTable': (float(last_line[1]), float(last_line[2])),
            'LoadData': (float(last_line[3]), float(last_line[4])),
            'CreateIndex': (float(last_line[5]), float(last_line[6])),
            'ExecuteQuery': (float(last_line[7]), float(last_line[8])),
            'WriteRes': (float(last_line[9]), float(last_line[10])),
        }

    @staticmethod
    def __process_neo4j_data(last_line: list[str]) -> dict[str, tuple[float, float]]:
        return {
            'DeleteData': (float(last_line[1]), float(last_line[2])),
            'LoadData': (float(last_line[3]), float(last_line[4])),
            'CreateIndex': (
                float(last_line[5]) + float(last_line[7]),
                float(last_line[6]) + float(last_line[8]),
            ),
            'ExecuteQuery': (float(last_line[9]), float(last_line[10])),
            'WriteRes': (float(last_line[11]), float(last_line[12])),
        }

    @staticmethod
    def __process_alda_data(last_line: list[str]) -> dict[str, tuple[float, float]]:
        try:
            elapsed_time = (float(last_line[1]), float(last_line[2]))
        except IndexError:
            elapsed_time = (float(last_line[0]), float(last_line[1]))
        return {'Overall': elapsed_time}

    def __find_cpu_time(
        self, key: tuple[str, str, str], size: int, query_type: str
    ) -> Union[float, None]:
        if key in self.data:
            for entry in self.data[key]:
                if entry[0] == size:
                    return entry[1][query_type][1]
        return None

    def __find_max_cpu_time_across_envs(self, graph_type: str, mode: str) -> float:
        max_cpu_time = 0
        for key, entries in self.data.items():
            env_name, g_type, m = key
            if g_type == graph_type and m == mode:
                for entry in entries:
                    cpu_times = [value[1] for value in entry[1].values()]
                    max_cpu_time = max(max_cpu_time, max(cpu_times))
        return max_cpu_time

    def __find_max_real_time(self, env_name: str, graph_type: str, mode: str) -> float:
        max_real_time = 0
        key = (env_name, graph_type, mode)
        if key in self.data:
            for entry in self.data[key]:
                real_times = [value[0] for value in entry[1].values()]
                max_real_time = max(max_real_time, max(real_times))
        return max_real_time

    def __find_max_real_time_across_envs(self, graph_type: str, mode: str) -> float:
        max_real_time = 0
        for key, entries in self.data.items():
            env_name, g_type, m = key
            if g_type == graph_type and m == mode:
                for entry in entries:
                    real_times = [value[0] for value in entry[1].values()]
                    max_real_time = max(max_real_time, max(real_times))
        return max_real_time

    @staticmethod
    def __is_latexindent_installed() -> bool:
        try:
            subprocess.run(
                ['latexindent', '--version'],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False

    def __format_latex_file(self, file_path: Path) -> None:
        if self.__is_latexindent_installed():
            try:
                subprocess.run(['latexindent', '-w', str(file_path)], check=True)
                logging.info(f'Formatted {file_path} using latexindent.')
            except subprocess.CalledProcessError as e:
                logging.error(f'Error formatting {file_path} using latexindent: {e}')
        else:
            logging.warning('latexindent is not installed. Skipping formatting.')

    @staticmethod
    def __list_to_ordered_set(input_list: list) -> set:
        return set(dict.fromkeys(input_list))

    def __compile_latex_to_pdf(self, directory: Path) -> None:
        latex_distribution = self.__find_latex_distribution()
        if latex_distribution is None:
            logging.error('No LaTeX distribution found on the system.')
            return

        for f in directory.iterdir():
            logging.info(f'Compiling {f} to PDF.')
            if f.suffix == '.tex':
                self.__compile_file(f, latex_distribution, directory)

        for f in directory.iterdir():
            if f.suffix not in ['.tex', '.pdf']:
                f.unlink()

    @staticmethod
    def __find_latex_distribution() -> Union[str, None]:
        latex_distributions = ['xelatex', 'pdflatex', 'lualatex']
        for distribution in latex_distributions:
            try:
                subprocess.run(
                    ['which', distribution], check=True, stdout=subprocess.DEVNULL
                )
                return distribution
            except subprocess.CalledProcessError:
                continue
        return None

    @staticmethod
    def __compile_file(file: Path, latex_distribution: str, directory: Path) -> None:
        try:
            if latex_distribution == 'xelatex':
                subprocess.run(
                    [latex_distribution, '--shell-escape', file.name],
                    cwd=directory,
                    check=True,
                )
            else:
                subprocess.run(
                    [latex_distribution, file.name], cwd=directory, check=True
                )
        except subprocess.CalledProcessError as e:
            logging.error(f'Error compiling {file}: {e}')

    @staticmethod
    def __read_latex_content(file_path: Path) -> str:
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()

    def __extract_axis_content(
        self, latex_content: str, tool: str, environments: list[str]
    ) -> str:
        match = re.search(r'\\begin{axis}.*?\\end{axis}', latex_content, re.DOTALL)
        if match:
            axis_content = match.group(0)
            if tool == environments[0]:
                axis_content = re.sub(
                    r'\\begin{axis}\[',
                    r'\\begin{axis}[bar shift=-25pt, ',
                    axis_content,
                    1,
                )
            elif tool in environments[1:]:
                axis_content = self.__adjust_axis_content(
                    axis_content, tool, environments
                )
            return axis_content
        return ''

    @staticmethod
    def __adjust_axis_content(
        axis_content: str, tool: str, environments: list[str]
    ) -> str:

        axis_content = re.sub(
            r'\\begin{axis}\[',
            rf'\\begin{{axis}}[bar shift={-25 + 3.7 * environments.index(tool)}pt, ',
            axis_content,
            1,
        )

        axis_content = re.sub(r'(axis x line\*=)[^,]*', r'\1none', axis_content)
        axis_content = re.sub(r'(axis y line\*=)[^,]*', r'\1none', axis_content)
        axis_content = re.sub(
            r'major grid style={draw=gray!20},',
            'major grid style={draw=none},',
            axis_content,
        )
        axis_content = re.sub(r'xlabel={.*?},', '', axis_content)
        axis_content = re.sub(r'ylabel={.*?},', '', axis_content)
        axis_content = re.sub(r'(?m)^\s*\n', '', axis_content)
        return axis_content

    @staticmethod
    def __write_latex_header(f) -> None:
        f.write('\\documentclass[border=10pt]{standalone}\n')
        f.write('\\usepackage[svgnames]{xcolor}\n')
        f.write('\\usepackage{amsmath}\n')
        f.write('\\usepackage{pgfplots}\n')
        f.write('\\pgfplotsset{compat=newest}\n')
        f.write('\\usepackage[sfdefault]{FiraSans}\n')
        f.write('\\usepackage{FiraMono}\n')
        f.write('\\renewcommand*\\familydefault{\\sfdefault}\n')
        f.write('\\begin{document}\n')

    @staticmethod
    def __write_latex_footer(f) -> None:
        f.write('\\end{document}\n')

    def __adjust_ymax(self, max_value_func: Callable, func_params: tuple) -> float:
        max_value = max_value_func(*func_params)
        if max_value < 1:
            return max_value + 0.02
        elif 1 <= max_value < 5:
            return max_value + 8
        elif 5 <= max_value < 10:
            return max_value + 20
        elif 10 <= max_value < 100:
            return max_value + 40
        elif 100 <= max_value < 1000:
            return max_value + 50
        elif 1000 <= max_value < 10000:
            return max_value + 500
        elif 10000 <= max_value < 100000:
            return max_value + 5000


class TableAndPlotGenerator(BaseTableAndPlotGenerator):
    def __init__(
        self,
        timing_base_dir: Path,
        pattern: str,
        latex_file_dir: Path,
        config: dict[str, Any] | None = None,
        envs: list[str] | None = None,
    ):
        super().__init__(timing_base_dir, pattern, latex_file_dir)
        self.config = config
        self.environments = config['environmentsToCombine'] if config else envs

    def __write_latex_body_for_environment(
        self,
        f,
        env_name: str,
        key: tuple[str, str, str],
        values: list[tuple[int, dict[str, tuple[float, float]]]],
        component_legend_colors: list[tuple[str, str]],
        max_real_time: float,
    ) -> None:
        """
        Write the LaTeX body to the file.

        Args:
            `f`: The file object to write to.
            `env_name (str)`: The environment name.
            `key (tuple)`: The key containing environment, graph type, and mode.
            `values (list)`: The sorted values.
            `component_legend_colors (list[tuple[str, str]])`: A list of tuples containing the component name and color.
            `max_real_time (float)`: The maximum real-time value.
        """
        f.write('\\begin{tikzpicture}\n')
        for i in range(2):
            f.write('\\begin{axis}[\n')
            f.write('   ybar stacked,\n')
            if i == 0:
                f.write(
                    f'   title={{{env_name.upper() if env_name=="xsb" else env_name.capitalize()} performance for {key[2].replace("_", " ").capitalize()}}},\n'
                )
                f.write('   bar shift=-10pt,\n')
            else:
                f.write('   bar shift=13pt,\n')
            f.write('   width=1.5\\textwidth,\n')
            f.write('   bar width=0.7cm,\n')
            f.write('   ymajorgrids, tick align=inside,\n')
            f.write(
                '   major grid style={draw=gray!20},\n'
                if i == 0
                else '   major grid style={draw=none},\n'
            )
            f.write('   xtick=data,\n')
            f.write(f'   ymin=0, ymax={max_real_time},\n')
            f.write(
                '   axis x line*=bottom,\n' if i == 0 else '   axis x line*=none,\n'
            )
            f.write('   axis y line*=left,\n' if i == 0 else '   axis y line*=none,\n')
            f.write('   enlarge x limits=0.1,\n')
            if i == 0:
                f.write('   legend style={\n')
                f.write('       at={(0.23, 1)},\n')
                f.write('       anchor=north,\n')
                f.write('       legend columns=1,\n')
                f.write('       font=\\Huge,\n')
                f.write('   },\n')
                f.write('   ylabel={Time (seconds)},\n')
                if key[1] in [
                    'complete',
                    'cycle',
                    'max_acyclic',
                    'grid',
                    'path',
                    'star',
                ]:
                    f.write('   xlabel={Number of nodes},\n')
                elif key[1] in ['cycle_with_shortcuts', 'w', 'y', 'multi_path']:
                    f.write('   xlabel={Number of nodes $\\times 10$},\n')
                elif key[1] in ['binary_tree', 'reverse_binary_tree']:
                    f.write('   xlabel={Height of the tree},\n')
            f.write('   label style={font=\\Huge},\n')
            f.write('   tick label style={font=\\Huge},\n')
            f.write(']\n')

            if i == 0:
                for entry, color in component_legend_colors:
                    f.write(
                        f'\\addlegendimage{{fill={color}, draw=black, line width=0.2pt}}\n'
                    )
                    f.write(
                        f'\\addlegendentry{{{self.component_legend[env_name][entry]}}}\n'
                    )

            for component in self.components[env_name]:
                color = self.component_colors[env_name][component]
                f.write(
                    f'\\addplot +[fill={color}, draw=black, line width=0.5pt] coordinates {{\n'
                )
                for value in values:
                    size_to_plot = value[0]
                    if key[1] in ['cycle_with_shortcuts', 'w', 'y', 'multi_path', 'x']:
                        size_to_plot *= 10
                    elif key[1] in ['binary_tree', 'reverse_binary_tree']:
                        size_to_plot = math.floor(math.log2(value[0]))
                    f.write(f'    ({size_to_plot}, {value[1][component][i]})\n')
                f.write('};\n')

            f.write('\\end{axis}\n')

        f.write('\\end{tikzpicture}\n\n')

    def __generate_latex_for_environment(
        self, output_dir: Path, env_name: str, compile_alone: bool
    ) -> None:
        """
        This function generates LaTeX files for a given environment.

        Args:
            `output_dir (Path)`: A Path object representing the directory where the generated LaTeX files will be saved.
            `env_name (str)`: The environment name.
            `compile_alone (bool)`: A boolean indicating whether to compile the LaTeX files to PDFs alone.

        The function operates as follows:
        1. It initializes the legend entries and the maximum value.
        2. It iterates over the data and sorts the values by graph size.
        3. It creates a directory for the environment if it doesn't exist.
        4. It creates a LaTeX file for each key in the data.
        5. It writes the necessary LaTeX commands for setting up the document and the plot.
        6. It writes the legend entries, the y-axis label, and the x-axis label.
        7. It writes the symbolic x coordinates for the plot.
        8. It writes the real-time plots for each component.
        9. It compiles the LaTeX files to PDFs using the compile_latex_to_pdf function.
        """

        if compile_alone:
            self._BaseTableAndPlotGenerator__compile_latex_to_pdf(output_dir / env_name)
            return

        component_legend_colors = [
            (component, self.component_colors[env_name][component])
            for component in reversed(self.components[env_name])
        ]

        for key, values in self.data.items():
            if key[0] == env_name:
                # Sort values by graph size
                values.sort(key=lambda x: x[0])

                folder_dir = output_dir / env_name
                folder_dir.mkdir(exist_ok=True, parents=True)
                file_name = f'{key[1]}_{key[2]}.tex'
                full_file_name = folder_dir / file_name

                # Find the maximum real-time value for double_recursion
                max_real_time = self._BaseTableAndPlotGenerator__adjust_ymax(
                    self._BaseTableAndPlotGenerator__find_max_real_time,
                    (env_name, key[1], 'left_recursion'),
                )

                with open(full_file_name, 'w') as f:
                    self._BaseTableAndPlotGenerator__write_latex_header(f)
                    self.__write_latex_body_for_environment(
                        f, env_name, key, values, component_legend_colors, max_real_time
                    )
                    self._BaseTableAndPlotGenerator__write_latex_footer(f)

                self._BaseTableAndPlotGenerator__format_latex_file(full_file_name)

        self._BaseTableAndPlotGenerator__compile_latex_to_pdf(output_dir / env_name)

    def __generate_latex_comparison_tables(self, latex_file_dir: Path) -> None:

        file_dir = latex_file_dir / 'comparison' / 'tables'
        file_dir.mkdir(exist_ok=True, parents=True)

        env_list = [key[0] for key in self.data if key[0] != 'alda']
        environments = sorted(
            list(self._BaseTableAndPlotGenerator__list_to_ordered_set(env_list)),
            reverse=True,
        )

        mds = [key[2] for key in self.data if key[0] in environments]
        modes = sorted(list(self._BaseTableAndPlotGenerator__list_to_ordered_set(mds)))
        sizes = sorted(
            {entry[0] for entries in self.data.values() for entry in entries}
        )

        for mode in modes:
            with open(file_dir / f'{mode}_tables.tex', 'w') as latex_file:
                latex_file.write('\\documentclass{article}\n')
                latex_file.write(
                    '\\usepackage{booktabs, siunitx, tabularx, adjustbox}\n'
                )
                latex_file.write('\\usepackage[table]{xcolor}\n')
                latex_file.write('\\usepackage{geometry}\n')
                latex_file.write('\\usepackage{caption}\n')
                latex_file.write('\\usepackage[sfdefault]{FiraSans}\n')
                latex_file.write('\\usepackage{FiraMono}\n')
                latex_file.write('\\renewcommand*\\familydefault{\\sfdefault}\n')
                latex_file.write('\\sisetup{round-mode=places, round-precision=2,}\n')
                latex_file.write('\\newcolumntype{R}{>{\\hsize=.5\\hsize}X}\n')
                latex_file.write('\\begin{document}\n')
                latex_file.write('\\begin{table}[h]\n\\centering\n')
                latex_file.write('\\rowcolors{4}{gray!15}{white}\n')
                latex_file.write('\\tiny\n')
                latex_file.write('\\begin{adjustbox}{max width=\\textwidth}\n')
                latex_file.write(
                    '\\begin{tabularx}{1.27\\textwidth}{X'
                    + ' S' * len(sizes) * 3
                    + '}\n'
                )
                latex_file.write('\\toprule\n')
                latex_file.write('\\rowcolor{gray!20}\n')
                latex_file.write('\\textbf{Graph Type} ')
                for size in sizes:
                    latex_file.write(
                        f'& \\multicolumn{{3}}{{c}}{{\\textbf{{{size}}}}} '
                    )
                latex_file.write('\\\\\n')
                latex_file.write('\\cmidrule(lr){2-4} ')
                for i in range(1, len(sizes)):
                    latex_file.write(f'\\cmidrule(lr){{{3*i+2}-{3*i+4}}} ')
                latex_file.write('\n')
                for _ in sizes:
                    latex_file.write(
                        '& \\textbf{XSB} & \\textbf{Clingo} & \\textbf{Souffle} '
                    )
                latex_file.write('\\\\\n')
                latex_file.write('\\midrule\n')

                graph_types = set(
                    key[1]
                    for key in self.data
                    if key[2] == mode and key[0] in environments
                )
                for graph_type in sorted(graph_types):
                    latex_file.write(f'{graph_type.replace("_", " ").title()} ')
                    for size in sizes:
                        xsb_time = self._BaseTableAndPlotGenerator__find_cpu_time(
                            ('xsb', graph_type, mode), size, 'Querying'
                        )
                        clingo_time = self._BaseTableAndPlotGenerator__find_cpu_time(
                            ('clingo', graph_type, mode), size, 'Querying'
                        )
                        souffle_time = self._BaseTableAndPlotGenerator__find_cpu_time(
                            ('souffle', graph_type, mode), size, 'Querying'
                        )
                        latex_file.write(
                            f'& {xsb_time if xsb_time is not None else "-"} & {clingo_time if clingo_time is not None else "-"} & {souffle_time if souffle_time is not None else "-"} '
                        )
                    latex_file.write('\\\\\n')
                latex_file.write('\\bottomrule\n')
                latex_file.write('\\end{tabularx}\n')
                latex_file.write('\\end{adjustbox}\n')
                latex_file.write(
                    f'\\caption{{CPU Time Comparison across XSB, Clingo, and Souffle for {mode.replace("_", " ").title()}}}\n'
                )
                latex_file.write(f'\\label{{tab:{mode}_comparison}}\n')
                latex_file.write('\\end{table}\n')
                latex_file.write('\\end{document}\n')

        self._BaseTableAndPlotGenerator__compile_latex_to_pdf(file_dir)

    def __generate_latex_comparison_charts(
        self, latex_file_dir: Path, env_name: str, compile_file_alone: bool
    ) -> None:
        file_dir = latex_file_dir / 'comparison' / 'charts' / env_name
        file_dir.mkdir(exist_ok=True, parents=True)

        mds = [key[2] for key in self.data if key[0] == env_name]
        modes = sorted(list(self._BaseTableAndPlotGenerator__list_to_ordered_set(mds)))
        sizes = sorted(
            {entry[0] for entries in self.data.values() for entry in entries}
        )

        component_legend_colors = [
            (component, self.component_colors[env_name][component])
            for component in reversed(self.components[env_name])
        ]

        for mode in modes:
            if compile_file_alone:
                self._BaseTableAndPlotGenerator__compile_latex_to_pdf(file_dir / mode)
                return
            graph_types = set(
                key[1] for key in self.data if key[2] == mode and key[0] == env_name
            )
            for graph_type in sorted(graph_types):
                ymax = self._BaseTableAndPlotGenerator__adjust_ymax(
                    self._BaseTableAndPlotGenerator__find_max_real_time_across_envs,
                    (graph_type, 'left_recursion'),
                )
                file_folder = file_dir / mode
                file_folder.mkdir(exist_ok=True, parents=True)
                file_path = file_folder / f'{graph_type}.tex'

                with open(file_path, 'w') as f:
                    self._BaseTableAndPlotGenerator__write_latex_header(f)
                    f.write('\\begin{tikzpicture}\n')
                    f.write('\\begin{axis}[\n')
                    f.write('   ybar stacked,\n')
                    f.write('   width=2\\textwidth,\n')
                    f.write('   bar width=0.35cm,\n')
                    f.write('   ymajorgrids, tick align=inside,\n')
                    f.write('   major grid style={draw=gray!20},\n')
                    f.write('   xtick=data,\n')
                    f.write(f'   ymin=0, ymax={ymax},\n')
                    f.write('   axis x line*=bottom,\n')
                    f.write('   axis y line*=left,\n')
                    f.write('   enlarge x limits=0.01,\n')
                    f.write('   legend style={\n')
                    if env_name == self.environments[0]:
                        f.write('       at={(0.23, 0.97)},\n')
                    else:
                        f.write(
                            f'       at={{({0.224 * (self.environments.index(env_name))+1}, 0.97)}},\n'
                        )
                    f.write('       anchor=north east,\n')
                    f.write('       legend columns=1,\n')
                    f.write('       font=\\Huge,\n')
                    f.write('   },\n')
                    f.write('   ylabel={CPU Time (seconds)},\n')
                    if graph_type in [
                        'complete',
                        'cycle',
                        'max_acyclic',
                        'grid',
                        'path',
                        'star',
                    ]:
                        f.write('   xlabel={Number of nodes},\n')
                    elif graph_type in [
                        'cycle_with_shortcuts',
                        'w',
                        'y',
                        'x',
                        'multi_path',
                    ]:
                        f.write('   xlabel={Number of nodes $\\times 10$},\n')
                    elif graph_type in ['binary_tree', 'reverse_binary_tree']:
                        f.write('   xlabel={Height of the tree},\n')
                    f.write('   label style={font=\\Huge},\n')
                    f.write('   tick label style={font=\\Huge},\n')
                    f.write(']\n')

                    for entry, color in component_legend_colors:
                        f.write(
                            f'\\addlegendimage{{fill={color}, draw=black, line width=0.2pt}}\n'
                        )
                        f.write(
                            f'\\addlegendentry{{{self.component_legend[env_name][entry]}}}\n'
                        )

                    for component in self.components[env_name]:
                        color = self.component_colors[env_name][component]
                        f.write(
                            f'\\addplot +[fill={color}, draw=black, line width=0.2pt] coordinates {{\n'
                        )
                        for size in sizes:
                            key = (env_name, graph_type, mode)
                            cpu_time = self._BaseTableAndPlotGenerator__find_cpu_time(
                                key, size, component
                            )
                            size_to_plot = size
                            if graph_type in [
                                'cycle_with_shortcuts',
                                'w',
                                'y',
                                'multi_path',
                                'x',
                            ]:
                                size_to_plot = size * 10
                            elif graph_type in ['binary_tree', 'reverse_binary_tree']:
                                size_to_plot = math.floor(math.log2(size))
                            f.write(f'({size_to_plot}, {cpu_time})\n')
                        f.write('};\n')
                    f.write('\\end{axis}\n')
                    f.write('\\end{tikzpicture}\n\n')
                    self._BaseTableAndPlotGenerator__write_latex_footer(f)

                self._BaseTableAndPlotGenerator__format_latex_file(file_path)

            self._BaseTableAndPlotGenerator__compile_latex_to_pdf(file_folder)

    def __combine_files(self, directory_path: Path, file_name: str, mode: str) -> str:
        """
        This function combines the axis content from different LaTeX files for comparison.

        Args:
            `directory_path (Path)`: A Path object representing the directory containing the LaTeX files.
            `file_name (str)`: The file name.
            `mode (str)`: The mode.

        Returns:
            `str`: The combined axis content as a string.

        The function iterates over the tools and reads the LaTeX content from the corresponding files. It then extracts the axis content using the extract_axis_content function and appends it to a list. If the axis content is not empty, it is added to the list. Finally, the function returns the combined axis content as a string.
        """
        tools = (
            self.config['environmentsToCombine'] if self.config else self.environments
        )
        axis_contents = []
        for tool in tools:
            file_path = directory_path / tool / mode / f'{file_name}.tex'
            if file_path.exists():
                logging.info(f"Processing file: {file_path}")
                latex_content = self._BaseTableAndPlotGenerator__read_latex_content(
                    file_path
                )
                axis_content = self._BaseTableAndPlotGenerator__extract_axis_content(
                    latex_content, tool, tools
                )
                axis_contents.append(axis_content)
            else:
                logging.warning(f"Expected file not found: {file_path}")

        if axis_contents:
            return '\n\n'.join(axis_contents) + '\n\n'
        return ''

    def __combine_files_for_comparison(
        self, directory_path: Path, compile_file_alone: bool
    ) -> None:
        """
        This function combines the axis content from different LaTeX files for comparison.

        Args:
            `directory_path (Path)`: A Path object representing the directory containing the LaTeX files.
            `compile_file_alone (bool)`: A boolean indicating whether to compile the LaTeX files to PDFs alone.

        The function first creates a directory for the combined LaTeX files if it doesn't exist. It then iterates over the modes and calls the combine_files function for each mode. The combined axis content is written to a new LaTeX file. If the compile_file_alone flag is set, the function compiles the LaTeX files to PDFs using the compile_latex_to_pdf function.
        """
        graph_types = [
            'binary_tree',
            'complete',
            'cycle',
            'cycle_with_shortcuts',
            'max_acyclic',
            'multi_path',
            'path',
            'grid',
            'reverse_binary_tree',
            'star',
            'w',
            'x',
            'y',
        ]
        modes = ['left_recursion', 'right_recursion', 'double_recursion']

        for mode in modes:
            mode_dir = directory_path / 'combined' / mode
            mode_dir.mkdir(parents=True, exist_ok=True)

            if compile_file_alone:
                self._BaseTableAndPlotGenerator__compile_latex_to_pdf(mode_dir)

            else:

                for graph_type in graph_types:
                    combined_content = self.__combine_files(
                        directory_path, graph_type, mode
                    )
                    final_content = f"""
\\documentclass[border=10pt, 12pt]{{standalone}}
\\usepackage[svgnames]{{xcolor}}
\\usepackage{{amsmath}}
\\usepackage{{pgfplots}}
\\pgfplotsset{{compat=newest}}
\\usepackage[sfdefault]{{FiraSans}}
\\usepackage{{FiraMono}}
\\renewcommand*\\familydefault{{\\sfdefault}}
\\begin{{document}}
\\begin{{tikzpicture}}
                        {combined_content}
\\node[anchor=south, draw, fill=white] at (rel axis cs:0.42,1) {{\\Huge L-R: {', '.join(map(str, self.environments))}}};
\\end{{tikzpicture}}
\\end{{document}}
                    """
                    output_file_path = mode_dir / f'{graph_type}.tex'
                    output_file_path.write_text(final_content)

                    self._BaseTableAndPlotGenerator__format_latex_file(output_file_path)

                    logging.info(
                        f"Combined LaTeX for {graph_type} in {mode} saved to {output_file_path}"
                    )

                self._BaseTableAndPlotGenerator__compile_latex_to_pdf(mode_dir)

    def __generate_csv_from_json(self):
        # Organize data
        organized_data = {'left_recursion': {}, 'right_recursion': {}}

        for key, value in self.data.items():
            environment, graph_type, recursion_variant = key
            if recursion_variant not in organized_data:
                continue

            if graph_type not in organized_data[recursion_variant]:
                organized_data[recursion_variant][graph_type] = {
                    'cpu_times': {},
                    'real_times': {},
                    'environments': set(),
                }

            for size, metrics in value:
                if 'Query' in metrics:
                    query_metrics = metrics.get('Querying')
                    if query_metrics:
                        real_time, cpu_time = query_metrics
                        if (
                            size
                            not in organized_data[recursion_variant][graph_type][
                                'cpu_times'
                            ]
                        ):
                            organized_data[recursion_variant][graph_type]['cpu_times'][
                                size
                            ] = {}
                            organized_data[recursion_variant][graph_type]['real_times'][
                                size
                            ] = {}

                        organized_data[recursion_variant][graph_type]['cpu_times'][size][
                            environment
                        ] = cpu_time
                        organized_data[recursion_variant][graph_type]['real_times'][size][
                            environment
                        ] = real_time
                        organized_data[recursion_variant][graph_type]['environments'].add(
                            environment
                        )

        # Generate CSV files
        output_dir = self.latex_file_dir / 'CSVs'
        output_dir.mkdir(parents=True, exist_ok=True)

        for recursion_variant, graph_types in organized_data.items():
            for graph_type, times in graph_types.items():
                graph_dir = output_dir / recursion_variant / graph_type
                graph_dir.mkdir(parents=True, exist_ok=True)

                sorted_sizes = sorted(times['cpu_times'].keys())
                environments = sorted(times['environments'])

                # CPU Times CSV
                cpu_csv_path = graph_dir / 'cpu_times.csv'
                with open(cpu_csv_path, 'a', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(['environment'] + sorted_sizes)
                    for env in environments:
                        row = [env] + [
                            times['cpu_times'][size].get(env, '')
                            for size in sorted_sizes
                        ]
                        writer.writerow(row)

                # Real Times CSV
                real_csv_path = graph_dir / 'real_times.csv'
                with open(real_csv_path, 'a', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(['environment'] + sorted_sizes)
                    for env in environments:
                        row = [env] + [
                            times['real_times'][size].get(env, '')
                            for size in sorted_sizes
                        ]
                        writer.writerow(row)

    def generate_plot_table(self, compile_file_alone: bool) -> None:
        """
        This function generates plot tables for the timing data.

        Args:
            `compile_file_alone (bool)`: A boolean indicating whether to compile the LaTeX files to PDFs alone.

        The function first collects the timing data using the collect_data function. It then iterates over the environments and generates LaTeX files for each environment using the generate_latex_for_environment function. If the environment is not 'alda', it generates LaTeX comparison charts using the generate_latex_comparison_charts function. Finally, it generates LaTeX comparison tables using the generate_latex_comparison_tables function and combines the files for comparison using the combine_files_for_comparison function.
        """
        self._BaseTableAndPlotGenerator__collect_data()
        self.__generate_csv_from_json()
        # environments = set(key[0] for key in self.data)
        # for env_name in environments:
        #     self.__generate_latex_for_environment(
        #         self.latex_file_dir, env_name, compile_file_alone
        #     )
        #     if env_name != 'alda':
        #         self.__generate_latex_comparison_charts(
        #             self.latex_file_dir, env_name, compile_file_alone
        #         )
        # self.__generate_latex_comparison_tables(self.latex_file_dir)
        # self.__combine_files_for_comparison(
        #     self.latex_file_dir / 'comparison' / 'charts', compile_file_alone
        # )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--config', type=str, required=False, help='JSON string of the config'
    )
    parser.add_argument(
        '--environments',
        nargs='+',
        default=[
            'xsb',
            'postgres',
            'mariadb',
            'duckdb',
            # 'mongodb',
            'neo4j',
            'cockroachdb',
        ],
        help='Specify the list of environments to combine',
    )
    # Compile the LaTeX files to PDFs alone
    parser.add_argument(
        '--compile-latex',
        action='store_true',
        help='Compile the LaTeX files to PDFs alone',
    )
    args = parser.parse_args()

    config = json.loads(args.config if args.config else '{}')

    timing_base_dir = Path('timing')
    pattern = r'^timing_(.*?)_graph_(\d+)\.csv$'
    latex_file_dir = Path('output')
    latex_file_dir.mkdir(exist_ok=True)

    table_plot_generator = TableAndPlotGenerator(
        timing_base_dir, pattern, latex_file_dir, config, args.environments
    )
    table_plot_generator.generate_plot_table(args.compile_latex)


if __name__ == '__main__':
    main()
