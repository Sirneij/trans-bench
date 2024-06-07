import argparse
import logging
import re
import subprocess
from pathlib import Path
import math

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s'
)


class BaseTableAndPlotGenerator:
    def __init__(self, timing_base_dir: Path, pattern: str, latex_file_dir: Path):
        self.timing_base_dir = timing_base_dir
        self.pattern = pattern
        self.latex_file_dir = latex_file_dir
        self.data = None
        self.components = {
            'alda': ['Overall'],
            'xsb': ['LoadRules', 'LoadFacts', 'Querying', 'Writing'],  # Example order
            'clingo': ['LoadRules', 'LoadFacts', 'Ground', 'Querying', 'Writing'],
            'souffle': [
                'DatalogToCpp',
                'CppToO',
                'InstanceLoading',
                'LoadFacts',
                'Querying',
                'Writing',
            ],
        }
        self.component_colors = {
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
        }

        self.component_legend = {
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
        }
        self.graphs_k_values = {
            'cycle_with_shortcuts': 10,
            'multi_path': 10,
            'w': 10,
            'y': 10,
            'x': 'n',
        }

    def __collect_data(self) -> None:
        """
        This function collects timing data from the CSV files in the timing directory.

        The function operates as follows:
        1. It initializes an empty dictionary to store the timing data.
        2. It iterates over the CSV files in the timing directory.
        3. It extracts the environment name, graph type, mode, and graph size from the file path.
        4. It reads the last line of the CSV file and extracts the timing data.
        5. It processes the timing data based on the environment name.
        6. It stores the processed timing data in the dictionary.
        7. If an error occurs during processing, the function logs the error.
        8. After processing all CSV files, the function sets the data attribute to the collected data.
        """
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
                        # Average - 0, LoadRulesRealTime -1,LoadRulesCPUTime -2,LoadFactsRealTime -3,LoadFactsCPUTime -4,GroundRealTime -5,GroundCPUTime -6,QueryRealTime -7,QueryCPUTime -8,WriteRealTime -9,WriteCPUTime -10
                        load_rules_time = (float(last_line[1]), float(last_line[2]))
                        load_facts_time = (float(last_line[3]), float(last_line[4]))
                        ground_time = (float(last_line[5]), float(last_line[6]))
                        query_time = (float(last_line[7]), float(last_line[8]))
                        write_time = (float(last_line[9]), float(last_line[10]))
                        data[key].append(
                            (
                                graph_size,
                                {
                                    'LoadRules': load_rules_time,
                                    'LoadFacts': load_facts_time,
                                    'Ground': ground_time,
                                    'Querying': query_time,
                                    'Writing': write_time,
                                },
                            )
                        )

                    elif env_name == 'xsb':
                        # Average - 0, LoadRulesRealTime -1,LoadRulesCPUTime -2,LoadFactsRealTime -3,LoadFactsCPUTime -4,QueryRealTime -5,QueryCPUTime -6,WriteRealTime -7,WriteCPUTime -8
                        load_rules_time = (float(last_line[1]), float(last_line[2]))
                        load_facts_time = (float(last_line[3]), float(last_line[4]))
                        query_time = (float(last_line[5]), float(last_line[6]))
                        write_time = (float(last_line[7]), float(last_line[8]))
                        data[key].append(
                            (
                                graph_size,
                                {
                                    'LoadRules': load_rules_time,
                                    'LoadFacts': load_facts_time,
                                    'Querying': query_time,
                                    'Writing': write_time,
                                },
                            )
                        )

                    elif env_name == 'souffle':
                        # Souffle has a different format for timing data
                        # Average - 0,DatalogToCPPRealTime - 1,DatalogToCPPCPUTime -2,CompileRealTime -3,CompileCPUTime -4,InstanceLoadingRealTime -5,InstanceLoadingCPUTime -6,LoadingFactRealTime -7,LoadingFactCPUTime -8,QueryRealTime -9,QueryCPUTime -10,WritingResultRealTime -11,WritingResultCPUTime -12
                        datalog_to_cpp = (float(last_line[1]), float(last_line[2]))
                        cpp_to_o = (float(last_line[3]), float(last_line[4]))
                        instance_loading_time = (
                            float(last_line[5]),
                            float(last_line[6]),
                        )
                        facts_loading_time = (float(last_line[7]), float(last_line[8]))
                        running = (float(last_line[9]), float(last_line[10]))
                        writing = (float(last_line[11]), float(last_line[12]))
                        data[key].append(
                            (
                                graph_size,
                                {
                                    'DatalogToCpp': datalog_to_cpp,
                                    'CppToO': cpp_to_o,
                                    'InstanceLoading': instance_loading_time,
                                    'LoadFacts': facts_loading_time,
                                    'Querying': running,
                                    'Writing': writing,
                                },
                            )
                        )

                    elif env_name == 'alda':
                        try:
                            # Average - 0, ElapsedRealTime - 1, ElapsedCPUTime - 2
                            elapsed_time = (float(last_line[1]), float(last_line[2]))
                        except IndexError:
                            elapsed_time = (float(last_line[0]), float(last_line[1]))
                        data[key].append((graph_size, {'Overall': elapsed_time}))

            except Exception as e:
                logging.error(f"Error processing file {csv_file}: {e}")
        self.data = data

    def __find_cpu_time(self, key: tuple, size: int, query_type: str) -> float:
        """
        This function finds the CPU time for a given environment, graph type, mode, size, and query type.

        Args:
            `key (tuple)`: A tuple containing the environment name, graph type, and mode.
            `size (int)`: The graph size.
            `query_type (str)`: The query type.

        Returns:
            `float`: The CPU time for the given environment, graph type, mode, size, and query type. If the CPU time is not found, the function returns None.

        The function operates as follows:
        1. It checks if the key is in the data dictionary. If it is, it proceeds with the following steps.
        2. It iterates over the values for the given key. If the first element of the entry is equal to the size, it returns the CPU time for the given query type.
        3. If the CPU time is not found, the function returns None.
        """
        if key in self.data:
            for entry in self.data[key]:
                if entry[0] == size:
                    return entry[1][query_type][1]
        return None

    def __find_max_cpu_time_across_envs(self, graph_type: str, mode: str) -> float:
        """
        This function finds the maximum CPU time for a given graph type and mode across all environments and sizes.

        Args:
            `graph_type (str)`: The graph type.
            `mode (str)`: The recursion mode.

        Returns:
            `float`: The maximum CPU time.
        """
        max_cpu_time = 0
        for key, entries in self.data.items():
            env_name, g_type, m = key
            if g_type == graph_type and m == mode:
                for entry in entries:
                    cpu_times = [value[1] for value in entry[1].values()]
                    max_cpu_time = max(max_cpu_time, max(cpu_times))
        return max_cpu_time

    def __find_max_real_time(self, env_name: str, graph_type: str, mode: str) -> float:
        """
        Find the maximum real-time value for double_recursion within the same graph_type and environment.

        Args:
            `env_name (str)`: The environment name.
            `graph_type (str)`: The graph type.
            `mode (str)`: The recursion mode (double_recursion).

        Returns:
            `float`: The maximum real-time value.
        """
        max_real_time = 0
        key = (env_name, graph_type, mode)
        if key in self.data:
            for entry in self.data[key]:
                real_times = [value[0] for value in entry[1].values()]
                max_real_time = max(max_real_time, max(real_times))
        return max_real_time

    def __is_latexindent_installed(self) -> bool:
        """
        Check if latexindent is installed.

        Returns:
            bool: True if latexindent is installed, False otherwise.
        """
        try:
            subprocess.run(
                ['latexindent', '--version'],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            return True
        except subprocess.CalledProcessError:
            return False
        except FileNotFoundError:
            return False

    def __format_latex_file(self, file_path: Path) -> None:
        """
        Format the LaTeX file using latexindent if it is installed.

        Args:
            file_path (Path): Path to the LaTeX file to format.
        """
        if self.__is_latexindent_installed():
            try:
                subprocess.run(['latexindent', '-w', str(file_path)], check=True)
                logging.info(f'Formatted {file_path} using latexindent.')
            except subprocess.CalledProcessError as e:
                logging.error(f'Error formatting {file_path} using latexindent: {e}')
        else:
            logging.warning('latexindent is not installed. Skipping formatting.')

    def __list_to_ordered_set(self, input_list: list) -> set:
        """
        This function converts a list to an ordered set.

        Args:
            `input_list (list)`: A list of elements.

        Returns:
            `set`: An ordered set of elements.

        """
        return set(dict.fromkeys(input_list))

    def __compile_latex_to_pdf(self, directory: Path):
        """
        This function compiles LaTeX files to PDFs using the available LaTeX distribution.

        Args:
            `directory (Path)`: A Path object representing the directory containing the LaTeX files.

        The function first checks for the available LaTeX distributions in the system. It then iterates over the files in the directory and compiles the LaTeX files to PDFs using the available LaTeX distribution. If no LaTeX distribution is found, the function logs an error message.
        """
        latex_distributions = ['xelatex', 'pdflatex', 'lualatex']
        latex_distribution = None

        for distribution in latex_distributions:
            try:
                subprocess.run(
                    ['which', distribution], check=True, stdout=subprocess.DEVNULL
                )
                latex_distribution = distribution
                break
            except subprocess.CalledProcessError:
                continue

        if latex_distribution is None:
            logging.error('No LaTeX distribution found on the system.')
            # Return
            return

        for f in directory.iterdir():
            logging.info(f'Compiling {f} to PDF.')
            if f.suffix == '.tex':
                if latex_distribution == 'xelatex':
                    subprocess.run(
                        [latex_distribution, '--shell-escape', f.name],
                        cwd=directory,
                        check=True,
                    )
                else:
                    subprocess.run(
                        [latex_distribution, f.name], cwd=directory, check=True
                    )

        for f in directory.iterdir():
            if f.suffix not in ['.tex', '.pdf']:
                f.unlink()

    def __read_latex_content(self, file_path: Path) -> str:
        """
        This function reads the content of a LaTeX file.

        Args:
            `file_path (Path)`: A Path object representing the path to the LaTeX file.

        Returns:
            `str`: The content of the LaTeX file as a string.

        The function reads the content of the LaTeX file using the 'utf-8' encoding and returns it as a string.
        """
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()

    def __extract_axis_content(self, latex_content: str, tool: str) -> str:
        """
        This function extracts the axis content from a LaTeX file.

        Args:
            `latex_content (str)`: The content of the LaTeX file as a string.
            `tool (str)`: The tool name.

        Returns:
            `str`: The axis content as a string.

        The function uses a regular expression to extract the axis content from the LaTeX file. If the tool is 'xsb', it adds a 'bar shift' option to the axis content. If the tool is 'clingo' or 'souffle', it modifies the axis content to match the expected format for the comparison charts.
        """
        match = re.search(r'\\begin{axis}.*?\\end{axis}', latex_content, re.DOTALL)
        if match:
            axis_content = match.group(0)
            if tool == 'xsb':
                axis_content = re.sub(
                    r'\\begin{axis}\[',
                    r'\\begin{axis}[bar shift=-25pt, ',
                    axis_content,
                    1,
                )
            elif tool in ['clingo', 'souffle']:
                if tool == 'clingo':
                    axis_content = re.sub(
                        r'\\begin{axis}\[',
                        r'\\begin{axis}[bar shift=-3.7pt, ',
                        axis_content,
                        1,
                    )
                elif tool == 'souffle':
                    axis_content = re.sub(
                        r'\\begin{axis}\[',
                        r'\\begin{axis}[bar shift=18pt, ',
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
        return ''

    def __write_latex_header(self, f) -> None:
        """
        Write the LaTeX header to the file.

        Args:
            `file`: The file object to write to.
        """
        f.write('\\documentclass[border=10pt]{standalone}\n')
        f.write('\\usepackage[svgnames]{xcolor}\n')
        f.write('\\usepackage{amsmath}\n')
        f.write('\\usepackage{pgfplots}\n')
        f.write('\\pgfplotsset{compat=newest}\n')
        f.write('\\usepackage[sfdefault]{FiraSans}\n')
        f.write('\\usepackage{FiraMono}\n')
        f.write('\\renewcommand*\\familydefault{\\sfdefault}\n')
        f.write('\\begin{document}\n')

    def __write_latex_footer(self, f) -> None:
        """
        Write the LaTeX footer to the file.

        Args:
            `file`: The file object to write to.
        """
        f.write('\\end{document}\n')

    def __adjust_ymax(self, max_value_func: callable, func_params: tuple) -> float:
        """
        Adjust the maximum value for the y-axis.

        Args:
            `max_value_func (callable)`: A function that returns the maximum value.
            `func_params (tuple)`: The parameters to pass to the function.

        Returns:
            `float`: The adjusted maximum value.
        """
        max_value = max_value_func(*func_params)
        if max_value < 1:
            return max_value + 0.02
        elif 1 <= max_value < 5:
            return max_value + 2
        elif 5 <= max_value < 10:
            return max_value + 3
        elif 10 <= max_value < 100:
            return max_value + 5
        elif 100 <= max_value < 1000:
            return max_value + 50
        elif 1000 <= max_value < 10000:
            return max_value + 500
        elif 10000 <= max_value < 100000:
            return max_value + 5000


class TableAndPlotGenerator(BaseTableAndPlotGenerator):
    def __init__(self, timing_base_dir: Path, pattern: str, latex_file_dir: Path):
        super().__init__(timing_base_dir, pattern, latex_file_dir)

    def __write_latex_body_for_environment(
        self,
        f,
        env_name: str,
        key: tuple,
        values: list,
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
                if key[1] in ['complete', 'cycle', 'max_acyclic', 'grid', 'path', 'star']:
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
                    (env_name, key[1], 'double_recursion'),
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
                        xsb_time = self.__find_cpu_time(
                            ('xsb', graph_type, mode), size, 'Querying'
                        )
                        clingo_time = self.__find_cpu_time(
                            ('clingo', graph_type, mode), size, 'Querying'
                        )
                        souffle_time = self.__find_cpu_time(
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
                    self._BaseTableAndPlotGenerator__find_max_cpu_time_across_envs,
                    (graph_type, 'double_recursion'),
                )
                file_folder = file_dir / mode
                file_folder.mkdir(exist_ok=True, parents=True)
                file_path = file_folder / f'{graph_type}.tex'

                with open(file_path, 'w') as f:
                    self._BaseTableAndPlotGenerator__write_latex_header(f)
                    f.write('\\begin{tikzpicture}\n')
                    f.write('\\begin{axis}[\n')
                    f.write('   ybar stacked,\n')
                    f.write('   width=1.7\\textwidth,\n')
                    f.write('   bar width=0.7cm,\n')
                    f.write('   ymajorgrids, tick align=inside,\n')
                    f.write('   major grid style={draw=gray!20},\n')
                    f.write('   xtick=data,\n')
                    f.write(f'   ymin=0, ymax={ymax},\n')
                    f.write('   axis x line*=bottom,\n')
                    f.write('   axis y line*=left,\n')
                    f.write('   enlarge x limits=0.05,\n')
                    f.write('   legend style={\n')
                    if env_name == 'xsb':
                        f.write('       at={(0.23, 0.97)},\n')
                    elif env_name == 'clingo':
                        f.write('       at={(0.454, 0.97)},\n')
                    elif env_name == 'souffle':
                        f.write('       at={(0.69, 0.97)},\n')
                    f.write('       anchor=north east,\n')
                    f.write('       legend columns=1,\n')
                    f.write('       font=\\Huge,\n')
                    f.write('   },\n')
                    f.write('   ylabel={CPU Time (seconds)},\n')
                    if graph_type in ['complete', 'cycle', 'max_acyclic', 'grid', 'path', 'star']:
                        f.write('   xlabel={Number of nodes},\n')
                    elif graph_type in ['cycle_with_shortcuts', 'w', 'y', 'x', 'multi_path']:
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
                            if graph_type in ['cycle_with_shortcuts', 'w', 'y', 'multi_path', 'x']:
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
        tools = ['xsb', 'clingo', 'souffle']
        axis_contents = []
        for tool in tools:
            file_path = directory_path / tool / mode / f'{file_name}.tex'
            if file_path.exists():
                logging.info(f"Processing file: {file_path}")
                latex_content = self._BaseTableAndPlotGenerator__read_latex_content(
                    file_path
                )
                axis_content = self._BaseTableAndPlotGenerator__extract_axis_content(
                    latex_content, tool
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
\\node[anchor=south, draw, fill=white] at (rel axis cs:0.42,1) {{\\Huge Left: XSB, Middle: Clingo, Right: Soufflé}};
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

    def generate_plot_table(self, compile_file_alone: bool) -> None:
        """
        This function generates plot tables for the timing data.

        Args:
            `compile_file_alone (bool)`: A boolean indicating whether to compile the LaTeX files to PDFs alone.

        The function first collects the timing data using the collect_data function. It then iterates over the environments and generates LaTeX files for each environment using the generate_latex_for_environment function. If the environment is not 'alda', it generates LaTeX comparison charts using the generate_latex_comparison_charts function. Finally, it generates LaTeX comparison tables using the generate_latex_comparison_tables function and combines the files for comparison using the combine_files_for_comparison function.
        """
        self._BaseTableAndPlotGenerator__collect_data()
        environments = set(key[0] for key in self.data)
        for env_name in environments:
            self.__generate_latex_for_environment(
                self.latex_file_dir, env_name, compile_file_alone
            )
            if env_name != 'alda':
                self.__generate_latex_comparison_charts(
                    self.latex_file_dir, env_name, compile_file_alone
                )
        # self.__generate_latex_comparison_tables(self.latex_file_dir, compile_file_alone)
        # self.__generate_comparison_csv_files(self.latex_file_dir, compile_file_alone)
        self.__combine_files_for_comparison(
            self.latex_file_dir / 'comparison' / 'charts', compile_file_alone
        )


def main():
    parser = argparse.ArgumentParser()
    # Compile the LaTeX files to PDFs alone
    parser.add_argument(
        '--compile-latex',
        action='store_true',
        help='Compile the LaTeX files to PDFs alone',
    )
    args = parser.parse_args()

    timing_base_dir = Path('timing')
    pattern = r'^timing_(.*?)_graph_(\d+)\.csv$'
    latex_file_dir = Path('output')
    latex_file_dir.mkdir(exist_ok=True)

    table_plot_generator = TableAndPlotGenerator(
        timing_base_dir, pattern, latex_file_dir
    )
    table_plot_generator.generate_plot_table(args.compile_latex)


if __name__ == '__main__':
    main()
