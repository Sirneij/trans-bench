import ast
import math
import re
from pathlib import Path
from typing import Any, List

import pandas as pd


def load_data(file_path: str) -> Any:
    with open(file_path, 'r') as file:
        data = ast.literal_eval(file.read())
    return data


def extract_records(data: Any, sizes_to_analyze: List[int]) -> List[dict]:
    records = []
    for (environment, graph_type, recursion_variant), entries in data.items():
        for entry in entries:
            size, metrics = entry
            if size in sizes_to_analyze:
                for metric_name, (real_time, cpu_time) in metrics.items():
                    if 'Query' in metric_name:
                        records.append(
                            {
                                'environment': environment,
                                'graph_type': graph_type,
                                'recursion_variant': recursion_variant,
                                'size': size,
                                'metric_name': metric_name,
                                'real_time': real_time,
                                'cpu_time': cpu_time,
                            }
                        )
    return records


def process_data(records: List[dict]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    unique_df = df.drop_duplicates(
        subset=['environment', 'graph_type', 'recursion_variant', 'metric_name', 'size']
    )
    return unique_df


def analyze_data(unique_df: pd.DataFrame) -> dict:
    unique_result = {}
    grouped_unique = unique_df.groupby(['graph_type', 'recursion_variant'])

    for name, group in grouped_unique:
        graph_type, recursion_variant = name
        group_sorted_by_real_time = group.sort_values(by='real_time')
        group_sorted_by_cpu_time = group.sort_values(by='cpu_time')
        unique_result[name] = {
            'sorted_by_real_time': group_sorted_by_real_time[
                ['environment', 'real_time', 'size']
            ].reset_index(drop=True),
            'sorted_by_cpu_time': group_sorted_by_cpu_time[
                ['environment', 'cpu_time', 'size']
            ].reset_index(drop=True),
        }

    return unique_result


def calculate_factors(unique_result: dict) -> dict:
    final_tables = {}

    for key in unique_result:
        graph_type, recursion_variant = key
        sorted_by_real_time = unique_result[key]['sorted_by_real_time']
        sorted_by_cpu_time = unique_result[key]['sorted_by_cpu_time']

        factors_real_time = [None] + (
            (
                sorted_by_real_time['real_time'].iloc[1:].values
                / sorted_by_real_time['real_time'].iloc[:-1].values
            )
            * 100
        ).tolist()
        factors_cpu_time = [None] + (
            (
                sorted_by_cpu_time['cpu_time'].iloc[1:].values
                / sorted_by_cpu_time['cpu_time'].iloc[:-1].values
            )
            * 100
        ).tolist()

        real_time_table = sorted_by_real_time.copy()
        real_time_table['position'] = range(1, len(real_time_table) + 1)
        real_time_table['factor'] = factors_real_time

        cpu_time_table = sorted_by_cpu_time.copy()
        cpu_time_table['position'] = range(1, len(cpu_time_table) + 1)
        cpu_time_table['factor'] = factors_cpu_time

        if (graph_type, 'real_time') not in final_tables:
            final_tables[(graph_type, 'real_time')] = {}
        if (graph_type, 'cpu_time') not in final_tables:
            final_tables[(graph_type, 'cpu_time')] = {}

        final_tables[(graph_type, 'real_time')][recursion_variant] = real_time_table
        final_tables[(graph_type, 'cpu_time')][recursion_variant] = cpu_time_table

    return final_tables


def export_to_csv(final_tables: dict) -> None:
    for key, tables in final_tables.items():
        graph_type, time_type = key

        for recursion_variant, table in tables.items():
            variant_dir = Path(f"analysis/{graph_type}/{recursion_variant}")
            variant_dir.mkdir(parents=True, exist_ok=True)

            file_path = variant_dir / f"{time_type}_times.csv"
            table.to_csv(file_path, index=False, mode='w', header=True)


def get_short_graph_name(graph_type: str, size: int) -> str:
    graph_names = {
        'complete': f'Cmpl_{{n={size}}}',
        'max_acyclic': f'MaxAcyc_{{n={size}}}',
        'cycle': f'Cyc_{{n={size}}}',
        'cycle_with_shortcuts': f'CycExtra_{{n={size},k=10}}',
        'path': f'Path_{{n={size}}}',
        'multi_path': f'PathDisj_{{n={size},k=10}}',
        'grid': f'Grid_{{n={size}}}',
        'star': f'Star_{{n={size}}}',
        'binary_tree': f'BinTree_{{h={math.log2(size):.0f}}}',
        'reverse_binary_tree': f'BinTreeRev_{{h={math.log2(size):.0f}}}',
        'x': f'X_{{n={size}, k=10}}',
        'y': f'Y_{{n={size},k=10}}',
        'w': f'W_{{n={size},k=10}}',
    }
    return graph_names.get(graph_type, f'Unknown_{graph_type}_{{n={size}}}')


def create_overall_csvs(unique_result: dict, size: int):
    overall_data = {
        'left_recursion': {'real_time': [], 'cpu_time': []},
        'right_recursion': {'real_time': [], 'cpu_time': []},
    }

    graph_types = list(unique_result.keys())

    environments = sorted(
        {
            env
            for results in unique_result.values()
            for table in results.values()
            for env in table['environment'].unique()
        }
    )

    for (graph_type, recursion_variant), results in unique_result.items():
        short_name = get_short_graph_name(graph_type, size)

        sorted_by_real_time = results['sorted_by_real_time']
        sorted_by_cpu_time = results['sorted_by_cpu_time']

        row_real_time = [short_name] + [None] * len(environments)
        row_cpu_time = [short_name] + [None] * len(environments)

        for i, env in enumerate(environments):
            real_time_value = sorted_by_real_time.loc[
                sorted_by_real_time['environment'] == env, 'real_time'
            ]
            cpu_time_value = sorted_by_cpu_time.loc[
                sorted_by_cpu_time['environment'] == env, 'cpu_time'
            ]
            if not real_time_value.empty:
                row_real_time[i + 1] = real_time_value.values[0]
            if not cpu_time_value.empty:
                row_cpu_time[i + 1] = cpu_time_value.values[0]

        positions_real_time = '-'.join(
            str(
                sorted_by_real_time.index[sorted_by_real_time['environment'] == env][0]
                + 1
            )
            for env in environments
            if not sorted_by_real_time.loc[
                sorted_by_real_time['environment'] == env, 'real_time'
            ].empty
        )
        positions_cpu_time = '-'.join(
            str(
                sorted_by_cpu_time.index[sorted_by_cpu_time['environment'] == env][0]
                + 1
            )
            for env in environments
            if not sorted_by_cpu_time.loc[
                sorted_by_cpu_time['environment'] == env, 'cpu_time'
            ].empty
        )

        row_real_time.append(positions_real_time)
        row_cpu_time.append(positions_cpu_time)

        overall_data[recursion_variant]['real_time'].append(row_real_time)
        overall_data[recursion_variant]['cpu_time'].append(row_cpu_time)

    columns = ['graph\_type'] + environments + ['positions']

    captions = {
        'left_recursion': {
            'real_time': "Performance comparison of different environments based on real time for various graph types using left recursion. Each row represents the time taken by different environments to process the graph type. The \\texttt{positions} column indicates the ranking of environments based on their performance.",
            'cpu_time': "CPU time analysis of different environments for various graph types using left recursion. This table highlights the CPU time taken by each environment to execute queries on the graph type. The \\texttt{positions} column shows the ranking of environments based on their CPU time performance.",
        },
        'right_recursion': {
            'real_time': "Real time performance comparison of different environments for various graph types using right recursion. This table shows the real time taken by each environment to process the graph type. The \\texttt{positions} column indicates the ranking of environments based on their performance.",
            'cpu_time': "CPU time performance of different environments for various graph types using right recursion. This table displays the CPU time required by each environment to execute queries on the graph type. The \\texttt{positions} column shows the ranking of environments based on their CPU time performance.",
        },
    }

    for recursion_variant in overall_data:
        df_real_time = pd.DataFrame(
            overall_data[recursion_variant]['real_time'], columns=columns
        )
        df_cpu_time = pd.DataFrame(
            overall_data[recursion_variant]['cpu_time'], columns=columns
        )

        overall_dir = Path(f'analysis/overall/{recursion_variant}')
        overall_dir.mkdir(parents=True, exist_ok=True)

        real_time_csv_path = overall_dir / 'real_times.csv'
        cpu_time_csv_path = overall_dir / 'cpu_times.csv'

        df_real_time.to_csv(real_time_csv_path, index=False)
        df_cpu_time.to_csv(cpu_time_csv_path, index=False)

        create_latex_table(
            df_real_time,
            overall_dir / 'real_times.tex',
            captions[recursion_variant]['real_time'],
        )
        create_latex_table(
            df_cpu_time,
            overall_dir / 'cpu_times.tex',
            captions[recursion_variant]['cpu_time'],
        )


def create_latex_table(df: pd.DataFrame, file_path: Path, caption: str = ''):
    def format_cell(cell):
        # Use regular expression to find sequences before '_'
        formatted_cell = re.sub(r'([^_]+)(?=_)', r'\\text{\1}', cell)
        # Wrap the entire cell content with '$$'
        return f'${formatted_cell}$'

    # Apply the formatting function to the first column
    df[df.columns[0]] = df[df.columns[0]].apply(format_cell)

    # Generate the LaTeX table string
    latex_string = df.to_latex(index=False, caption=caption, label='table:results')

    # Write the LaTeX string to the file
    with open(file_path, 'w') as f:
        f.write(latex_string)


def main(file_path: str, sizes_to_analyze: List[int]):
    data = load_data(file_path)
    records = extract_records(data, sizes_to_analyze)
    unique_df = process_data(records)
    unique_result = analyze_data(unique_df)
    final_tables = calculate_factors(unique_result)
    export_to_csv(final_tables)
    for size in sizes_to_analyze:
        create_overall_csvs(unique_result, size)


if __name__ == '__main__':
    file_path = 'data.txt'
    sizes_to_analyze = [400]
    main(file_path, sizes_to_analyze)
