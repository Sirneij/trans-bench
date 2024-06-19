import ast
from pathlib import Path
from typing import Any

import pandas as pd


def load_data(file_path):
    with open(file_path, 'r') as file:
        data = ast.literal_eval(file.read())
    return data


def extract_records(data: Any, sizes_to_analyze: list[int]) -> list[dict[str, Any]]:
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


def process_data(records: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    unique_df = df.drop_duplicates(
        subset=['environment', 'graph_type', 'recursion_variant', 'metric_name', 'size']
    )
    return unique_df


def analyze_data(unique_df: pd.DataFrame) -> dict[str, Any]:
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


def calculate_factors(unique_result: dict[str, Any]) -> dict[str, Any]:
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


def export_to_csv(final_tables: dict[str, Any]) -> None:
    for key, tables in final_tables.items():
        graph_type, time_type = key

        for recursion_variant, table in tables.items():
            variant_dir = Path(f"analysis/{graph_type}/{recursion_variant}")
            variant_dir.mkdir(parents=True, exist_ok=True)

            file_path = variant_dir / f"{time_type}_times.csv"
            table.to_csv(file_path, index=False, mode='w', header=True)


def main(file_path: str, sizes_to_analyze: list[int]):
    data = load_data(file_path)
    records = extract_records(data, sizes_to_analyze)
    unique_df = process_data(records)
    unique_result = analyze_data(unique_df)
    final_tables = calculate_factors(unique_result)
    export_to_csv(final_tables)


if __name__ == '__main__':
    file_path = 'data.txt' 
    sizes_to_analyze = [400]  
    main(file_path, sizes_to_analyze)
