import unittest
from pathlib import Path

import pandas as pd

from analyze import (
    analyze_data,
    calculate_factors,
    export_to_csv,
    extract_records,
    load_data,
    process_data,
)
from tests import BaseTest


class TestAnalysisScript(BaseTest):

    def setUp(self):
        super().setUp()

        self.test_data = {
            ('env1', 'graph1', 'left_recursion'): [
                (200, {'QueryTime': (0.1, 0.05), 'LoadRules': (0.233, 0.199)}),
                (400, {'QueryTime': (0.2, 0.1), 'LoadRules': (0.4, 0.366)}),
            ],
            ('env2', 'graph1', 'left_recursion'): [
                (200, {'QueryTime': (0.15, 0.07)}),
                (400, {'QueryTime': (0.25, 0.12)}),
            ],
            ('env1', 'graph1', 'right_recursion'): [
                (200, {'QueryTime': (0.05, 0.02)}),
                (400, {'QueryTime': (0.1, 0.04)}),
            ],
            ('env2', 'graph1', 'right_recursion'): [
                (200, {'QueryTime': (0.07, 0.03)}),
                (400, {'QueryTime': (0.15, 0.06)}),
            ],
        }

        # Create a sample data file
        self.sample_data_path = self.test_dir / 'data.txt'
        with open(self.sample_data_path, 'w') as f:
            f.write(str(self.test_data))

    def test_load_data(self):
        data = load_data(self.sample_data_path)
        self.assertEqual(data, self.test_data)

    def test_extract_records(self):
        sizes_to_analyze = [200, 400]
        records = extract_records(self.test_data, sizes_to_analyze)
        expected_records = [
            {
                'environment': 'env1',
                'graph_type': 'graph1',
                'recursion_variant': 'left_recursion',
                'size': 200,
                'metric_name': 'QueryTime',
                'real_time': 0.1,
                'cpu_time': 0.05,
            },
            {
                'environment': 'env1',
                'graph_type': 'graph1',
                'recursion_variant': 'left_recursion',
                'size': 400,
                'metric_name': 'QueryTime',
                'real_time': 0.2,
                'cpu_time': 0.1,
            },
            {
                'environment': 'env2',
                'graph_type': 'graph1',
                'recursion_variant': 'left_recursion',
                'size': 200,
                'metric_name': 'QueryTime',
                'real_time': 0.15,
                'cpu_time': 0.07,
            },
            {
                'environment': 'env2',
                'graph_type': 'graph1',
                'recursion_variant': 'left_recursion',
                'size': 400,
                'metric_name': 'QueryTime',
                'real_time': 0.25,
                'cpu_time': 0.12,
            },
            {
                'environment': 'env1',
                'graph_type': 'graph1',
                'recursion_variant': 'right_recursion',
                'size': 200,
                'metric_name': 'QueryTime',
                'real_time': 0.05,
                'cpu_time': 0.02,
            },
            {
                'environment': 'env1',
                'graph_type': 'graph1',
                'recursion_variant': 'right_recursion',
                'size': 400,
                'metric_name': 'QueryTime',
                'real_time': 0.1,
                'cpu_time': 0.04,
            },
            {
                'environment': 'env2',
                'graph_type': 'graph1',
                'recursion_variant': 'right_recursion',
                'size': 200,
                'metric_name': 'QueryTime',
                'real_time': 0.07,
                'cpu_time': 0.03,
            },
            {
                'environment': 'env2',
                'graph_type': 'graph1',
                'recursion_variant': 'right_recursion',
                'size': 400,
                'metric_name': 'QueryTime',
                'real_time': 0.15,
                'cpu_time': 0.06,
            },
        ]
        self.assertEqual(records, expected_records)

    def test_process_data(self):
        sizes_to_analyze = [200, 400]
        records = extract_records(self.test_data, sizes_to_analyze)
        unique_df = process_data(records)
        self.assertEqual(len(unique_df), 8)

    def test_analyze_data(self):
        sizes_to_analyze = [200, 400]
        records = extract_records(self.test_data, sizes_to_analyze)
        unique_df = process_data(records)
        unique_result = analyze_data(unique_df)
        self.assertIn(('graph1', 'left_recursion'), unique_result)
        self.assertIn(('graph1', 'right_recursion'), unique_result)
        self.assertEqual(
            len(unique_result[('graph1', 'left_recursion')]['sorted_by_real_time']), 4
        )
        self.assertEqual(
            len(unique_result[('graph1', 'right_recursion')]['sorted_by_real_time']), 4
        )

    def test_calculate_factors(self):
        sizes_to_analyze = [200, 400]
        records = extract_records(self.test_data, sizes_to_analyze)
        unique_df = process_data(records)
        unique_result = analyze_data(unique_df)
        final_tables = calculate_factors(unique_result)
        for key in final_tables:
            graph_type, time_type = key
            for recursion_variant, table in final_tables[key].items():
                self.assertIn('factor', table.columns)
                self.assertIn('position', table.columns)
                self.assertEqual(len(table), 4)

    def test_export_to_csv(self):
        sizes_to_analyze = [200, 400]
        records = extract_records(self.test_data, sizes_to_analyze)
        unique_df = process_data(records)
        unique_result = analyze_data(unique_df)
        final_tables = calculate_factors(unique_result)
        export_to_csv(final_tables)

        for key in final_tables:
            graph_type, time_type = key
            for recursion_variant in final_tables[key]:
                variant_dir = Path(f"analysis/{graph_type}/{recursion_variant}")
                self.assertTrue(variant_dir.exists())
                file_path = variant_dir / f"{time_type}_times.csv"
                self.assertTrue(file_path.exists())
                df = pd.read_csv(file_path)
                self.assertEqual(len(df), 4)


if __name__ == '__main__':
    unittest.main()
