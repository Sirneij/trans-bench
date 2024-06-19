import math
import unittest
from unittest.mock import MagicMock, patch

from analyze import (
    analyze_data,
    calculate_factors,
    create_overall_csvs,
    export_to_csv,
    extract_records,
    get_short_graph_name,
    load_data,
    process_data,
)
from tests import BaseTest


class TestAnalysisScript(BaseTest):

    def setUp(self):
        super().setUp()

        self.test_data = {
            ('xsb', 'graph1', 'left_recursion'): [
                (200, {'QueryTime': (0.1, 0.05), 'LoadRules': (0.233, 0.199)}),
                (400, {'QueryTime': (0.2, 0.1), 'LoadRules': (0.4, 0.366)}),
            ],
            ('postgres', 'graph1', 'left_recursion'): [
                (200, {'ExecuteQuery': (0.15, 0.07)}),
                (400, {'ExecuteQuery': (0.25, 0.12)}),
            ],
            ('xsb', 'graph1', 'right_recursion'): [
                (200, {'QueryTime': (0.05, 0.02)}),
                (400, {'QueryTime': (0.1, 0.04)}),
            ],
            ('postgres', 'graph1', 'right_recursion'): [
                (200, {'ExecuteQuery': (0.07, 0.03)}),
                (400, {'ExecuteQuery': (0.15, 0.06)}),
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
                'environment': 'xsb',
                'graph_type': 'graph1',
                'recursion_variant': 'left_recursion',
                'size': 200,
                'metric_name': 'QueryTime',
                'real_time': 0.1,
                'cpu_time': 0.05,
            },
            {
                'environment': 'xsb',
                'graph_type': 'graph1',
                'recursion_variant': 'left_recursion',
                'size': 400,
                'metric_name': 'QueryTime',
                'real_time': 0.2,
                'cpu_time': 0.1,
            },
            {
                'environment': 'postgres',
                'graph_type': 'graph1',
                'recursion_variant': 'left_recursion',
                'size': 200,
                'metric_name': 'ExecuteQuery',
                'real_time': 0.15,
                'cpu_time': 0.07,
            },
            {
                'environment': 'postgres',
                'graph_type': 'graph1',
                'recursion_variant': 'left_recursion',
                'size': 400,
                'metric_name': 'ExecuteQuery',
                'real_time': 0.25,
                'cpu_time': 0.12,
            },
            {
                'environment': 'xsb',
                'graph_type': 'graph1',
                'recursion_variant': 'right_recursion',
                'size': 200,
                'metric_name': 'QueryTime',
                'real_time': 0.05,
                'cpu_time': 0.02,
            },
            {
                'environment': 'xsb',
                'graph_type': 'graph1',
                'recursion_variant': 'right_recursion',
                'size': 400,
                'metric_name': 'QueryTime',
                'real_time': 0.1,
                'cpu_time': 0.04,
            },
            {
                'environment': 'postgres',
                'graph_type': 'graph1',
                'recursion_variant': 'right_recursion',
                'size': 200,
                'metric_name': 'ExecuteQuery',
                'real_time': 0.07,
                'cpu_time': 0.03,
            },
            {
                'environment': 'postgres',
                'graph_type': 'graph1',
                'recursion_variant': 'right_recursion',
                'size': 400,
                'metric_name': 'ExecuteQuery',
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

    @patch('analyze.Path.mkdir')
    @patch('analyze.Path.exists', MagicMock(return_value=True))
    @patch('analyze.pd.DataFrame.to_csv')
    def test_export_to_csv(self, mock_to_csv, mock_mkdir):
        sizes_to_analyze = [200, 400]
        records = extract_records(self.test_data, sizes_to_analyze)
        unique_df = process_data(records)
        unique_result = analyze_data(unique_df)
        final_tables = calculate_factors(unique_result)
        export_to_csv(final_tables)

        mock_mkdir.assert_called()
        self.assertEqual(mock_to_csv.call_count, len(final_tables) * 2)

    def test_get_short_graph_name(self):
        size = 400
        self.assertEqual(get_short_graph_name('complete', size), f'Cmpl_{{n={size}}}')
        self.assertEqual(
            get_short_graph_name('binary_tree', size),
            f'BinTree_{{h={math.log2(size):.0f}}}',
        )
        self.assertEqual(
            get_short_graph_name('unknown', size), f'Unknown_unknown_{{n={size}}}'
        )

    @patch('analyze.Path.mkdir')
    @patch('analyze.Path.exists', MagicMock(return_value=True))
    @patch('analyze.pd.DataFrame.to_csv')
    def test_create_overall_csvs(self, mock_to_csv, mock_mkdir):
        sizes_to_analyze = [200, 400]
        records = extract_records(self.test_data, sizes_to_analyze)
        unique_df = process_data(records)
        unique_result = analyze_data(unique_df)
        for size in sizes_to_analyze:
            create_overall_csvs(unique_result, size)
        self.assertEqual(mock_mkdir.call_count, 2 * len(sizes_to_analyze))
        self.assertEqual(mock_to_csv.call_count, 4 * len(sizes_to_analyze))


if __name__ == '__main__':
    unittest.main()
