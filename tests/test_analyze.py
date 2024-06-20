import math
from unittest.mock import MagicMock, patch

from analyze import (
    analyze_data,
    calculate_factors,
    create_overall_csvs,
    export_to_csv,
    extract_records,
    get_short_graph_name,
    load_data,
)
from analyze import main as analyze_main
from analyze import process_data, run_main
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

    def test_export_to_csv(self):
        sizes_to_analyze = [200, 400]
        records = extract_records(self.test_data, sizes_to_analyze)
        unique_df = process_data(records)
        unique_result = analyze_data(unique_df)
        final_tables = calculate_factors(unique_result)

        with patch('analyze.Path.mkdir') as mock_mkdir, patch(
            'analyze.Path.exists', MagicMock(return_value=True)
        ), patch('analyze.pd.DataFrame.to_csv') as mock_to_csv:
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

    def test_create_overall_csvs(self):
        sizes_to_analyze = [200, 400]
        records = extract_records(self.test_data, sizes_to_analyze)
        unique_df = process_data(records)
        unique_result = analyze_data(unique_df)

        with patch('analyze.Path.mkdir') as mock_mkdir, patch(
            'analyze.Path.exists', MagicMock(return_value=True)
        ), patch('analyze.pd.DataFrame.to_csv') as mock_to_csv, patch(
            'analyze.pd.DataFrame.to_latex', return_value='dummy_latex'
        ) as mock_to_latex, patch(
            'builtins.open', new_callable=MagicMock
        ):
            for size in sizes_to_analyze:
                create_overall_csvs(unique_result, size)

            self.assertEqual(mock_mkdir.call_count, 2 * len(sizes_to_analyze))
            self.assertEqual(mock_to_csv.call_count, 4 * len(sizes_to_analyze))
            self.assertEqual(mock_to_latex.call_count, 4 * len(sizes_to_analyze))

    @patch('analyze.create_overall_csvs')
    @patch('analyze.export_to_csv')
    @patch('analyze.calculate_factors')
    @patch('analyze.analyze_data')
    @patch('analyze.process_data')
    @patch('analyze.extract_records')
    @patch('analyze.load_data')
    def test_main(
        self,
        mock_load_data,
        mock_extract_records,
        mock_process_data,
        mock_analyze_data,
        mock_calculate_factors,
        mock_export_to_csv,
        mock_create_overall_csvs,
    ):
        # Setup test data
        mock_data = {'mock': 'data'}
        mock_records = [{'mock': 'record'}]
        mock_df = MagicMock()
        mock_unique_result = {'mock': 'unique_result'}
        mock_final_tables = {'mock': 'final_tables'}

        # Configure mocks
        mock_load_data.return_value = mock_data
        mock_extract_records.return_value = mock_records
        mock_process_data.return_value = mock_df
        mock_analyze_data.return_value = mock_unique_result
        mock_calculate_factors.return_value = mock_final_tables

        # Test parameters
        test_file_path = 'test_data.txt'
        test_sizes_to_analyze = [200, 400]

        # Call the main function
        analyze_main(test_file_path, test_sizes_to_analyze)

        # Verify that all functions were called with the expected arguments
        mock_load_data.assert_called_once_with(test_file_path)
        mock_extract_records.assert_called_once_with(mock_data, test_sizes_to_analyze)
        mock_process_data.assert_called_once_with(mock_records)
        mock_analyze_data.assert_called_once_with(mock_df)
        mock_calculate_factors.assert_called_once_with(mock_unique_result)
        mock_export_to_csv.assert_called_once_with(mock_final_tables)

        # Verify create_overall_csvs called twice (once for each size)
        self.assertEqual(
            mock_create_overall_csvs.call_count, len(test_sizes_to_analyze)
        )
        for size in test_sizes_to_analyze:
            mock_create_overall_csvs.assert_any_call(mock_unique_result, size)

    @patch('analyze.main')
    def test_run_main(self, mock_main):
        # Call the function
        run_main()

        # Verify that main was called with the expected arguments
        mock_main.assert_called_once_with('data.txt', [400])
