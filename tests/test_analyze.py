import os
import ast
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest
import pandas as pd

import analyze

def test_load_data(tmp_path):
    f = tmp_path / "data.txt"
    f.write_text("{('env', 'graph', 'rec'): [(100, {'metric': (1.0, 2.0)})]}")
    data = analyze.load_data(f)
    assert ('env', 'graph', 'rec') in data

def test_extract_records():
    data = {
        ('neo4j', 'graph1', 'left_recursion'): [(10, {'WriteRes': (1.0, 1.0)})],
        ('neo4j', 'graph2', 'right_recursion'): [(10, {'WriteRes': (1.0, 1.0)})],
        ('postgres', 'graph3', 'left_recursion'): [(10, {'Query1': (2.0, 2.0)})],
        ('postgres', 'star', 'left_recursion'): [(10, {'Query1': (2.0, 2.0)})],
    }
    records = analyze.extract_records(data, [10])
    assert len(records) == 2
    assert records[0]['environment'] == 'neo4j'
    assert records[1]['environment'] == 'postgres'

def test_process_data():
    records = [
        {'environment': 'e1', 'graph_type': 'g1', 'recursion_variant': 'r1', 'metric_name': 'm1', 'size': 10, 'real_time': 1, 'cpu_time': 1},
        {'environment': 'e1', 'graph_type': 'g1', 'recursion_variant': 'r1', 'metric_name': 'm1', 'size': 10, 'real_time': 2, 'cpu_time': 2},
    ]
    df = analyze.process_data(records)
    assert len(df) == 1

def test_analyze_data():
    df = pd.DataFrame([
        {'environment': 'e1', 'graph_type': 'g1', 'recursion_variant': 'r1', 'metric_name': 'm1', 'size': 10, 'real_time': 2, 'cpu_time': 1},
        {'environment': 'e2', 'graph_type': 'g1', 'recursion_variant': 'r1', 'metric_name': 'm1', 'size': 10, 'real_time': 1, 'cpu_time': 2},
    ])
    res = analyze.analyze_data(df)
    assert ('g1', 'r1') in res
    assert res[('g1', 'r1')]['sorted_by_real_time']['environment'].iloc[0] == 'e2'

def test_calculate_factors():
    res = {
        ('g1', 'r1'): {
            'sorted_by_real_time': pd.DataFrame({'environment': ['e1', 'e2'], 'real_time': [1.0, 2.0], 'size': [10, 10]}),
            'sorted_by_cpu_time': pd.DataFrame({'environment': ['e1', 'e2'], 'cpu_time': [2.0, 4.0], 'size': [10, 10]}),
        }
    }
    factors = analyze.calculate_factors(res)
    assert factors[('g1', 'real_time')]['r1']['factor'].iloc[1] == 200.0

@patch('pandas.DataFrame.to_csv')
def test_export_to_csv(mock_csv):
    factors = {
        ('g1', 'real_time'): {
            'r1': pd.DataFrame()
        }
    }
    analyze.export_to_csv(factors)
    mock_csv.assert_called_once()

def test_get_short_graph_name():
    assert analyze.get_short_graph_name('complete', 10) == 'Cmpl_{n=10}'
    assert analyze.get_short_graph_name('binary_tree', 8) == 'BinTree_{h=3}'
    assert analyze.get_short_graph_name('unknown', 10) == 'Unknown_unknown_{n=10}'

@patch('pandas.DataFrame.to_csv')
@patch('analyze.create_latex_table')
def test_create_overall_csvs(mock_latex, mock_csv):
    unique_result = {
        ('complete', 'left_recursion'): {
            'sorted_by_real_time': pd.DataFrame({'environment': ['xsb'], 'real_time': [1.0], 'size': [10]}),
            'sorted_by_cpu_time': pd.DataFrame({'environment': ['xsb'], 'cpu_time': [1.0], 'size': [10]}),
        },
        ('complete', 'right_recursion'): {
            'sorted_by_real_time': pd.DataFrame({'environment': [], 'real_time': [], 'size': []}),
            'sorted_by_cpu_time': pd.DataFrame({'environment': [], 'cpu_time': [], 'size': []})
        }
    }
    analyze.create_overall_csvs(unique_result, 10)
    mock_csv.assert_called()
    mock_latex.assert_called()

    # test recursion variant not in overall_data
    unique_result[('complete', 'unknown_recursion')] = {
        'sorted_by_real_time': pd.DataFrame({'environment': ['xsb'], 'real_time': [1.0], 'size': [10]}),
        'sorted_by_cpu_time': pd.DataFrame({'environment': ['xsb'], 'cpu_time': [1.0], 'size': [10]}),
    }
    analyze.create_overall_csvs(unique_result, 10)

@patch('subprocess.run')
def test_is_latexindent_installed(mock_run):
    mock_run.return_value = MagicMock()
    assert analyze.is_latexindent_installed() is True

    import subprocess
    mock_run.side_effect = subprocess.CalledProcessError(1, 'cmd')
    assert analyze.is_latexindent_installed() is False

@patch('analyze.is_latexindent_installed', return_value=True)
@patch('subprocess.run')
def test_format_latex_file(mock_run, mock_is_installed, tmp_path):
    analyze.format_latex_file(tmp_path / "test.tex")
    mock_run.assert_called_once()
    
    import subprocess
    mock_run.side_effect = subprocess.CalledProcessError(1, 'cmd')
    analyze.format_latex_file(tmp_path / "test.tex")

@patch('analyze.is_latexindent_installed', return_value=False)
@patch('subprocess.run')
def test_format_latex_file_not_installed(mock_run, mock_is_installed, tmp_path):
    analyze.format_latex_file(tmp_path / "test.tex")
    mock_run.assert_not_called()

@patch('subprocess.run')
def test_find_latex_distribution(mock_run):
    mock_run.return_value = MagicMock()
    assert analyze.find_latex_distribution() == 'xelatex'
    
    import subprocess
    mock_run.side_effect = subprocess.CalledProcessError(1, 'cmd')
    assert analyze.find_latex_distribution() is None

@patch('subprocess.run')
def test_compile_file(mock_run, tmp_path):
    analyze.compile_file(tmp_path / "test.tex", "xelatex", tmp_path)
    mock_run.assert_called_with(['xelatex', '--shell-escape', 'test.tex'], cwd=tmp_path, check=True)
    
    analyze.compile_file(tmp_path / "test.tex", "pdflatex", tmp_path)
    
    import subprocess
    mock_run.side_effect = subprocess.CalledProcessError(1, 'cmd')
    analyze.compile_file(tmp_path / "test.tex", "pdflatex", tmp_path)

@patch('analyze.find_latex_distribution', return_value="xelatex")
@patch('analyze.compile_file')
def test_compile_latex_to_pdf(mock_compile, mock_find, tmp_path):
    (tmp_path / "test.tex").touch()
    (tmp_path / "test.aux").touch()
    analyze.compile_latex_to_pdf(tmp_path)
    mock_compile.assert_called_once()
    assert not (tmp_path / "test.aux").exists()
    
    mock_find.return_value = None
    analyze.compile_latex_to_pdf(tmp_path)

def test_create_latex_table(tmp_path):
    df = pd.DataFrame({'col_name': ['val_1_2']})
    f = tmp_path / "test.tex"
    analyze.create_latex_table(df, f)
    # val_1_2 -> \text{val}_\text{1}_2 -> $\text{val}_\text{1}_2$
    assert '$\\text{val}_\\text{1}_2$' in f.read_text()

def test_transform_text():
    assert analyze.transform_text('cpu_time') == 'CPU time'
    assert analyze.transform_text('real_time') == 'Elapsed time'
    assert analyze.transform_text('other_time') == 'Other Time'

def test_generate_pgfplots():
    df = pd.DataFrame({'environment': ['xsb', 'mariadb'], 'size': [10, 10], 'real_time': [1.0, 2.0], 'cpu_time': [1.0, 2.0]})
    res = analyze.generate_pgfplots(df, 'complete', 'left_recursion', 'real_time', 10.0)
    assert 'XSB' in res
    assert 'MariaDB' not in res
    
    res = analyze.generate_pgfplots(df, 'cycle', 'left_recursion', 'cpu_time', 10.0)
    assert 'XSB' in res
    assert 'MariaDB' not in res

@patch('analyze.compile_latex_to_pdf')
def test_create_overall_latex_plots(mock_compile):
    unique_result = {
        ('complete', 'left_recursion'): {
            'sorted_by_real_time': pd.DataFrame({'environment': ['unknown'], 'real_time': [1.0], 'size': [10]}),
            'sorted_by_cpu_time': pd.DataFrame({'environment': ['unknown'], 'cpu_time': [1.0], 'size': [10]}),
        }
    }
    analyze.create_overall_latex_plots(unique_result, [10])
    mock_compile.assert_called()

@patch('analyze.load_data')
@patch('analyze.create_overall_latex_plots')
@patch('analyze.create_overall_csvs')
@patch('analyze.export_to_csv')
def test_run_main(mock_export, mock_csvs, mock_plots, mock_load):
    mock_load.return_value = {
        ('neo4j', 'complete', 'left_recursion'): [(10000, {'WriteRes': (1.0, 1.0)})]
    }
    analyze.run_main()
    mock_export.assert_called()
    mock_plots.assert_called()
