import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import generate_plot_table
from generate_plot_table import TableAndPlotGenerator


@patch('subprocess.run')
def test_latexindent_check(mock_run):
    mock_run.return_value.returncode = 0
    assert TableAndPlotGenerator._BaseTableAndPlotGenerator__is_latexindent_installed() is True
    
    mock_run.side_effect = FileNotFoundError
    assert TableAndPlotGenerator._BaseTableAndPlotGenerator__is_latexindent_installed() is False

def test_process_functions():
    last_line = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']
    gen = TableAndPlotGenerator(Path('t'), '', Path('o'))
    
    clingo = gen._BaseTableAndPlotGenerator__process_clingo_data(last_line)
    assert clingo['LoadRules'] == (1.0, 2.0)
    
    xsb = gen._BaseTableAndPlotGenerator__process_xsb_data(last_line)
    assert xsb['LoadRules'] == (1.0, 2.0)
    
    souffle = gen._BaseTableAndPlotGenerator__process_souffle_data(last_line)
    assert souffle['DatalogToCpp'] == (1.0, 2.0)
    
    sql = gen._BaseTableAndPlotGenerator__process_sql_data(last_line)
    assert sql['CreateTable'] == (1.0, 2.0)
    
    mongo = gen._BaseTableAndPlotGenerator__process_mongo_data(last_line)
    assert mongo['CreateTable'] == (1.0, 2.0)
    
    neo4j = gen._BaseTableAndPlotGenerator__process_neo4j_data(last_line)
    assert neo4j['DeleteData'] == (1.0, 2.0)
    
    alda = gen._BaseTableAndPlotGenerator__process_alda_data(last_line)
    assert alda['Overall'] == (1.0, 2.0)

@patch('pathlib.Path.glob')
@patch('builtins.open')
def test_collect_data(mock_open, mock_glob):
    gen = TableAndPlotGenerator(Path('t'), r'^(.*?)_graph_(\d+)\.csv$', Path('o'))
    
    m1 = MagicMock()
    m1.name = "left_recursion_graph_100.csv"
    m1.parts = ["timing", "domain", "clingo", "complete", m1.name]
    
    m2 = MagicMock()
    m2.name = "left_recursion_graph_100.csv"
    m2.parts = ["timing", "domain", "postgres", "complete", m2.name]
    
    mock_glob.return_value = [m1, m2]
    
    mock_file = MagicMock()
    mock_file.readlines.return_value = ["a,b\n", "0,1,2,3,4,5,6,7,8,9,10,11,12\n"]
    mock_open.return_value.__enter__.return_value = mock_file
    
    gen.collect_data()
    
    assert ('domain', 'clingo', 'complete', 'left_recursion') in gen.data
    assert ('domain', 'postgres', 'complete', 'left_recursion') in gen.data

@patch('pathlib.Path.unlink')
@patch('pathlib.Path.iterdir')
@patch('subprocess.run')
@patch('generate_plot_table.TableAndPlotGenerator.collect_data')
@patch('pathlib.Path.mkdir')
@patch('pathlib.Path.write_text')
@patch('pathlib.Path.exists')
@patch('builtins.open')
def test_generate_plot_table_main(mock_open, mock_exists, mock_write, mock_mkdir, mock_collect, mock_run, mock_iterdir, mock_unlink):
    mock_iterdir.return_value = [Path("test.tex"), Path("test.pdf"), Path("test.log")]
    
    gen = TableAndPlotGenerator(Path('t'), '', Path('o'))
    gen.max_x = 400
    gen.data = {
        ('domain', 'xsb', 'complete', 'left_recursion'): [(100, {'Querying': (1.0, 1.0), 'LoadRules': (1.0, 1.0), 'LoadFacts': (1.0, 1.0), 'Writing': (1.0, 1.0)})],
        ('domain', 'clingo', 'complete', 'left_recursion'): [(100, {'Querying': (1.0, 1.0), 'LoadRules': (1.0, 1.0), 'LoadFacts': (1.0, 1.0), 'Writing': (1.0, 1.0), 'Ground': (1.0, 1.0)})]
    }
    
    # Let exists return True to hit processing block in combine files
    mock_exists.return_value = True
    
    mock_open.return_value.__enter__.return_value.read.return_value = "\\begin{axis}test\\end{axis}"
    
    gen.generate_plot_table(False)
    gen.generate_plot_table(True)
    
    # Hit missing lines
    gen._TableAndPlotGenerator__generate_latex_for_environment(Path('o'), 'xsb', False)
    gen._TableAndPlotGenerator__generate_latex_for_environment(Path('o'), 'xsb', True)
    gen._TableAndPlotGenerator__generate_latex_comparison_tables(Path('o'))
    gen._TableAndPlotGenerator__combine_files_for_comparison(Path('o'), True)
    gen._TableAndPlotGenerator__combine_files_for_comparison(Path('o'), False)
    
@patch('argparse.ArgumentParser.parse_args')
@patch('generate_plot_table.TableAndPlotGenerator.generate_plot_table')
def test_main(mock_gen, mock_args, tmp_path):
    args = MagicMock()
    args.config = None
    args.environments = ['xsb']
    args.max_x_axis = 400
    args.compile_latex = False
    args.timing_base_dir = "timing"
    args.exclude_modes = []
    mock_args.return_value = args
    
    generate_plot_table.main()
    mock_gen.assert_called_once()
