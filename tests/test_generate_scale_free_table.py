import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

import generate_scale_free_table

def test_get_query_time():
    headers = ['a', 'WriteResultRealTime', 'ExecuteQueryRealTime', 'QueryRealTime']
    values = ['0', '1.0', '2.0', '3.0', '4.0']
    
    assert generate_scale_free_table.get_query_time('neo4j', headers, values) == 2.0
    assert generate_scale_free_table.get_query_time('xsb', headers, values) == 4.0
    assert generate_scale_free_table.get_query_time('postgres', headers, values) == 3.0
    
    assert generate_scale_free_table.get_query_time('neo4j', ['a'], ['1']) is None

@patch('pathlib.Path.rglob')
@patch('pathlib.Path.write_text')
def test_generate(mock_write, mock_rglob, tmp_path):
    # Mocking files
    mock_file1 = MagicMock()
    mock_file1.name = "left_recursion_graph_100.csv"
    mock_file1.parts = ["timing", "domain", "xsb", "scale_free"]
    
    mock_file2 = MagicMock()
    mock_file2.name = "left_recursion_graph_100.csv"
    mock_file2.parts = ["timing", "domain", "postgres", "scale_free"]
    
    mock_file3 = MagicMock()
    mock_file3.name = "invalid.csv"
    
    mock_file4 = MagicMock()
    mock_file4.name = "left_recursion_graph_100.csv"
    mock_file4.parts = ["timing", "domain", "invalid_env", "scale_free"]
    
    mock_file5 = MagicMock()
    mock_file5.name = "left_recursion_graph_100.csv"
    mock_file5.parts = ["timing", "domain", "neo4j", "scale_free"]
    
    mock_rglob.return_value = [mock_file1, mock_file2, mock_file3, mock_file4, mock_file5]

    with patch('builtins.open', create=True) as mock_open:
        mock_open.return_value.__enter__.return_value = [
            "a,QueryRealTime,ExecuteQueryRealTime,WriteResultRealTime\n",
            "1,2,3,4\n",
            "Average,0.0,150.0,2.0,0.5\n"
        ]
        
        generate_scale_free_table.generate()
        
    mock_write.assert_called_once()
    content = mock_write.call_args[0][0]
    assert "Left Recursion" in content
    assert "150.0" in content
    assert "2.00" in content
    assert "0.5000" in content
