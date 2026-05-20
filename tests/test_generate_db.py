import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import generate_db


def test_data_generator():
    dg = generate_db.DataGenerator()
    assert list(dg.generate_complete_graph(2)) == [(1,1), (1,2), (2,1), (2,2)]
    assert list(dg.generate_max_acyclic_graph(3)) == [(2,1), (3,1), (3,2)]
    assert list(dg.generate_cycle_graph(3)) == [(1,2), (2,3), (3,1)]
    
    dg.k = 1
    assert len(list(dg.generate_cycle_with_shortcuts_graph(3))) == 6
    
    assert list(dg.generate_path_graph(3)) == [(1,2), (2,3)]
    assert list(dg.generate_multi_path_graph(3)) == [(1,2), (2,3)]
    
    assert list(dg.generate_binary_tree_graph(4)) == [(1,2), (1,3)]
    assert list(dg.generate_reverse_binary_tree_graph(4)) == [(2,1), (3,1)]
    dg.k = 2
    assert list(dg.generate_y_graph(2)) == [(1,3), (2,3), (3,4)]
    assert list(dg.generate_w_graph(2)) == [(1,4), (1,3), (2,3), (2,4)]
    assert list(dg.generate_x_graph(2)) == [(1,3), (2,3), (3,4), (3,5)]
    assert list(dg.generate_star_graph(3)) == [(2,1), (3,1)]
    assert list(dg.generate_grid_graph(4)) == [(1,2), (3,4), (1,3), (2,4)]

@patch('networkx.barabasi_albert_graph')
def test_barabasi_albert_graph(mock_ba):
    dg = generate_db.DataGenerator()
    assert list(dg.generate_barabasi_albert_graph(2, 2)) == [(1,2)]
    
    mock_g = MagicMock()
    mock_g.edges.return_value = [(0,1)]
    mock_ba.return_value = mock_g
    assert list(dg.generate_barabasi_albert_graph(4, 2)) == [(1,2)]

@patch('networkx.scale_free_graph')
def test_scale_free_graph(mock_sf):
    dg = generate_db.DataGenerator()
    mock_g = MagicMock()
    mock_g.edges.return_value = [(0,1)]
    mock_sf.return_value = mock_g
    assert list(dg.generate_scale_free_graph(3)) == [(1,2)]

def test_graph_generator_save(tmp_path):
    gg = generate_db.GraphGenerator(str(tmp_path), {})
    
    def gen_func(s):
        yield (1, 2)
        yield (2, 3, 4)
    
    gg.save_for_alda(gen_func, 2, tmp_path / "alda.pkl")
    assert (tmp_path / "alda.pkl").exists()
    
    gg.save_for_souffle(gen_func, 2, tmp_path / "souffle.txt")
    assert "1\t2" in (tmp_path / "souffle.txt").read_text()
    
    gg.save_for_clingo_xsb(gen_func, 2, tmp_path / "clingo.lp")
    content = (tmp_path / "clingo.lp").read_text()
    assert "edge(1, 2)." in content
    assert "edge(2,3,4)." in content

def test_generate_and_save_graphs(tmp_path):
    config = {
        'defaults': {
            'systems': {
                'environmentExtensions': {'clingo': '.lp', 'postgres': '.py', 'alda': '.da', 'souffle': '.dl'},
                'dbSystems': ['postgres'],
            }
        }
    }
    gg = generate_db.GraphGenerator(str(tmp_path), config)
    gg.generate_and_save_graphs('complete', 2)
    
    assert (tmp_path / "clingo_xsb" / "complete" / "graph_2.lp").exists()
    assert (tmp_path / "alda" / "complete" / "graph_2.da").exists()
    assert (tmp_path / "souffle" / "complete" / "2" / "edge.facts").exists()
    assert (tmp_path / "clingo_xsb" / "complete" / "queries_2.csv").exists()

    gg.generate_and_save_graphs('grid', 4)
    gg.generate_graphs([2], ['complete'])
    assert (tmp_path / "clingo_xsb" / "grid" / "queries_4.csv").exists()

    # Shortest path domain
    gg2 = generate_db.GraphGenerator(str(tmp_path), config, domain='shortest_path')
    gg2.generate_and_save_graphs('complete', 2)
    assert (tmp_path / "souffle" / "complete" / "2" / "edge_weighted.facts").exists()

    # Invalid graph type
    gg.generate_and_save_graphs('invalid', 2)

@patch('argparse.ArgumentParser.parse_args')
def test_main(mock_args, tmp_path):
    cfg_file = tmp_path / "cfg.json"
    cfg_file.write_text("{}")
    
    args = MagicMock()
    args.config = str(cfg_file)
    args.domain = 'transitive_closure'
    args.sizes = [2, 3, 1]
    args.graph_types = ['complete']
    mock_args.return_value = args
    
    with patch('generate_db.GraphGenerator') as mock_gg:
        generate_db.main()
        mock_gg.return_value.generate_graphs.assert_called_once()
    
    # without config file
    args.config = '{"key": "val"}'
    with patch('generate_db.GraphGenerator') as mock_gg:
        generate_db.main()
        mock_gg.assert_called()
