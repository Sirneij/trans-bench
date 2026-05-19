
from engine.parameter_sweep import ParameterSweep


def test_parameter_sweep_expand_empty():
    sweep = ParameterSweep()
    assert sweep.count() == 1
    combos = list(sweep.expand())
    assert combos == [{}]

def test_parameter_sweep_expand_multiple():
    config = {
        'query_bindings': {'limit': [10, 20]},
        'modes': ['right_recursion', 'left_recursion'],
        'graph_sizes': [100],
        'systems': ['postgres'],
        'custom_params': {'p1': ['a', 'b']}
    }
    sweep = ParameterSweep.from_dict(config)
    assert sweep.count() == 8 # 2 * 2 * 1 * 1 * 2

    combos = list(sweep.expand())
    assert len(combos) == 8
    
    # Check one specific combo
    first = combos[0]
    assert 'query_bindings' in first
    assert 'mode' in first
    assert 'size' in first
    assert 'system' in first
    assert 'custom_params' in first

    assert first['size'] == 100
    assert first['system'] == 'postgres'
    assert first['query_bindings'] in [{'limit': '10'}, {'limit': '20'}]

def test_parameter_sweep_expand_scalar():
    config = {
        'query_bindings': {'limit': 10},
        'custom_params': {'p1': 'a'}
    }
    sweep = ParameterSweep.from_dict(config)
    combos = list(sweep.expand())
    assert len(combos) == 1
    assert combos[0]['query_bindings'] == {'limit': '10'}
    assert combos[0]['custom_params'] == {'p1': 'a'}

