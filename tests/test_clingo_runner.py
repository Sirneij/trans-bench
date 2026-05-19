import math
import sys
from unittest.mock import MagicMock, patch

import pytest

from engine.connectors.clingo_runner import _estimate_os_times, main


def test_estimate_os_times():
    t1 = (1.0, 2.0, 3.0, 4.0, 10.0)
    t2 = (2.0, 3.0, 4.0, 5.0, 15.0)
    real, cpu = _estimate_os_times(t1, t2)
    assert real == 5.0
    assert cpu == 4.0

@patch('os.times')
@patch('clingo.Control')
@patch('builtins.open')
def test_clingo_runner_main(mock_open, mock_control, mock_times, tmp_path):
    # Set up argv
    test_argv = ['clingo_runner.py', 'rules.lp', 'facts.lp', 'output.txt']
    
    # Mock times
    mock_times.side_effect = [
        (1, 1, 1, 1, 10.0), # t0 load rules
        (1, 1, 1, 1, 10.1), # t1 load rules
        (1, 1, 1, 1, 10.2), # t0 load facts
        (1, 1, 1, 1, 10.3), # t1 load facts
        (1, 1, 1, 1, 10.4), # t0 ground
        (1, 1, 1, 1, 10.5), # t1 ground
        (1, 1, 1, 1, 10.6), # t0 solve
        (1, 1, 1, 1, 10.7), # t1 solve
        (1, 1, 1, 1, 10.8), # t0 write
        (1, 1, 1, 1, 10.9), # t1 write
    ]

    # Mock Control
    ctl = MagicMock()
    mock_control.return_value = ctl

    # Mock solve callback execution
    def mock_solve(on_model):
        model = MagicMock()
        model.__str__.return_value = "path(1,2)"
        on_model(model)
    ctl.solve.side_effect = mock_solve

    # Run main
    with patch('sys.argv', test_argv):
        with patch('builtins.print') as mock_print:
            main()
            
            # Verify printed values
            printed_outputs = [args[0] for args, kwargs in mock_print.call_args_list]
            
            # Parse outputs into a dictionary
            results = {}
            for out in printed_outputs:
                key, val = out.split(': ')
                results[key] = float(val)
                
            assert math.isclose(results["LoadRuleTime"], 0.1, rel_tol=1e-5)
            assert math.isclose(results["LoadFactsTime"], 0.1, rel_tol=1e-5)
            assert math.isclose(results["GroundTime"], 0.1, rel_tol=1e-5)
            assert math.isclose(results["QueryTime"], 0.1, rel_tol=1e-5)
            assert math.isclose(results["WriteTime"], 0.1, rel_tol=1e-5)

@patch('sys.argv', ['clingo_runner.py'])
def test_clingo_runner_main_usage():
    with pytest.raises(SystemExit) as e:
        main()
    assert e.value.code == 1

def test_clingo_runner_subprocess():
    import subprocess
    result = subprocess.run([sys.executable, 'engine/connectors/clingo_runner.py'], capture_output=True)
    assert result.returncode == 1

