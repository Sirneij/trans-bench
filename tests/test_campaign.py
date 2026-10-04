"""
tests/test_campaign.py

The campaign engine (engine/campaign.py) that benchmark.py, transitive.py and the Web UI share. The
end-to-end tests use DuckDB, which needs no server, and the inputs tracked under input/.
"""

import json
import shlex
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from engine.campaign import Campaign, CampaignError, CampaignSpec, log_errors, parse_outcome, read_done
from engine.run_one import RESULT_MARKER


def _spec(tmp_path, **kw):
    args = dict(
        systems=['duckdb'],
        graphs=['path'],
        modes=['left_recursion'],
        sizes=[10, 20],
        runs=2,
        timeout=120,
        campaign_dir=tmp_path / 'camp',
        expected_cache=tmp_path / 'cache.json',
    )
    args.update(kw)
    return CampaignSpec(**args)


def _records(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


class TestSpec:
    @pytest.mark.parametrize(
        'change, message',
        [
            ({'systems': []}, 'at least one system'),
            ({'sizes': [20, 10]}, 'increasing'),
            ({'sizes': [10, 10]}, 'increasing'),
            ({'runs': 0}, 'positive'),
            ({'timeout': 0}, 'positive'),
        ],
    )
    def test_invalid(self, tmp_path, change, message):
        with pytest.raises(CampaignError, match=message):
            _spec(tmp_path, **change).validate()

    def test_series_dir_and_suffix(self, tmp_path):
        spec = _spec(tmp_path, systems=['mariadb'], series_suffix='_tuned')
        assert spec.series_dir('mariadb') == (tmp_path / 'camp' / 'mariadb_tuned').resolve()

    def test_verifies_only_transitive_full_materialization(self, tmp_path):
        assert _spec(tmp_path).verifies
        assert not _spec(tmp_path, query_mode='demand_driven').verifies
        assert not _spec(tmp_path, domain='shortest_path').verifies

    def test_command_round_trips_through_benchmark_parser(self, tmp_path):
        from benchmark import build_parser

        spec = _spec(tmp_path, label='a label')
        args = build_parser().parse_args(shlex.split(spec.command())[2:])
        assert args.systems == ['duckdb'] and args.sizes == [10, 20] and args.runs == 2
        assert Path(args.campaign).resolve() == spec.campaign_dir and args.label == 'a label'


class TestHelpers:
    def test_parse_outcome_takes_the_last_marker(self):
        text = f'noise\n{RESULT_MARKER}{{"errors": ["a"]}}\n{RESULT_MARKER}{{"errors": []}}\n'
        assert parse_outcome(text) == {'errors': []}
        assert parse_outcome('killed before printing') is None

    def test_log_errors(self):
        assert log_errors('x - INFO: fine\n2026 - ERROR: broken pipe\n') == ['broken pipe']

    def test_read_done(self, tmp_path):
        f = tmp_path / 'runs.jsonl'
        assert read_done(f) == set()
        f.write_text(json.dumps({'system': 's', 'graph': 'g', 'mode': 'm', 'n': 1}) + '\n\n')
        assert read_done(f) == {('s', 'g', 'm', 1)}


class TestPrepare:
    def test_unknown_system_and_graph(self, tmp_path):
        with pytest.raises(CampaignError, match='unknown system'):
            Campaign(_spec(tmp_path, systems=['nope'])).prepare()
        with pytest.raises(CampaignError, match='unknown graph'):
            Campaign(_spec(tmp_path, graphs=['nope'])).prepare()

    def test_plan_keeps_only_declared_modes(self, tmp_path):
        # only DuckDB declares doublerecurring_recursion (double recursion over its RECURRING table)
        spec = _spec(tmp_path, systems=['duckdb', 'postgres'], modes=['left_recursion', 'doublerecurring_recursion'])
        campaign = Campaign(spec)
        plan = campaign.prepare()
        assert plan['system_modes'] == {
            'duckdb': ['left_recursion', 'doublerecurring_recursion'],
            'postgres': ['left_recursion'],
        }
        assert campaign.total == 3 * 1 * 2  # (2 + 1 modes) x 1 graph x 2 sizes

    def test_missing_inputs(self, tmp_path):
        campaign = Campaign(_spec(tmp_path, sizes=[7, 10]))  # n = 7 is not among the tracked inputs
        campaign.prepare()
        assert campaign.missing_inputs() == {'path': [7]}

    def test_generation_that_leaves_inputs_missing_is_an_error(self, tmp_path):
        campaign = Campaign(_spec(tmp_path, sizes=[7]))
        campaign.prepare()
        fake = MagicMock()
        fake.__enter__.return_value = fake
        fake.stdout = ['generated nothing\n']
        fake.returncode = 0
        with patch('engine.campaign.subprocess.Popen', return_value=fake):
            with pytest.raises(CampaignError, match='still missing'):
                campaign.generate_inputs()


@pytest.mark.xdist_group('duckdb')  # DuckDB trials share systems/duckdb/rules/duckdb/duckdb_file.db
class TestRun:
    """End to end with DuckDB."""

    def test_runs_are_recorded_verified_and_resumed(self, tmp_path):
        events = []
        counts = Campaign(_spec(tmp_path), on_event=events.append).run()
        assert counts == {'ok': 2, 'failed': 0, 'skipped': 0, 'resumed': 0, 'stopped': False}
        recs = _records(tmp_path / 'camp' / 'duckdb' / 'runs.jsonl')
        assert [(r['n'], r['run'], r['status'], r['correct']) for r in recs] == [
            (10, 1, 'ok', True),
            (10, 2, 'ok', True),
            (20, 1, 'ok', True),
            (20, 2, 'ok', True),
        ]
        progress = [(e['size'], e['status'], e['outcome']) for e in events if e['type'] == 'progress']
        assert progress == [(10, 'running', ''), (10, 'done', 'ok'), (20, 'running', ''), (20, 'done', 'ok')]
        assert sum(e['type'] == 'run' for e in events) == 4
        assert all('command' not in e for e in events)

        again = Campaign(_spec(tmp_path)).run()
        assert again['resumed'] == 2 and again['ok'] == 0
        assert len(_records(tmp_path / 'camp' / 'duckdb' / 'runs.jsonl')) == 4

    def test_results_are_not_checked_outside_the_closure_domain(self, tmp_path):
        Campaign(_spec(tmp_path, sizes=[10], runs=1, query_mode='demand_driven')).run()
        rec = _records(tmp_path / 'camp' / 'duckdb' / 'runs.jsonl')[0]
        assert 'correct' not in rec and 'result' not in rec

    def test_stop_kills_the_run_and_records_nothing(self, tmp_path):
        stop = threading.Event()
        events = []

        def on_event(e):
            events.append(e)
            if e['type'] == 'progress' and e['status'] == 'running':
                threading.Timer(0.3, stop.set).start()

        # expected_max_n=0: no expected closure is computed first, so the run starts at once
        spec = _spec(tmp_path, graphs=['complete'], sizes=[1000], runs=3, expected_max_n=0)
        t0 = time.monotonic()
        counts = Campaign(spec, on_event=on_event, should_stop=stop.is_set).run()
        assert counts['stopped']
        assert time.monotonic() - t0 < 60
        runs_file = tmp_path / 'camp' / 'duckdb' / 'runs.jsonl'
        assert not runs_file.exists() or runs_file.read_text() == ''
        assert any(e['type'] == 'log' and 'Stopped on request' in e['message'] for e in events)


def test_clingo_results_are_verified(tmp_path):
    """Clingo's runner writes one "x,y" pair per line, so its results can be checked like the others."""
    pytest.importorskip('clingo')
    spec = _spec(
        tmp_path, systems=['clingo'], graphs=['cycle'], modes=['left_recursion', 'double_recursion'], sizes=[10], runs=1
    )
    Campaign(spec).run()
    recs = _records(tmp_path / 'camp' / 'clingo' / 'runs.jsonl')
    assert [(r['mode'], r['status'], r['correct']) for r in recs] == [
        ('left_recursion', 'ok', True),
        ('double_recursion', 'ok', True),
    ]
