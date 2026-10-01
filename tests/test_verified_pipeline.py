"""
Tests for the verified benchmark pipeline: correctness check (engine/verify.py), input generation
rules, connector behaviour that the published measurements depend on, per-run isolation
(engine/run_one.py, benchmark.py) and the analysis (analyze_verified.py).

The end-to-end tests use DuckDB (in-process, no server) on inputs that are tracked in git.
"""

import csv
import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from engine import verify
from engine.connectors import PROTOCOL_REGISTRY, get_connector
from engine.connectors.neo4j_conn import Neo4jConnector
from engine.connectors.rdbms import (
    CockroachDBConnector,
    PostgreSQLConnector,
    MariaDBConnector,
    SingleStoreConnector,
    concatenate_chunks,
)
from engine.connectors.subprocess_conn import XSBConnector
from engine.loader import DescriptorLoader, SystemDescriptor, TimingPhase

BASE = Path(__file__).resolve().parent.parent
PY = sys.executable
VERIFIED = BASE / 'results' / 'verified_2026'
# the campaign of the paper (rerun with memory measurements and recorded failure kinds)
VERIFIED_V2 = BASE / 'results' / 'verified_2026_v2'


def closure_bruteforce(edges):
    adj = {}
    for a, b in edges:
        adj.setdefault(a, set()).add(b)
    pairs = set()
    for s in adj:
        stack, seen = list(adj[s]), set()
        while stack:
            v = stack.pop()
            if v not in seen:
                seen.add(v)
                stack.extend(adj.get(v, ()))
        pairs |= {(s, t) for t in seen}
    return pairs


def summary_of(pairs):
    xs = np.array([p[0] for p in pairs], dtype=np.int64)
    ys = np.array([p[1] for p in pairs], dtype=np.int64)
    c, h = verify.summarize_pairs(xs, ys)
    return {'count': c, 'hash': f'{h:016x}'}


# ─────────────────────────────────────────────────────────────────────────────
# engine/verify.py
# ─────────────────────────────────────────────────────────────────────────────


class TestVerify:
    EDGES = [(1, 2), (2, 3), (3, 1), (3, 4), (5, 6), (1, 2)]  # a cycle, a tail, a parallel edge

    def test_hash_is_order_independent_and_sensitive(self):
        pairs = sorted(closure_bruteforce(self.EDGES))
        assert summary_of(pairs) == summary_of(list(reversed(pairs)))
        assert summary_of(pairs) != summary_of(pairs[:-1])  # a missing pair
        assert summary_of(pairs) != summary_of(pairs + [pairs[0]])  # a duplicated pair
        assert summary_of(pairs) != summary_of(pairs[:-1] + [(9, 9)])  # a wrong pair

    def test_expected_summary_matches_bruteforce(self, tmp_path):
        edge_file = tmp_path / 'edge.facts'
        edge_file.write_text(''.join(f'{a}\t{b}\n' for a, b in self.EDGES))
        c, h = verify.expected_summary(edge_file)
        assert {'count': c, 'hash': f'{h:016x}'} == summary_of(closure_bruteforce(self.EDGES))

    @pytest.mark.parametrize(
        'fmt',
        [
            'x,y\n{rows_csv}',  # DuckDB/PostgreSQL/SingleStore/Neo4j (CSV with header)
            '{rows_csv}',  # MariaDB/CockroachDB (CSV without header)
            '"x","y"\n{rows_quoted}',  # APOC export (quoted)
            '{rows_space}',  # XSB
            '{rows_tab}',
        ],
    )
    def test_summarize_file_formats(self, tmp_path, fmt):
        pairs = sorted(closure_bruteforce(self.EDGES))
        text = fmt.format(
            rows_csv=''.join(f'{a},{b}\n' for a, b in pairs),
            rows_quoted=''.join(f'"{a}","{b}"\n' for a, b in pairs),
            rows_space=''.join(f'{a} {b}\n' for a, b in pairs),
            rows_tab=''.join(f'{a}\t{b}\n' for a, b in pairs),
        )
        f = tmp_path / 'result'
        f.write_text(text)
        c, h = verify.summarize_file(f)
        assert {'count': c, 'hash': f'{h:016x}'} == summary_of(pairs)

    def test_summarize_file_streams_in_chunks(self, tmp_path):
        pairs = [(i, i + 1) for i in range(5000)]
        f = tmp_path / 'r.csv'
        f.write_text('x,y\n' + ''.join(f'{a},{b}\n' for a, b in pairs))
        c, h = verify.summarize_file(f, chunk_bytes=97)  # chunk boundaries inside lines
        assert {'count': c, 'hash': f'{h:016x}'} == summary_of(pairs)

    def test_cached_expected(self, tmp_path):
        edge_file = tmp_path / 'edge.facts'
        edge_file.write_text('1\t2\n2\t3\n')
        cache = tmp_path / 'cache.json'
        exp = verify.cached_expected(edge_file, cache)
        assert exp['count'] == 3
        assert json.loads(cache.read_text())[str(edge_file)] == exp

    def test_shipped_cache_matches_tracked_inputs(self):
        """Expected closures in input/expected_closures.json are what verify computes."""
        cache = json.loads((BASE / 'input' / 'expected_closures.json').read_text())
        for key in ['input/souffle/cycle/100/edge.facts', 'input/souffle/binary_tree/200/edge.facts']:
            c, h = verify.expected_summary(BASE / key)
            assert cache[key] == {'count': c, 'hash': f'{h:016x}'}


# ─────────────────────────────────────────────────────────────────────────────
# generate_db.py: logic-system facts for multigraphs
# ─────────────────────────────────────────────────────────────────────────────


class TestInputGeneration:
    def _generate(self, tmp_path, graph, size):
        from generate_db import GraphGenerator

        GraphGenerator(str(tmp_path / 'input'), {}).generate_and_save_graphs(graph, size)
        tsv = [tuple(map(int, l.split('\t'))) for l in (tmp_path / f'input/souffle/{graph}/{size}/edge.facts').read_text().splitlines()]
        lp = (tmp_path / f'input/clingo_xsb/{graph}/graph_{size}.lp').read_text().splitlines()
        return tsv, lp

    def test_scale_free_lp_is_distinct_and_sorted_tsv_keeps_parallel_edges(self, tmp_path):
        tsv, lp = self._generate(tmp_path, 'scale_free', 300)
        assert len(tsv) > len(set(tsv)), 'networkx.scale_free_graph(300, seed=42) has parallel edges'
        assert lp == [f'edge({a}, {b}).' for a, b in sorted(set(tsv))]

    def test_duplicate_free_graph_keeps_generation_order(self, tmp_path):
        tsv, lp = self._generate(tmp_path, 'barabasi_albert', 300)
        assert len(tsv) == len(set(tsv))
        assert lp == [f'edge({a}, {b}).' for a, b in tsv]

    def test_manifest_covers_campaign_inputs(self):
        lines = (BASE / 'input' / 'SHA256SUMS').read_text().splitlines()
        assert len(lines) == 2 * (12 * 10 + 9 + 10)
        assert all(len(l.split()[0]) == 64 for l in lines)


# ─────────────────────────────────────────────────────────────────────────────
# Descriptors
# ─────────────────────────────────────────────────────────────────────────────


class TestDescriptors:
    @pytest.fixture(scope='class')
    def systems(self):
        return {s.name: s for s in DescriptorLoader(base_dir=BASE, detect_versions=False).load_systems()}

    def test_protocols_route_to_their_connectors(self, systems):
        assert get_connector(systems['cockroachdb'].protocol) is CockroachDBConnector
        assert get_connector(systems['singlestore'].protocol) is SingleStoreConnector
        assert get_connector(systems['mariadb'].protocol) is MariaDBConnector
        assert PROTOCOL_REGISTRY['singlestore'] is SingleStoreConnector

    def test_query_phase_and_result_file(self, systems):
        for s in systems.values():
            s.query_columns  # raises if query_phase is not one of the timing phases
        verified = ['postgres', 'mariadb', 'duckdb', 'cockroachdb', 'mongodb', 'neo4j', 'singlestore', 'xsb']
        for name in verified:
            assert systems[name].result_file, name
        assert systems['neo4j'].query_columns == ('QueryRealTime', 'QueryCPUTime')
        assert systems['xsb'].query_columns == ('QueryRealTime', 'QueryCPUTime')
        assert systems['postgres'].query_columns == ('ExecuteQueryRealTime', 'ExecuteQueryCPUTime')

    def test_every_declared_mode_has_a_rule_file(self, systems):
        for name in ['postgres', 'mariadb', 'duckdb', 'cockroachdb', 'mongodb', 'neo4j', 'singlestore', 'xsb']:
            s = systems[name]
            for mode in s.modes:
                assert (s.rules_dir / f'transitive_{mode}{s.rule_extension}').exists(), (name, mode)
        assert 'doublerecurring_recursion' in systems['duckdb'].modes

    def test_neo4j_rules_time_the_distinct_pairs(self):
        for f in (BASE / 'systems/neo4j/rules').glob('transitive_*.cypher'):
            statements = [s.strip() for s in f.read_text().split(';') if s.strip()]
            assert 'WITH DISTINCT start.id AS x, end.id AS y' in statements[-2]
            assert 'RETURN count(*)' in statements[-2]

    def test_singlestore_rules_use_union_all_with_outer_distinct(self):
        for f in (BASE / 'systems/singlestore/rules').glob('transitive_*.py'):
            text = f.read_text()
            assert 'UNION ALL' in text and 'SELECT DISTINCT x, y FROM tc' in text


# ─────────────────────────────────────────────────────────────────────────────
# Connectors
# ─────────────────────────────────────────────────────────────────────────────


def _desc(name, protocol, phases, **kw):
    return SystemDescriptor(
        name=name, display_name=name, category='db', protocol=protocol,
        timing_phases=[TimingPhase(p.lower(), p) for p in phases], input_format='tsv', modes=['left_recursion'],
        rule_extension=kw.pop('rule_extension', '.py'), flags=kw.pop('flags', {}), execution={},
        descriptor_path=kw.pop('descriptor_path', Path('dummy/descriptor.yaml')), rules_dir=Path('dummy'),
        credentials=kw.pop('credentials', {}), **kw,
    )


class TestNeo4jTiming:
    @patch('neo4j.GraphDatabase.driver')
    def test_setup_consumed_query_and_export_fetched(self, mock_driver, tmp_path):
        """session.run() is lazy: every statement must be finished inside its own timed call."""
        driver, session = MagicMock(), MagicMock()
        mock_driver.return_value = driver
        driver.session.return_value = session
        events = []

        def run(cypher):
            res = MagicMock()
            res.consume.side_effect = lambda: events.append(('consume', cypher.split()[0]))
            res.__iter__.side_effect = lambda: (events.append(('fetch', cypher.split()[0])), iter([{'pairs': 3}]))[1]
            return res

        session.run.side_effect = run
        desc = _desc('neo4j', 'neo4j', ['DeleteData', 'LoadData', 'CreateIndexX', 'CreateIndexY', 'Query', 'WriteResult'],
                     rule_extension='.cypher', result_file='neo4j_results.csv')
        conn = Neo4jConnector()
        conn.connect({'import_directory': str(tmp_path)}, desc)
        rule = BASE / 'systems/neo4j/rules/transitive_left_recursion.cypher'
        facts = tmp_path / 'in' / 'edge.facts'
        facts.parent.mkdir()
        facts.write_text('1\t2\n')
        with patch('subprocess.run'):
            conn.run_experiment(rule, facts, tmp_path, desc, {})
        assert [e[0] for e in events] == ['consume', 'consume', 'consume', 'fetch', 'fetch']
        assert events[3][1] == 'MATCH' and events[4][1] == 'CALL'
        assert conn.errors == []


class TestSingleStoreConnector:
    def test_statement_sequence_and_local_result_file(self, tmp_path):
        desc = DescriptorLoader(base_dir=BASE, detect_versions=False).get_system('singlestore')
        cursor = MagicMock()
        cursor.fetchall.return_value = [(1, 2), (2, 3)]
        mconn = MagicMock()
        mconn.cursor.return_value = cursor
        with patch('MySQLdb.connect', return_value=mconn), \
                patch.object(SingleStoreConnector, 'memory_sampler', return_value=None):
            c = SingleStoreConnector()
            c.connect({'host': 'h', 'port': 3307, 'user': 'u', 'password': 'p', 'database': 'benchmark'}, desc)
            row = c.run_experiment(desc.rules_dir / 'transitive_left_recursion.py', Path('/data/edge.facts'), tmp_path,
                                   desc, {})
        sql = [' '.join(str(call.args[0]).split()) for call in cursor.execute.call_args_list]
        assert sql[:2] == ['DROP TABLE IF EXISTS tc_result;', 'DROP TABLE IF EXISTS edge;']
        assert sql[2] == 'SET SESSION max_recursive_cte_iterations = 10000;'
        assert sql[3].startswith('CREATE TABLE edge')
        assert sql[4].startswith("LOAD DATA LOCAL INFILE '/data/edge.facts'")
        assert 'UNION ALL' in sql[7] and sql[7].startswith('CREATE TABLE tc_result AS')
        assert sql[8] == 'SELECT x, y FROM tc_result;'
        assert (tmp_path / 'singlestore_results.csv').read_text() == 'x,y\n1,2\n2,3\n'
        assert c.errors == [] and set(row) >= {'ExecuteQueryRealTime', 'WriteResultRealTime'}

    def test_failure_is_recorded(self, tmp_path):
        desc = DescriptorLoader(base_dir=BASE, detect_versions=False).get_system('singlestore')
        cursor = MagicMock()
        cursor.execute.side_effect = [None, None, None, None, None, None, None,
                                      Exception('(2741, "recursive CTE iteration limit")')]
        mconn = MagicMock()
        mconn.cursor.return_value = cursor
        with patch('MySQLdb.connect', return_value=mconn), \
                patch.object(SingleStoreConnector, 'memory_sampler', return_value=None):
            c = SingleStoreConnector()
            c.connect({}, desc)
            c.run_experiment(desc.rules_dir / 'transitive_left_recursion.py', Path('x'), tmp_path, desc, {})
        assert len(c.errors) == 1 and '2741' in c.errors[0]


class TestMemoryProbes:
    def test_postgres_probe_is_the_backend_process(self):
        import os

        desc = DescriptorLoader(base_dir=BASE, detect_versions=False).get_system('postgres')
        conn = MagicMock()
        conn.cursor.return_value.fetchone.return_value = (os.getpid(),)  # pretend we are the backend
        with patch('psycopg2.connect', return_value=conn):
            c = PostgreSQLConnector()
            c.connect({'dbURL': 'x'}, desc)
        assert c.memory_sampler().probe() > 0

    def test_query_phase_records_memory(self, tmp_path):
        """DuckDB (in-process): the query phase gets a memory record; other phases do not affect it."""
        from engine.connectors.duckdb_conn import DuckDBConnector

        desc = DescriptorLoader(base_dir=BASE, detect_versions=False).get_system('duckdb')
        c = DuckDBConnector()
        c.connect({}, desc)
        rule = tmp_path / 'transitive_left_recursion.sql'
        rule.write_text((desc.rules_dir / 'transitive_left_recursion.sql').read_text())
        c.run_experiment(rule, BASE / 'input/souffle/cycle/100/edge.facts', tmp_path, desc, {})
        assert c.errors == [] and c.memory['probe'] == 'duckdb process RSS' and c.memory['used_mb'] >= 0


class TestCockroachChunks:
    def test_concatenate_in_name_order(self, tmp_path):
        (tmp_path / 'n1.1.csv').write_text('3,4\n')
        (tmp_path / 'n1.0.csv').write_text('1,2\n')
        out = tmp_path / 'r.csv'
        assert concatenate_chunks(tmp_path, out) == 2
        assert out.read_text() == '1,2\n3,4\n'

    def test_no_chunks_is_an_error(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            concatenate_chunks(tmp_path, tmp_path / 'r.csv')


class TestErrorReporting:
    def test_mariadb_error_is_recorded(self, tmp_path):
        rule = tmp_path / 'transitive_left_recursion.py'
        rule.write_text('class MariaDBLeftRecursion:\n'
                        '    def __init__(self, config, conn): pass\n'
                        '    def drop_tc_path_tc_result_tables(self): pass\n'
                        '    def set_standard_cte_to_zero(self): pass\n'
                        '    def create_tc_path_table(self): raise RuntimeError("boom")\n')
        (tmp_path / '__init__.py').write_text('')
        desc = _desc('mariadb', 'mysqlclient', ['A', 'B', 'C', 'D', 'E', 'F'],
                     descriptor_path=tmp_path / 'descriptor.yaml')
        c = MariaDBConnector()
        c._connection = MagicMock()
        c.run_experiment(rule, Path('x'), tmp_path, desc, {})
        assert c.errors == ['MariaDB experiment error: boom']

    @patch('engine.connectors.base.BaseConnector.timed_subprocess')
    def test_xsb_failure_is_recorded(self, mock_sub, tmp_path):
        mock_sub.return_value = (1.0, 0.1, 10.0, subprocess.CompletedProcess([], 1, '', 'Memory exhausted'))
        desc = _desc('xsb', 'subprocess', ['LoadRules', 'LoadFacts', 'Query', 'Write'], rule_extension='.P')
        c = XSBConnector()
        c.run_experiment(tmp_path / 'r.P', tmp_path / 'g.lp', tmp_path, desc, {})
        assert len(c.errors) == 2 and 'Memory exhausted' in c.errors[0]


# ─────────────────────────────────────────────────────────────────────────────
# engine/run_one.py and benchmark.py (end to end, DuckDB)
# ─────────────────────────────────────────────────────────────────────────────


def _run(cmd, **kw):
    return subprocess.run(cmd, cwd=BASE, capture_output=True, text=True, timeout=300, **kw)


class TestRunOne:
    def test_one_trial_writes_timing_and_correct_result(self, tmp_path):
        p = _run([PY, '-m', 'engine.run_one', '--system', 'duckdb', '--graph', 'cycle', '--mode', 'left_recursion',
                  '--size', '100', '--timing-dir', str(tmp_path)])
        assert p.returncode == 0, p.stdout + p.stderr
        outcome = json.loads(p.stdout.strip().splitlines()[-1].split(' ', 1)[1])
        assert outcome['errors'] == []
        rows = list(csv.DictReader(open(outcome['timing_path'])))
        assert len(rows) == 1 and float(rows[0]['ExecuteQueryRealTime']) > 0
        c, h = verify.summarize_file(Path(outcome['result_path']))
        assert {'count': c, 'hash': f'{h:016x}'} == verify.cached_expected(Path('input/souffle/cycle/100/edge.facts'),
                                                                            BASE / 'input/expected_closures.json')

    def test_setup_failure_exit_code(self, tmp_path):
        p = _run([PY, '-m', 'engine.run_one', '--system', 'duckdb', '--graph', 'cycle', '--mode', 'left_recursion',
                  '--size', '123', '--timing-dir', str(tmp_path)])
        assert p.returncode == 2
        assert 'not found' in p.stdout


class TestBenchmarkDriver:
    def _records(self, out):
        return [json.loads(l) for l in (out / 'runs.jsonl').read_text().splitlines()]

    def test_ok_runs_are_verified(self, tmp_path):
        out = tmp_path / 'duckdb'
        p = _run([PY, 'benchmark.py', '--systems', 'duckdb', '--graphs', 'path', '--modes', 'left_recursion',
                  'double_recursion', 'doublerecurring_recursion', '--sizes', '100', '--runs', '2', '--timeout', '120',
                  '--out', str(out), '--expected-cache', str(tmp_path / 'cache.json'), '--no-analysis'])
        assert p.returncode == 0, p.stderr
        recs = self._records(out)
        assert [(r['mode'], r['run'], r['status']) for r in recs] == [
            (m, i, 'ok') for m in ('left_recursion', 'double_recursion', 'doublerecurring_recursion') for i in (1, 2)]
        correct = {r['mode']: r['correct'] for r in recs}
        # DuckDB's plain double recursion is incomplete on Path; with recurring.tc it is correct
        assert correct == {'left_recursion': True, 'double_recursion': False, 'doublerecurring_recursion': True}
        assert all(r['timing_row'] and r['errors'] == [] for r in recs)
        assert all(r['timeout_s'] == 120 for r in recs)  # the limit is stored with every run
        assert not list((out / 'timing').rglob('duckdb_results.csv')), 'result files are deleted after checking'
        assert (out / 'logs' / 'duckdb_path_left_recursion_100_run1.log').exists()

        # restarting with the same arguments runs nothing again
        p = _run([PY, 'benchmark.py', '--systems', 'duckdb', '--graphs', 'path', '--modes', 'left_recursion',
                  '--sizes', '100', '--runs', '2', '--out', str(out), '--expected-cache', str(tmp_path / 'cache.json'),
                  '--no-analysis'])
        assert len(self._records(out)) == len(recs)

    def test_campaign_is_analyzed_at_the_end(self, tmp_path):
        """benchmark.py analyzes the campaign directory: tables, matplotlib and LaTeX figures."""
        out = tmp_path / 'campaign' / 'duckdb'
        p = _run([PY, 'benchmark.py', '--systems', 'duckdb', '--graphs', 'cycle', '--modes', 'left_recursion',
                  'right_recursion', '--sizes', '100', '200', '--runs', '2', '--out', str(out),
                  '--expected-cache', str(tmp_path / 'cache.json'), '--no-latex-compile'])
        assert p.returncode == 0, p.stdout + p.stderr
        analysis = tmp_path / 'campaign' / 'analysis'
        assert (analysis / 'summary.csv').exists() and (analysis / 'figures' / 'cycle_elapsed.pdf').exists()
        tex = (analysis / 'figures_tex' / 'cycle_elapsed.tex').read_text()
        assert tex.startswith('\\documentclass') and 'DuckDB' in tex and 'ymode=log' in tex

    def test_one_series_per_output_directory(self, tmp_path):
        p = _run([PY, 'benchmark.py', '--systems', 'duckdb', 'xsb', '--graphs', 'cycle', '--sizes', '100',
                  '--out', str(tmp_path / 'duckdb')])
        assert p.returncode == 2 and 'its own directory' in p.stderr

    def test_timeout_kills_and_skips_larger_sizes(self, tmp_path):
        out = tmp_path / 'to'
        p = _run([PY, 'benchmark.py', '--systems', 'duckdb', '--graphs', 'cycle', '--modes', 'left_recursion',
                  '--sizes', '100', '200', '--runs', '3', '--timeout', '0.01', '--out', str(out),
                  '--expected-cache', str(tmp_path / 'cache.json'), '--no-analysis'])
        assert p.returncode == 0, p.stderr
        recs = self._records(out)
        assert [(r['n'], r['run'], r['status']) for r in recs] == [(100, 1, 'timeout'), (200, None, 'skipped')]

    def test_error_run_stops_configuration(self, tmp_path):
        if (BASE / 'systems/singlestore/credentials.yaml').exists():
            pytest.skip('local SingleStore credentials would override the unreachable test server')
        cfg = tmp_path / 'cfg.json'
        cfg.write_text(json.dumps({'singlestore': {'host': '127.0.0.1', 'port': 1, 'user': 'u', 'password': '',
                                                   'database': 'x'}}))
        out = tmp_path / 's2'
        p = _run([PY, 'benchmark.py', '--systems', 'singlestore', '--graphs', 'path', '--modes', 'left_recursion',
                  '--sizes', '100', '200', '--runs', '5', '--timeout', '60', '--out', str(out), '--config-file', str(cfg),
                  '--expected-cache', str(tmp_path / 'cache.json'), '--no-analysis'])
        assert p.returncode == 0, p.stderr
        recs = self._records(out)
        assert [(r['n'], r['run'], r['status']) for r in recs] == [(100, 1, 'error'), (200, None, 'skipped')]
        assert "Can't connect" in recs[0]['errors'][0]


# ─────────────────────────────────────────────────────────────────────────────
# analyze_verified.py on the shipped campaign
# ─────────────────────────────────────────────────────────────────────────────


class TestAnalysis:
    def test_series_to_system(self):
        import analyze_verified

        assert analyze_verified.base_system('mariadb_tuned') == 'mariadb'
        assert analyze_verified.query_columns('neo4j') == ('QueryRealTime', 'QueryCPUTime')
        assert analyze_verified.query_columns('duckdb') == ('ExecuteQueryRealTime', 'ExecuteQueryCPUTime')
        with pytest.raises(ValueError):
            analyze_verified.base_system('nosuchsystem')

    @pytest.mark.parametrize('campaign', [VERIFIED, VERIFIED_V2], ids=['verified_2026', 'verified_2026_v2'])
    def test_reanalysis_reproduces_published_tables(self, tmp_path, campaign):
        """Tables, summary and verification of the paper are regenerated exactly from runs.jsonl + logs."""
        if not campaign.exists():
            pytest.skip(f'{campaign} not present')
        p = _run([PY, 'analyze_verified.py', str(campaign), '--out', str(tmp_path), '--no-compile'])
        assert p.returncode == 0, p.stderr
        published = campaign / 'analysis'
        names = [f.name for f in published.glob('table_*.tex')] + ['summary.csv', 'verification.json', 'failures.csv']
        assert len(names) == 17  # 8 time tables, 6 memory tables, summary, verification, failures
        for name in names:
            assert (tmp_path / name).read_text() == (published / name).read_text(), name
        tex_figures = sorted(f.name for f in (published / 'figures_tex').glob('*.tex'))
        assert len(tex_figures) == 28
        for name in tex_figures:  # the pgfplots transcription is deterministic
            assert (tmp_path / 'figures_tex' / name).read_text() == (published / 'figures_tex' / name).read_text(), name
        v = json.loads((tmp_path / 'verification.json').read_text())
        assert v['incorrect_results'] == {
            'duckdb/double_recursion': ['binary_tree', 'cycle', 'grid', 'multi_path', 'path', 'reverse_binary_tree', 'y'],
            'mariadb/right_recursion': ['scale_free'],
        }
