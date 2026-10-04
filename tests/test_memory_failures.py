"""
Memory measurement (engine/memory.py), failure classification (engine/failures.py), and how the
analysis reports both.
"""

import os
import time

import pytest

from engine.failures import classify_failure
from engine.memory import MemorySampler, find_server_pid, rss_of_self


def test_sampler_reports_before_peak_and_used():
    values = iter([100.0, 150.0, 400.0, 120.0] + [120.0] * 1000)
    s = MemorySampler(lambda: next(values), 'fake', interval=0.001)
    with s:
        time.sleep(0.02)
    r = s.result()
    assert r['before_mb'] == round(100 / 2**20, 3) and r['peak_mb'] == round(400 / 2**20, 3)
    assert r['used_mb'] == round(300 / 2**20, 3) and r['samples'] >= 4


def test_sampler_sees_an_allocation():
    s = MemorySampler(rss_of_self(), 'self', interval=0.005)
    with s:
        block = bytearray(64 * 2**20)  # 64 MB, touched
        for i in range(0, len(block), 4096):
            block[i] = 1
        time.sleep(0.05)
    assert s.result()['used_mb'] > 40
    del block


def test_sampler_never_breaks_the_query():
    def broken():
        raise RuntimeError('process ended')

    s = MemorySampler(broken, 'broken', interval=0.001)
    with s:
        time.sleep(0.005)
    assert 'error' in s.result()


def test_find_server_pid_requires_exactly_one_process():
    import subprocess
    import sys
    import uuid

    import psutil

    with pytest.raises(RuntimeError):
        find_server_pid('no-such-process-name')
    # a process with a command line of its own (parallel test workers share the test runner's)
    token = f'trans-bench-probe-{uuid.uuid4().hex}'
    with subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)', token]) as probe:
        try:
            # on macOS the framework Python re-executes itself, so the name changes once after the start
            for _ in range(100):
                try:
                    assert find_server_pid(psutil.Process(probe.pid).name(), token) == probe.pid
                    break
                except RuntimeError:
                    time.sleep(0.05)
            else:
                pytest.fail('the probe process was never found')
        finally:
            probe.kill()


@pytest.mark.parametrize(
    'errors,expected',
    [
        (['DuckDB command 4 error: Out of Memory Error: failed to allocate data of size 8.0 GiB'], 'oom'),
        (['Neo4j experiment error: {neo4j_code: Neo.TransientError.General.MemoryPoolOutOfMemoryError}'], 'oom'),
        (
            [
                "SingleStore experiment error: (1712, \"Leaf Error: Memory used by MemSQL has reached the "
                "'maximum_memory' setting (6000 Mb)\")"
            ],
            'oom',
        ),
        (
            ['PostgreSQL experiment error: recursive reference to query "tc" must not appear more than once'],
            'unsupported',
        ),
        (
            [
                '(2741, "The query failed because a recursive common table expression exceeded the max number of iterations")'
            ],
            'iteration_limit',
        ),
        (['connection refused'], 'error'),
    ],
)
def test_failure_classification(errors, expected):
    assert classify_failure('error', 1, errors) == expected


def test_failure_classification_status():
    assert classify_failure('ok', 0, []) is None
    assert classify_failure('timeout', None, []) == 'timeout'
    assert classify_failure('error', -9, []) == 'killed'  # ended by a signal the driver did not send


def test_skipped_cells_show_the_cause():
    import analyze_verified as av

    base = dict(limit=600, mean=None, all_correct=None, mem_used_mb=None, error='')
    rows = {
        ('neo4j', 'g', 'm', 1): {**base, 'n': 1, 'status': 'ok', 'failure': None, 'mean': 1.0, 'mem_used_mb': 12.0},
        ('neo4j', 'g', 'm', 2): {**base, 'n': 2, 'status': 'error', 'failure': 'oom'},
        ('neo4j', 'g', 'm', 3): {**base, 'n': 3, 'status': 'skipped', 'failure': None},
        ('xsb', 'g', 'm', 2): {**base, 'n': 2, 'status': 'timeout', 'failure': 'timeout'},
        ('xsb', 'g', 'm', 3): {**base, 'n': 3, 'status': 'skipped', 'failure': None},
    }
    av.apply_skip_causes(rows)
    assert [av.cell(rows, 'neo4j', 'g', 'm', n) for n in (1, 2, 3)] == ['1.000', 'OOM', 'OOM$^{s}$']
    assert [av.cell(rows, 'xsb', 'g', 'm', n) for n in (2, 3)] == ['TO', 'TO$^{s}$']
    assert av.mem_cell(rows, 'neo4j', 'g', 'm', 1) == '12.0'
