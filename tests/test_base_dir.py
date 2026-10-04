"""
tests/test_base_dir.py

Tests of the modules at the root of the repository: common.py (the Base class of the systems' rule
modules), generate_db.py and transitive.py.
"""

import subprocess
import sys
from pathlib import Path

from common import Base

BASE = Path(__file__).resolve().parent.parent


class TestCommonModule:
    """common.Base keeps the configuration of the systems' operations classes."""

    def test_init_keeps_config(self):
        config = {'host': 'localhost', 'port': 5432, 'database': 'testdb'}
        base = Base(config)
        assert base.config == config
        assert base.driver is None
        assert base.db_path is None

    def test_close_without_driver(self):
        Base({}).close()  # nothing opened, nothing to close


class TestGenerateDB:
    """generate_db.py writes every input format; engine/campaign.py calls it with --size-list."""

    def test_data_generator_class_exists(self):
        from generate_db import DataGenerator

        assert isinstance(DataGenerator, type)

    def test_size_list_option(self, tmp_path):
        # generate_db.py writes into ./input, so it runs with the temporary directory as working directory
        script = BASE / 'generate_db.py'
        p = subprocess.run(
            [sys.executable, str(script), '--graph-types', 'path', '--size-list', '3', '5'],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert p.returncode == 0, p.stderr
        for n in (3, 5):
            assert (tmp_path / 'input' / 'souffle' / 'path' / str(n) / 'edge.facts').exists()
            assert (tmp_path / 'input' / 'clingo_xsb' / 'path' / f'graph_{n}.lp').exists()
        assert not (tmp_path / 'input' / 'souffle' / 'path' / '4').exists()


class TestTransitiveModule:
    """transitive.py answers --help with the campaign options."""

    def test_help(self):
        p = subprocess.run(
            [sys.executable, 'transitive.py', '--help'], cwd=BASE, capture_output=True, text=True, timeout=60
        )
        assert p.returncode == 0
        assert '--campaign' in p.stdout and '--timeout' in p.stdout
