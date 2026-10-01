"""
engine/connectors/rdbms.py

Connectors for SQL relational databases:
  - PostgreSQLConnector  (protocol: psycopg2)
  - MariaDBConnector     (protocol: mysqlclient)
  - CockroachDBConnector (protocol: cockroachdb)
  - SingleStoreConnector (protocol: singlestore)

Each connector dynamically imports the mode-specific operation class from
the system's rules/ directory, preserving the existing query files unchanged.
The class is ``<class_prefix><Mode>Recursion`` from ``rules/<domain>_<mode>.py``
(e.g. ``MariaDBLeftRecursion`` from ``rules/transitive_left_recursion.py``), and the
system's ``__init__.py`` is importable as ``<module_prefix>`` (e.g. ``mariadb_rules``);
both prefixes come from the descriptor's ``flags``.

Every step of a run is timed separately (one TimingPhase each). A failing step is
recorded in ``self.errors`` (see BaseConnector) and the remaining steps are skipped.
"""

from __future__ import annotations

import importlib.util
import logging
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from engine.connectors.base import BaseConnector
from engine.memory import rss_of_pid, sampler_or_none

if TYPE_CHECKING:
    from engine.loader import SystemDescriptor

log = logging.getLogger(__name__)


def _dynamic_import_class(module_path: Path, class_name: str) -> type:
    """Import a class from an arbitrary file path at runtime."""
    spec = importlib.util.spec_from_file_location(module_path.stem, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_path.stem] = module
    spec.loader.exec_module(module)
    return getattr(module, class_name)


def _load_operations(
    rule_path: Path,
    descriptor: 'SystemDescriptor',
    default_class_prefix: str,
    default_module_prefix: str,
    config: dict[str, Any],
    query_bindings: dict[str, Any] | None,
    connection: Any,
) -> Any:
    """
    Import the system's shared operations module (systems/<name>/__init__.py) under the name the
    rule files import it by, then instantiate the rule's operation class.
    """
    class_prefix = descriptor.flags.get('class_prefix', default_class_prefix)
    module_prefix = descriptor.flags.get('module_prefix', default_module_prefix)
    # transitive_right_recursion -> Right; transitive_double_recursion -> Double
    mode_word = rule_path.stem.split('_', 1)[1].split('_')[0].capitalize()
    class_name = f'{class_prefix}{mode_word}Recursion'

    init_path = descriptor.system_dir / '__init__.py'
    if not init_path.exists():
        init_path = Path(module_prefix) / '__init__.py'
    spec = importlib.util.spec_from_file_location(module_prefix, init_path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_prefix] = mod
    spec.loader.exec_module(mod)

    OpClass = _dynamic_import_class(rule_path, class_name)

    # Pass query_bindings to the operations class via config
    config_with_bindings = {**config}
    if query_bindings:
        config_with_bindings['query_bindings'] = query_bindings
    return OpClass(config_with_bindings, connection)


def _mysql_kill_sessions(credentials: dict[str, Any], statement: str) -> None:
    """Kill all other sessions on the benchmark database (MariaDB/SingleStore cancellation)."""
    import MySQLdb

    conn = MySQLdb.connect(
        host=credentials.get('host', 'localhost'),
        port=int(credentials.get('port', 3306)),
        user=credentials.get('user', ''),
        passwd=credentials.get('password', ''),
    )
    try:
        cur = conn.cursor()
        cur.execute(
            'SELECT id FROM information_schema.processlist WHERE db = %s AND id <> CONNECTION_ID()',
            (credentials.get('database', ''),),
        )
        for (pid,) in cur.fetchall():
            try:
                cur.execute(f'{statement} {pid}')
            except Exception as e:  # the session may have ended in the meantime
                log.debug(f'{statement} {pid}: {e}')
    finally:
        conn.close()


# ────────────────────────────────────────────────────────────────────────────
# PostgreSQL
# ────────────────────────────────────────────────────────────────────────────


class PostgreSQLConnector(BaseConnector):
    """Runs transitive closure experiments on PostgreSQL via psycopg2."""

    def connect(self, credentials: dict[str, Any], descriptor: 'SystemDescriptor') -> None:
        import psycopg2

        db_url = credentials.get('dbURL', '')
        self._connection = psycopg2.connect(db_url)
        cur = self._connection.cursor()
        cur.execute('SELECT pg_backend_pid()')
        self._backend_pid = cur.fetchone()[0]
        self._connection.commit()
        log.info('PostgreSQL connected')

    def memory_sampler(self):
        """RSS of the backend process that executes this connection's queries (one per connection,
        hence per run). The recursive CTE is not parallelized, so it runs in this process."""
        return sampler_or_none(lambda: rss_of_pid(int(self._backend_pid)), 'postgres backend RSS')

    def run_experiment(
        self,
        rule_path: Path,
        input_path: Path,
        output_folder: Path,
        descriptor: 'SystemDescriptor',
        config: dict[str, Any],
        query_bindings: dict[str, Any] | None = None,
    ) -> dict[str, float]:
        ops = _load_operations(
            rule_path, descriptor, 'PostgreSQL', 'postgres_rules', config, query_bindings, self._connection
        )

        results_path = self.result_path(output_folder, descriptor, 'postgres_results.csv')
        phases = descriptor.timing_phases
        measurements: list[tuple[float, float]] = [(0.0, 0.0)] * len(phases)

        try:
            ops.drop_tc_path_tc_result_tables()
            measurements[0] = self.timed(ops.create_tc_path_table)[:2]
            measurements[1] = self.timed(ops.import_data_from_tsv, 'edge', str(input_path))[:2]
            measurements[2] = self.timed(ops.create_tc_path_index)[:2]
            measurements[3] = self.timed(ops.analyze_tc_path_table)[:2]
            measurements[4] = self.timed_query(ops.run_recursive_query)[:2]
            measurements[5] = self.timed(ops.export_transitive_closure_results, results_path)[:2]
            ops.drop_tc_path_tc_result_tables()
        except Exception as e:
            self._record_error(f'PostgreSQL experiment error: {e}')

        return self.build_timing_row(phases, measurements)

    @classmethod
    def cancel_running(cls, credentials: dict[str, Any], descriptor: 'SystemDescriptor') -> None:
        """Terminate every other backend on the benchmark database."""
        import psycopg2

        conn = psycopg2.connect(credentials.get('dbURL', ''))
        conn.autocommit = True
        try:
            conn.cursor().execute(
                'SELECT pg_terminate_backend(pid) FROM pg_stat_activity '
                'WHERE datname = current_database() AND pid <> pg_backend_pid()'
            )
        finally:
            conn.close()

    def close(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None


# ────────────────────────────────────────────────────────────────────────────
# MariaDB
# ────────────────────────────────────────────────────────────────────────────


class MariaDBConnector(BaseConnector):
    """
    Runs transitive closure experiments on MariaDB via mysqlclient.

    MariaDB writes query results with SELECT ... INTO OUTFILE on the *server* side; the rules
    write /tmp/mariadb_results.csv (see systems/mariadb/__init__.py), which only works when the
    server runs on the same machine as the benchmark. The file is moved to the output folder.
    """

    SERVER_OUTFILE = '/tmp/mariadb_results.csv'

    def connect(self, credentials: dict[str, Any], descriptor: 'SystemDescriptor') -> None:
        import MySQLdb

        self._connection = MySQLdb.connect(
            db=credentials.get('database', ''),
            user=credentials.get('user', ''),
            passwd=credentials.get('password', ''),
            host=credentials.get('host', 'localhost'),
            port=int(credentials.get('port', 3306)),
            local_infile=1,
        )
        self._credentials = credentials
        log.info('MariaDB connected')

    def memory_sampler(self):
        """MariaDB's own accounting of the memory allocated by this connection's thread
        (information_schema.PROCESSLIST.MEMORY_USED), read through a second connection. The RSS of
        the server process is not usable: the allocator keeps and reuses memory across runs."""

        def make_probe():
            import MySQLdb

            c = self._credentials
            thread = self._connection.thread_id()
            conn = MySQLdb.connect(host=c.get('host', 'localhost'), port=int(c.get('port', 3306)),
                                   user=c.get('user', ''), passwd=c.get('password', ''))
            conn.autocommit(True)

            def probe():
                cur = conn.cursor()
                cur.execute('SELECT MEMORY_USED FROM information_schema.PROCESSLIST WHERE ID = %s', (thread,))
                row = cur.fetchone()
                return float(row[0]) if row else None

            return probe

        return sampler_or_none(make_probe, 'mariadb connection MEMORY_USED')

    def run_experiment(
        self,
        rule_path: Path,
        input_path: Path,
        output_folder: Path,
        descriptor: 'SystemDescriptor',
        config: dict[str, Any],
        query_bindings: dict[str, Any] | None = None,
    ) -> dict[str, float]:
        ops = _load_operations(
            rule_path, descriptor, 'MariaDB', 'mariadb_rules', config, query_bindings, self._connection
        )

        results_path = self.result_path(output_folder, descriptor, 'mariadb_results.csv')
        phases = descriptor.timing_phases
        measurements: list[tuple[float, float]] = [(0.0, 0.0)] * len(phases)

        # INTO OUTFILE refuses to overwrite an existing file (left behind by a killed run)
        if os.path.exists(self.SERVER_OUTFILE):
            os.remove(self.SERVER_OUTFILE)

        try:
            ops.drop_tc_path_tc_result_tables()
            ops.set_standard_cte_to_zero()
            measurements[0] = self.timed(ops.create_tc_path_table)[:2]
            measurements[1] = self.timed(ops.import_data_from_file, 'edge', str(input_path))[:2]
            measurements[2] = self.timed(ops.create_tc_path_index)[:2]
            measurements[3] = self.timed(ops.analyze_tc_path_table)[:2]
            measurements[4] = self.timed_query(ops.run_recursive_query)[:2]
            measurements[5] = self.timed(ops.export_data_to_file)[:2]
            ops.drop_tc_path_tc_result_tables()
        except Exception as e:
            self._record_error(f'MariaDB experiment error: {e}')

        # Copy MariaDB's forced /tmp output to desired location
        try:
            shutil.copy(self.SERVER_OUTFILE, str(results_path))
            os.remove(self.SERVER_OUTFILE)
        except Exception as e:
            log.warning(f'MariaDB result copy: {e}')

        return self.build_timing_row(phases, measurements)

    @classmethod
    def cancel_running(cls, credentials: dict[str, Any], descriptor: 'SystemDescriptor') -> None:
        _mysql_kill_sessions(credentials, 'KILL')

    def close(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None


# ────────────────────────────────────────────────────────────────────────────
# SingleStore
# ────────────────────────────────────────────────────────────────────────────


class SingleStoreConnector(MariaDBConnector):
    """
    Runs transitive closure experiments on SingleStore (MySQL wire protocol, mysqlclient).

    Differences from MariaDB (details in systems/singlestore/__init__.py and docs/SYSTEMS.md):
      * the recursive CTE must use UNION ALL; duplicates are removed by an outer DISTINCT, so the
        query terminates only on acyclic graphs (cyclic graphs hit the iteration limit, error 2741);
      * the per-session iteration limit (default 32) is raised before each run;
      * the result is fetched through the client and written locally, because INTO OUTFILE would
        write inside the server (usually a container).

    No memory probe: SingleStore reports its memory only for the whole server (Total_server_memory),
    and identical runs showed increases that differed by a factor of 5 (memory kept across runs);
    see docs/VERIFICATION.md.
    """

    def memory_sampler(self):
        return None


    def run_experiment(
        self,
        rule_path: Path,
        input_path: Path,
        output_folder: Path,
        descriptor: 'SystemDescriptor',
        config: dict[str, Any],
        query_bindings: dict[str, Any] | None = None,
    ) -> dict[str, float]:
        ops = _load_operations(
            rule_path, descriptor, 'SingleStore', 'singlestore_rules', config, query_bindings, self._connection
        )

        results_path = self.result_path(output_folder, descriptor, 'singlestore_results.csv')
        phases = descriptor.timing_phases
        measurements: list[tuple[float, float]] = [(0.0, 0.0)] * len(phases)

        try:
            ops.drop_tc_path_tc_result_tables()
            ops.set_iteration_limit()
            measurements[0] = self.timed(ops.create_tc_path_table)[:2]
            measurements[1] = self.timed(ops.import_data_from_file, 'edge', str(input_path))[:2]
            measurements[2] = self.timed(ops.create_tc_path_index)[:2]
            measurements[3] = self.timed(ops.analyze_tc_path_table)[:2]
            measurements[4] = self.timed_query(ops.run_recursive_query)[:2]
            measurements[5] = self.timed(ops.export_data_to_file, results_path)[:2]
            ops.drop_tc_path_tc_result_tables()
        except Exception as e:
            self._record_error(f'SingleStore experiment error: {e}')

        return self.build_timing_row(phases, measurements)

    @classmethod
    def cancel_running(cls, credentials: dict[str, Any], descriptor: 'SystemDescriptor') -> None:
        # SingleStore's KILL <id> closes the connection but may leave the query running; KILL QUERY
        # stops the query itself.
        _mysql_kill_sessions(credentials, 'KILL QUERY')


# ────────────────────────────────────────────────────────────────────────────
# CockroachDB
# ────────────────────────────────────────────────────────────────────────────


class CockroachDBConnector(BaseConnector):
    """
    Runs transitive closure experiments on CockroachDB via psycopg2.

    CockroachDB reads and writes files only through its external I/O directory
    (``cockroach start --external-io-dir``), configured as ``externalDirectory`` in the
    credentials. The input is copied there for IMPORT INTO, and EXPORT INTO CSV writes the result
    to ``<externalDirectory>/tmp/`` as one or more chunk files, which are concatenated into the
    output folder.
    """

    def connect(self, credentials: dict[str, Any], descriptor: 'SystemDescriptor') -> None:
        import psycopg2

        db_url = credentials.get('dbURL', '')
        self._connection = psycopg2.connect(db_url)
        self._http_url = credentials.get('httpURL', 'http://localhost:8080')
        log.info('CockroachDB connected')

    def memory_sampler(self):
        """CockroachDB's own accounting of SQL memory (the root SQL memory monitor, which
        --max-sql-memory limits), polled from the node's metrics endpoint (credentials: httpURL,
        default http://localhost:8080). The RSS of the Go server process is not usable: the Go
        runtime keeps freed heap across runs."""

        def make_probe():
            import urllib.request

            url = self._http_url.rstrip('/') + '/_status/vars'

            def probe():
                with urllib.request.urlopen(url, timeout=2) as resp:
                    for line in resp.read().decode().splitlines():
                        if line.startswith('sql_mem_root_current'):
                            return float(line.rsplit(' ', 1)[1])
                return None

            probe()  # fail now (no sampler) if the endpoint is unreachable
            return probe

        return sampler_or_none(make_probe, 'cockroachdb sql_mem_root_current', interval=0.05)

    def run_experiment(
        self,
        rule_path: Path,
        input_path: Path,
        output_folder: Path,
        descriptor: 'SystemDescriptor',
        config: dict[str, Any],
        query_bindings: dict[str, Any] | None = None,
    ) -> dict[str, float]:
        ops = _load_operations(
            rule_path, descriptor, 'CockroachDB', 'cockroachdb_rules', config, query_bindings, self._connection
        )

        results_path = self.result_path(output_folder, descriptor, 'cockroachdb_results.csv')
        external_dir = descriptor.credentials.get(
            'externalDirectory', config.get('cockroachdb', {}).get('externalDirectory', '')
        )
        export_dir = Path(os.path.expanduser(external_dir)) / 'tmp'
        phases = descriptor.timing_phases
        measurements: list[tuple[float, float]] = [(0.0, 0.0)] * len(phases)

        try:
            ops.drop_tc_path_tc_result_tables()
            # chunks left by an earlier (killed) run would otherwise be concatenated into this result
            shutil.rmtree(export_dir, ignore_errors=True)
            measurements[0] = self.timed(ops.create_tc_path_table)[:2]

            cmd = f'mkdir -p {external_dir} && cp {input_path} {external_dir}'
            subprocess.run(cmd, shell=True, check=True, capture_output=True)
            filename = f'{input_path.stem}{input_path.suffix}'
            measurements[1] = self.timed(ops.import_data_from_tsv, 'edge', filename)[:2]
            subprocess.run(f'rm -r {external_dir}{filename}', shell=True, capture_output=True)

            measurements[2] = self.timed(ops.create_tc_path_index)[:2]
            measurements[3] = self.timed(ops.analyze_tc_path_table)[:2]
            measurements[4] = self.timed_query(ops.run_recursive_query)[:2]
            measurements[5] = self.timed(ops.export_transitive_closure_results, results_path)[:2]
            ops.drop_tc_path_tc_result_tables()

            # EXPORT INTO CSV may split the result into several chunk files; concatenate all of
            # them (a plain `cp *.csv file` fails when there is more than one chunk), then remove
            # the export directory so chunks cannot accumulate across runs.
            concatenate_chunks(export_dir, results_path)
        except Exception as e:
            self._record_error(f'CockroachDB experiment error: {e}')
        finally:
            shutil.rmtree(export_dir, ignore_errors=True)

        return self.build_timing_row(phases, measurements)

    @classmethod
    def cancel_running(cls, credentials: dict[str, Any], descriptor: 'SystemDescriptor', wait_s: int = 600) -> None:
        """
        Cancel running queries, then the schema-change jobs they leave behind.

        The recursive query runs as CREATE TABLE ... AS, which CockroachDB executes as a
        schema-change *job*: it survives the client and even CANCEL QUERY, and while it runs the
        table is in the state "being added", so the next run fails when it drops the tables.
        Only plain SCHEMA CHANGE jobs are cancelable (a bulk CANCEL JOBS that also selects
        SCHEMA CHANGE GC jobs fails as a whole), so they are cancelled one by one until the
        tables can be dropped.
        """
        import psycopg2

        conn = psycopg2.connect(credentials.get('dbURL', ''))
        conn.autocommit = True
        try:
            cur = conn.cursor()
            cur.execute("SELECT query_id FROM [SHOW CLUSTER QUERIES] WHERE query NOT LIKE '%CLUSTER QUERIES%'")
            for (qid,) in cur.fetchall():
                cur.execute(f"CANCEL QUERY '{qid}'")
            deadline = time.time() + wait_s
            while time.time() < deadline:
                cur.execute(
                    "SELECT job_id FROM [SHOW JOBS] WHERE status IN ('running', 'pending', 'paused') "
                    "AND job_type = 'SCHEMA CHANGE'"
                )
                for (job,) in cur.fetchall():
                    try:
                        cur.execute(f'CANCEL JOB {job}')
                    except Exception as e:
                        log.debug(f'CANCEL JOB {job}: {e}')
                try:
                    cur.execute('DROP TABLE IF EXISTS tc_result, edge')
                    break
                except Exception:
                    time.sleep(5)
        finally:
            conn.close()

    def close(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None


def concatenate_chunks(export_dir: Path, results_path: Path) -> int:
    """Concatenate all CSV chunk files of an EXPORT (sorted by name) into one file; returns #chunks."""
    chunks = sorted(export_dir.glob('*.csv'))
    if not chunks:
        raise FileNotFoundError(f'EXPORT wrote no CSV files to {export_dir}')
    with open(results_path, 'wb') as out:
        for chunk in chunks:
            with open(chunk, 'rb') as f:
                shutil.copyfileobj(f, out)
    return len(chunks)
