"""
engine/validation.py

Validation tools for custom rule files and system configurations.
Checks for common errors without running actual queries.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from engine.loader import DomainDescriptor, SystemDescriptor

log = logging.getLogger(__name__)


class RuleValidator:
    """Validates rule files and system configurations."""

    def __init__(self, base_dir: Path):
        self.base_dir = Path(base_dir)
        from engine.loader import DescriptorLoader

        self.loader = DescriptorLoader(base_dir=self.base_dir)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate_system(self, system_name: str) -> bool:
        """
        Validate all rule files for a given system.

        Parameters
        ----------
        system_name : str
            Name of the system to validate

        Returns
        -------
        bool
            True if all checks pass, False otherwise
        """
        log.info(f'Validating system: {system_name}')

        try:
            systems = self.loader.load_systems(names=[system_name])
            if not systems:
                log.error(f'System {system_name} not found.')
                return False
            descriptor = systems[0]
        except Exception as e:
            log.error(f'Failed to load system descriptor: {e}')
            return False

        if not self._check_descriptor(descriptor):
            return False
        if not self._check_rule_files(descriptor):
            return False
        if not self._check_credentials(descriptor):
            return False

        log.info(f'✓ System {system_name} passed validation')
        return True

    def validate_domain(self, domain_name: str, system_names: list[str] | None = None) -> bool:
        """
        Validate that every (requested) system has rule files for all modes
        declared in the domain descriptor.

        Parameters
        ----------
        domain_name : str
            Name of the domain  (domains/<name>/descriptor.yaml must exist)
        system_names : list[str] | None
            Systems to check; None → check all discovered systems.

        Returns
        -------
        bool
            True if all checks pass, False otherwise
        """
        log.info(f'Validating domain: {domain_name}')

        domain = self.loader.get_domain(domain_name)
        if domain is None:
            log.error(
                f'Domain "{domain_name}" not found. '
                f'Expected: domains/{domain_name}/descriptor.yaml'
            )
            return False

        log.info(f'  Domain modes: {domain.modes}')

        systems = self.loader.load_systems(names=system_names)
        if not systems:
            log.warning('  No systems found to validate against.')
            return True

        all_pass = True
        for system in systems:
            ok = self._check_domain_rule_files(system, domain)
            if not ok:
                all_pass = False

        if all_pass:
            log.info(f'✓ Domain "{domain_name}" validated against {len(systems)} system(s)')
        else:
            log.warning(f'⚠ Domain "{domain_name}" has missing rule files (see above)')

        return all_pass

    def test_rule_file(self, rule_path: Path, system_name: str | None = None) -> bool:
        """
        Test a single rule file: static syntax check + optional live dry-run.

        Performs language-specific analysis and, for SQL systems where credentials
        are available, an EXPLAIN-level dry-run to catch parse errors without
        executing any writes.

        Parameters
        ----------
        rule_path : Path
            Path to the rule file.
        system_name : str | None
            If given, also attempt a live dry-run using that system's connector.

        Returns
        -------
        bool
            True if all checks pass.
        """
        rule_path = Path(rule_path)
        log.info(f'Testing rule file: {rule_path}')

        if not rule_path.exists():
            log.error(f'  ✗ File not found: {rule_path}')
            return False

        content = rule_path.read_text()
        if not content.strip():
            log.error(f'  ✗ Rule file is empty')
            return False

        log.info(f'  ✓ File exists ({len(content)} bytes)')

        ext = rule_path.suffix.lower()
        static_ok = self._static_check(rule_path, content, ext)

        live_ok = True
        if system_name:
            live_ok = self._live_dry_run(rule_path, content, ext, system_name)

        overall = static_ok and live_ok
        if overall:
            log.info(f'✓ Rule file {rule_path.name} passed all checks')
        else:
            log.error(f'✗ Rule file {rule_path.name} has issues (see above)')

        return overall

    # ------------------------------------------------------------------
    # Private – descriptor / rule-file checks
    # ------------------------------------------------------------------

    def _check_descriptor(self, descriptor: 'SystemDescriptor') -> bool:
        """Check that descriptor is well-formed."""
        log.info('  Checking descriptor...')

        checks = [
            (descriptor.name, 'name is empty'),
            (descriptor.display_name, 'display_name is empty'),
            (descriptor.protocol, 'protocol is empty'),
            (descriptor.timing_phases, 'timing_phases is empty'),
            (descriptor.modes, 'modes is empty'),
            (descriptor.rule_extension, 'rule_extension is empty'),
        ]

        all_pass = True
        for value, error_msg in checks:
            if not value:
                log.error(f'    ✗ {error_msg}')
                all_pass = False

        if all_pass:
            log.info('    ✓ Descriptor is well-formed')

        return all_pass

    def _check_rule_files(self, descriptor: 'SystemDescriptor') -> bool:
        """Check that rule files exist for all modes."""
        log.info('  Checking rule files...')

        rules_dir = descriptor.system_dir / 'rules'
        if not rules_dir.exists():
            log.error(f'    ✗ Rules directory not found: {rules_dir}')
            return False

        all_exist = True
        for mode in descriptor.modes:
            for domain in ['transitive', 'shortest_path', 'reachability', 'same_generation']:
                rule_file = rules_dir / f'{domain}_{mode}{descriptor.rule_extension}'
                if rule_file.exists():
                    log.info(f'    ✓ Found: {rule_file.name}')
                    break
            else:
                rule_file = rules_dir / f'{mode}{descriptor.rule_extension}'
                if rule_file.exists():
                    log.info(f'    ✓ Found: {rule_file.name}')
                else:
                    log.warning(
                        f'    ⚠ No rule file for mode {mode} '
                        f'(expected: transitive_{mode}{descriptor.rule_extension})'
                    )
                    all_exist = False

        return all_exist

    def _check_domain_rule_files(self, descriptor: 'SystemDescriptor', domain: 'DomainDescriptor') -> bool:
        """Check that a system has rule files for every mode in a domain."""
        log.info(f'  Checking {descriptor.name} for domain "{domain.name}" ...')
        rules_dir = descriptor.system_dir / 'rules'
        if not rules_dir.exists():
            log.error(f'    ✗ Rules directory not found: {rules_dir}')
            return False

        all_exist = True
        for mode in domain.modes:
            rule_file = rules_dir / f'{domain.name}_{mode}{descriptor.rule_extension}'
            if rule_file.exists():
                log.info(f'    ✓ {descriptor.name}: {rule_file.name}')
            else:
                log.warning(f'    ⚠ {descriptor.name}: missing {rule_file.name}')
                all_exist = False
        return all_exist

    def _check_credentials(self, descriptor: 'SystemDescriptor') -> bool:
        """Check that credentials file exists if required."""
        if not descriptor.flags.get('requires_credentials', False):
            log.info('  Credentials not required')
            return True

        log.info('  Checking credentials...')
        creds_file = descriptor.system_dir / 'credentials.yaml'

        if not creds_file.exists():
            log.warning(f'    ⚠ Credentials file not found: {creds_file}')
            log.info(f'      Create it with: echo "dbURL: ..." > {creds_file}')
            return False

        log.info('    ✓ Credentials file found')
        return True

    # ------------------------------------------------------------------
    # Private – static syntax analysis
    # ------------------------------------------------------------------

    def _static_check(self, rule_path: Path, content: str, ext: str) -> bool:
        ok = True

        if not self._check_balanced(content, '(', ')'):
            log.error('  ✗ Unbalanced parentheses')
            ok = False
        if not self._check_balanced(content, '[', ']'):
            log.error('  ✗ Unbalanced square brackets')
            ok = False

        if ext == '.sql':
            ok = self._check_sql(content) and ok
        elif ext in ('.lp', '.dl'):
            ok = self._check_datalog(content) and ok
        elif ext == '.cypher':
            ok = self._check_cypher(content) and ok
        elif ext in ('.pl', '.P'):
            ok = self._check_prolog(content) and ok
        elif ext == '.da':
            ok = self._check_distalgo(content) and ok
        elif ext == '.js':
            ok = self._check_javascript(content) and ok

        if ok:
            log.info(f'  ✓ Static syntax checks passed ({ext})')

        return ok

    def _check_sql(self, content: str) -> bool:
        lower = content.lower()
        ok = True
        if 'select' not in lower:
            log.warning('  ⚠ SQL file has no SELECT — is this intentional?')
        if 'recursive' in lower and 'union' not in lower:
            log.warning('  ⚠ WITH RECURSIVE without UNION — recursion may not terminate correctly')
        single_count = content.count("'") - content.count("\\'")
        if single_count % 2 != 0:
            log.error("  ✗ Unclosed single-quoted string literal")
            ok = False
        return ok

    def _check_datalog(self, content: str) -> bool:
        has_rule = ':- ' in content or ':-\n' in content or ':-\t' in content
        has_fact = any(line.strip().endswith('.') for line in content.splitlines() if line.strip())
        if not has_rule and not has_fact:
            log.warning('  ⚠ No Datalog rules (:-) or facts found')
        if '#show' not in content and '.output' not in content:
            log.warning('  ⚠ No output directive (#show or .output) — results may be empty')
        return True

    def _check_cypher(self, content: str) -> bool:
        lower = content.lower()
        if 'match' not in lower:
            log.warning('  ⚠ Cypher file has no MATCH clause')
        if 'return' not in lower:
            log.warning('  ⚠ Cypher file has no RETURN clause — query may not output anything')
        return True

    def _check_prolog(self, content: str) -> bool:
        if ':-' not in content:
            log.warning('  ⚠ Prolog file has no rules (:-)')
        return True

    def _check_distalgo(self, content: str) -> bool:
        if 'def ' not in content:
            log.warning('  ⚠ DistAlgo file has no function definitions')
        return True

    def _check_javascript(self, content: str) -> bool:
        ok = True
        if not self._check_balanced(content, '{', '}'):
            log.error('  ✗ Unbalanced curly braces')
            ok = False
        if 'function' not in content and '=>' not in content and 'db.' not in content:
            log.warning('  ⚠ JS file has no function or db. call — may not execute correctly')
        return ok

    # ------------------------------------------------------------------
    # Private – live dry-run
    # ------------------------------------------------------------------

    def _live_dry_run(self, rule_path: Path, content: str, ext: str, system_name: str) -> bool:
        descriptor = self.loader.get_system(system_name)
        if descriptor is None:
            log.warning(f'  ⚠ System "{system_name}" not found — skipping live dry-run')
            return True

        protocol = descriptor.protocol

        if protocol in ('psycopg2', 'cockroachdb'):
            return self._dry_run_postgres(content, descriptor.credentials)
        elif protocol == 'duckdb':
            return self._dry_run_duckdb(content, descriptor.credentials)
        else:
            log.info(f'  ℹ Live dry-run not supported for protocol "{protocol}" — skipping')
            return True

    def _dry_run_postgres(self, content: str, credentials: dict) -> bool:
        try:
            import psycopg2

            conn = psycopg2.connect(credentials.get('dbURL', ''))
            conn.autocommit = False
            cur = conn.cursor()
            try:
                cur.execute('BEGIN')
                stmt = content.strip().rstrip(';')
                cur.execute(f'EXPLAIN {stmt}')
                log.info('  ✓ PostgreSQL parse check passed (EXPLAIN succeeded)')
                result = True
            except Exception as e:
                log.error(f'  ✗ PostgreSQL parse error: {e}')
                result = False
            finally:
                conn.rollback()
                conn.close()
            return result
        except ImportError:
            log.warning('  ⚠ psycopg2 not installed — skipping PostgreSQL dry-run')
            return True
        except Exception as e:
            log.warning(f'  ⚠ Could not connect to PostgreSQL for dry-run: {e}')
            return True

    def _dry_run_duckdb(self, content: str, credentials: dict) -> bool:
        try:
            import duckdb

            db_path = credentials.get('database', ':memory:')
            conn = duckdb.connect(':memory:')
            try:
                stmt = content.strip().rstrip(';')
                conn.execute(f'EXPLAIN {stmt}')
                log.info('  ✓ DuckDB parse check passed (EXPLAIN succeeded)')
                result = True
            except Exception as e:
                log.error(f'  ✗ DuckDB parse error: {e}')
                result = False
            finally:
                conn.close()
            return result
        except ImportError:
            log.warning('  ⚠ duckdb not installed — skipping DuckDB dry-run')
            return True
        except Exception as e:
            log.warning(f'  ⚠ Could not connect to DuckDB for dry-run: {e}')
            return True

    # ------------------------------------------------------------------
    # Shared utilities
    # ------------------------------------------------------------------

    def validate_rule_file(self, rule_path: Path) -> bool:
        """Legacy API — delegates to test_rule_file."""
        return self.test_rule_file(rule_path)

    @staticmethod
    def _check_balanced(text: str, open_char: str, close_char: str) -> bool:
        """Check that pairs of characters are balanced (string-literal aware for ')."""
        count = 0
        in_string = False
        prev = ''
        for char in text:
            if char == "'" and prev != '\\':
                in_string = not in_string
            if not in_string:
                if char == open_char:
                    count += 1
                elif char == close_char:
                    count -= 1
                if count < 0:
                    return False
            prev = char
        return count == 0
