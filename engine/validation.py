"""
engine/validation.py

Validation tools for custom rule files and system configurations.
Checks for common errors without running actual queries.
"""

from __future__ import annotations

import logging
from pathlib import Path

import yaml

from engine.loader import DescriptorLoader, SystemDescriptor

log = logging.getLogger(__name__)


class RuleValidator:
    """Validates rule files and system configurations."""

    def __init__(self, base_dir: Path):
        self.base_dir = Path(base_dir)
        self.loader = DescriptorLoader(base_dir=self.base_dir)

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

        # Load descriptor
        try:
            systems = self.loader.load_systems(names=[system_name])
            if not systems:
                log.error(f'System {system_name} not found.')
                return False
            descriptor = systems[0]
        except Exception as e:
            log.error(f'Failed to load system descriptor: {e}')
            return False

        # Check descriptor validity
        if not self._check_descriptor(descriptor):
            return False

        # Check rule files
        if not self._check_rule_files(descriptor):
            return False

        # Check credentials if required
        if not self._check_credentials(descriptor):
            return False

        log.info(f'✓ System {system_name} passed validation')
        return True

    def _check_descriptor(self, descriptor: SystemDescriptor) -> bool:
        """Check that descriptor is well-formed."""
        log.info('  Checking descriptor...')

        checks = [
            (descriptor.name, f'name is empty'),
            (descriptor.display_name, f'display_name is empty'),
            (descriptor.protocol, f'protocol is empty'),
            (descriptor.timing_phases, f'timing_phases is empty'),
            (descriptor.modes, f'modes is empty'),
            (descriptor.rule_extension, f'rule_extension is empty'),
        ]

        all_pass = True
        for value, error_msg in checks:
            if not value:
                log.error(f'    ✗ {error_msg}')
                all_pass = False

        if all_pass:
            log.info(f'    ✓ Descriptor is well-formed')

        return all_pass

    def _check_rule_files(self, descriptor: SystemDescriptor) -> bool:
        """Check that rule files exist for all modes."""
        log.info(f'  Checking rule files...')

        rules_dir = descriptor.system_dir / 'rules'
        if not rules_dir.exists():
            log.error(f'    ✗ Rules directory not found: {rules_dir}')
            return False

        all_exist = True
        for mode in descriptor.modes:
            # Try common domain names
            for domain in ['transitive', 'shortest_path', 'reachability', 'same_generation']:
                rule_file = rules_dir / f'{domain}_{mode}{descriptor.rule_extension}'
                if rule_file.exists():
                    log.info(f'    ✓ Found: {rule_file.name}')
                    break
            else:
                # Fallback: look for mode-only file
                rule_file = rules_dir / f'{mode}{descriptor.rule_extension}'
                if rule_file.exists():
                    log.info(f'    ✓ Found: {rule_file.name}')
                else:
                    log.warning(
                        f'    ⚠ No rule file for mode {mode} (expected: {domain}_{mode}{descriptor.rule_extension})'
                    )
                    all_exist = False

        return all_exist

    def _check_credentials(self, descriptor: SystemDescriptor) -> bool:
        """Check that credentials file exists if required."""
        if not descriptor.flags.get('requires_credentials', False):
            log.info(f'  Credentials not required')
            return True

        log.info(f'  Checking credentials...')
        creds_file = descriptor.system_dir / 'credentials.yaml'

        if not creds_file.exists():
            log.warning(f'    ⚠ Credentials file not found: {creds_file}')
            log.info(f'      Create it with: echo "dbURL: ..." > {creds_file}')
            return False

        log.info(f'    ✓ Credentials file found')
        return True

    def validate_rule_file(self, rule_path: Path) -> bool:
        """
        Basic syntax check for a rule file.

        Parameters
        ----------
        rule_path : Path
            Path to the rule file

        Returns
        -------
        bool
            True if basic checks pass
        """
        log.info(f'Validating rule file: {rule_path}')

        if not rule_path.exists():
            log.error(f'Rule file not found: {rule_path}')
            return False

        # Check for common syntax issues
        with open(rule_path) as f:
            content = f.read()

        # File should not be empty
        if not content.strip():
            log.error(f'Rule file is empty')
            return False

        # Check for balanced parentheses/brackets
        if not self._check_balanced(content, '(', ')'):
            log.error(f'Unbalanced parentheses')
            return False

        if not self._check_balanced(content, '[', ']'):
            log.error(f'Unbalanced square brackets')
            return False

        # Language-specific checks
        ext = rule_path.suffix.lower()
        if ext == '.sql':
            if 'select' not in content.lower():
                log.warning(f'SQL file should contain SELECT statement')
        elif ext == '.lp':
            if ':- ' not in content and ':-' not in content:
                log.warning(f'Datalog file should contain rules (:-)')
        elif ext == '.cypher':
            if 'match' not in content.lower():
                log.warning(f'Cypher file should contain MATCH clause')

        log.info(f'✓ Rule file passed basic checks')
        return True

    @staticmethod
    def _check_balanced(text: str, open_char: str, close_char: str) -> bool:
        """Check that pairs of characters are balanced."""
        count = 0
        for char in text:
            if char == open_char:
                count += 1
            elif char == close_char:
                count -= 1
            if count < 0:
                return False
        return count == 0
