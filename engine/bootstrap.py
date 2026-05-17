"""
engine/bootstrap.py

Tools for bootstrapping new systems, domains, and graph types without Python code.
Provides CLI commands to scaffold new components from templates.
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

import yaml

log = logging.getLogger(__name__)


class BootstrapManager:
    """Handles creation of new systems, domains, and graphs from templates."""

    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.templates_dir = base_dir / 'templates'
        self.systems_dir = base_dir / 'systems'
        self.domains_dir = base_dir / 'domains'
        self.graph_types_dir = base_dir / 'graph_types'

    def bootstrap_system(self, system_name: str, template: str = 'descriptor_sql_database.yaml') -> Path:
        """
        Create a new system directory from a template.

        Parameters
        ----------
        system_name : str
            Name of the new system (e.g., 'my_postgres', 'cassandra')
        template : str
            Template file name (default: 'descriptor_sql_database.yaml')

        Returns
        -------
        Path
            Path to the newly created system directory
        """
        system_dir = self.systems_dir / system_name
        if system_dir.exists():
            raise ValueError(f'System {system_name} already exists at {system_dir}')

        # Create directory structure
        system_dir.mkdir(parents=True, exist_ok=True)
        rules_dir = system_dir / 'rules'
        rules_dir.mkdir(exist_ok=True)

        # Copy descriptor template
        template_path = self.templates_dir / template
        if not template_path.exists():
            raise FileNotFoundError(f'Template not found: {template_path}')

        # Read template and update name
        with open(template_path) as f:
            descriptor = yaml.safe_load(f)

        descriptor['name'] = system_name
        descriptor_path = system_dir / 'descriptor.yaml'
        with open(descriptor_path, 'w') as f:
            yaml.dump(descriptor, f, default_flow_style=False, sort_keys=False)

        log.info(f'✓ Created system directory at {system_dir}')
        log.info(f'  Descriptor: {descriptor_path}')
        log.info(f'  Next steps:')
        log.info(f'    1. Edit {descriptor_path} to set protocol, timing_phases, etc.')
        log.info(f'    2. Create {system_dir}/credentials.yaml with connection details')
        log.info(f'    3. Add rule files to {rules_dir}')
        log.info(f'    4. Test: python transitive.py --systems {system_name} --graphs cycle --sizes 10 11 1')

        return system_dir

    def bootstrap_domain(self, domain_name: str, template: str = 'domain_shortest_path.yaml') -> Path:
        """
        Create a new domain descriptor from a template.

        Parameters
        ----------
        domain_name : str
            Name of the new domain (e.g., 'shortest_path', 'my_query_pattern')
        template : str
            Template file name (default: 'domain_shortest_path.yaml')

        Returns
        -------
        Path
            Path to the newly created domain directory
        """
        domain_dir = self.domains_dir / domain_name
        if domain_dir.exists():
            raise ValueError(f'Domain {domain_name} already exists at {domain_dir}')

        domain_dir.mkdir(parents=True, exist_ok=True)

        # Copy template
        template_path = self.templates_dir / template
        if not template_path.exists():
            raise FileNotFoundError(f'Template not found: {template_path}')

        with open(template_path) as f:
            domain = yaml.safe_load(f)

        domain['name'] = domain_name
        descriptor_path = domain_dir / 'descriptor.yaml'
        with open(descriptor_path, 'w') as f:
            yaml.dump(domain, f, default_flow_style=False, sort_keys=False)

        log.info(f'✓ Created domain directory at {domain_dir}')
        log.info(f'  Descriptor: {descriptor_path}')
        log.info(f'  Next steps:')
        log.info(f'    1. Edit {descriptor_path} to define query parameters and output schema')
        log.info(f'    2. Create rule files in systems/*/rules/{domain_name}_*.sql (or .lp, .cypher, etc.)')
        log.info(f'    3. Test: python transitive.py --domains {domain_name} --systems postgres')

        return domain_dir

    def bootstrap_graph(self, graph_name: str, generator_path: str, description: str = '') -> Path:
        """
        Create a new graph type descriptor.

        Parameters
        ----------
        graph_name : str
            Name of the graph type (e.g., 'my_hexagon_grid')
        generator_path : str
            Python dotted path to the generator method
            (e.g., 'engine.data_generator.DataGenerator.generate_my_hexagon_grid')
        description : str
            Human-readable description of the graph type

        Returns
        -------
        Path
            Path to the newly created graph descriptor
        """
        graph_file = self.graph_types_dir / f'{graph_name}.yaml'
        if graph_file.exists():
            raise ValueError(f'Graph {graph_name} already exists at {graph_file}')

        descriptor = {
            'name': graph_name,
            'display_name': graph_name.replace('_', ' ').title(),
            'description': description or f'Graph topology: {graph_name}',
            'generator': generator_path,
            'parameters': {},
        }

        with open(graph_file, 'w') as f:
            yaml.dump(descriptor, f, default_flow_style=False, sort_keys=False)

        log.info(f'✓ Created graph descriptor at {graph_file}')
        log.info(f'  Next steps:')
        log.info(f'    1. Implement the generator in engine/data_generator.py')
        log.info(f'    2. Test: python transitive.py --graphs {graph_name} --systems postgres --sizes 10 11 1')

        return graph_file

    def copy_rule_template(self, target_dir: Path, template_name: str) -> Path:
        """
        Copy a rule template to a target directory.

        Parameters
        ----------
        target_dir : Path
            Directory where the rule file should be placed
        template_name : str
            Template file name (e.g., 'rule_template_sql_right_recursion.sql')

        Returns
        -------
        Path
            Path to the copied rule file
        """
        template_path = self.templates_dir / template_name
        if not template_path.exists():
            raise FileNotFoundError(f'Template not found: {template_path}')

        target_path = target_dir / template_name.replace('rule_template_', '').replace('domain_', '')
        target_dir.mkdir(parents=True, exist_ok=True)

        shutil.copy(template_path, target_path)
        log.info(f'✓ Copied rule template to {target_path}')
        return target_path

    def list_templates(self) -> dict[str, list[str]]:
        """List all available templates by category."""
        descriptors = [f.name for f in self.templates_dir.glob('descriptor_*.yaml')]
        domains = [f.name for f in self.templates_dir.glob('domain_*.yaml')]
        rules = [f.name for f in self.templates_dir.glob('rule_template_*.{sql,lp,cypher,da}')]

        return {
            'system_descriptors': descriptors,
            'domain_templates': domains,
            'rule_templates': rules,
        }
