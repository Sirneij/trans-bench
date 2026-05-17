"""
engine/loader.py

Scans the filesystem for system descriptors (systems/*/descriptor.yaml) and
graph type descriptors (graph_types/*.yaml), parses them into typed Python
objects, and optionally merges legacy config.json credentials.

No Python source edits required to add new systems — just drop a descriptor.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import yaml

import subprocess
import importlib.metadata

log = logging.getLogger(__name__)


def get_system_version(sys_name: str) -> str:
    cmd_map = {
        'clingo': ['clingo', '--version'],
        'souffle': ['souffle', '--version'],
        'xsb': ['xsb', '--version'],
        'mariadb': ['mariadb', '--version'],
        'postgres': ['psql', '--version'],
        'duckdb': ['duckdb', '--version'],
        'cockroachdb': ['cockroach', 'version'],
        'mongodb': ['mongod', '--version'],
        'neo4j': ['neo4j-admin', '--version'],
        'alda': ['alda', '--version']
    }
    
    version = None
    if sys_name in cmd_map:
        try:
            res = subprocess.run(cmd_map[sys_name], capture_output=True, text=True, timeout=2)
            if res.returncode == 0:
                out = res.stdout.strip()
                if not out:
                    out = res.stderr.strip()
                lines = [l.strip() for l in out.split('\n') if l.strip()]
                log.info(f"DEBUG: {sys_name} version output: {lines}")
                if sys_name == 'souffle':
                    for l in lines:
                        if l.startswith('Version:'):
                            version = l.replace('Version:', '').strip()
                            break
                elif sys_name == 'xsb':
                    for l in lines:
                        if l.startswith('XSB Version'):
                            version = l.replace('XSB Version', '').strip()
                            break
                elif sys_name == 'postgres':
                    for l in lines:
                        if 'psql (PostgreSQL)' in l:
                            version = l.replace('psql (PostgreSQL)', '').strip()
                            break
                elif sys_name == 'mariadb':
                    for l in lines:
                        if 'mariadb from' in l:
                            version = l.split('from')[1].split(',')[0].strip()
                            break
                elif sys_name == 'cockroachdb':
                    for l in lines:
                        if l.startswith('Build Tag:'):
                            version = l.replace('Build Tag:', '').strip()
                            break
                elif sys_name == 'mongodb':
                    for l in lines:
                        if l.startswith('db version'):
                            version = l.replace('db version', '').strip()
                            break
                else:
                    version = lines[0] if lines else 'Unknown'
        except Exception:
            pass

    if version and version != 'Unknown' and not version.startswith('---'):
        return version

    pkg = None
    if sys_name == 'neo4j': pkg = 'neo4j'
    elif sys_name in ('postgres', 'cockroachdb'): pkg = 'psycopg2'
    elif sys_name == 'mongodb': pkg = 'pymongo'
    elif sys_name == 'duckdb': pkg = 'duckdb'
    elif sys_name == 'mariadb': pkg = 'mariadb'
    elif sys_name == 'clingo': pkg = 'clingo'

    if pkg:
        try:
            return importlib.metadata.version(pkg)
        except Exception:
            pass

    return 'Unknown'


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class TimingPhase:
    """One timed section in an experiment run (maps to two CSV columns)."""

    id: str  # snake_case key used internally
    label: str  # CamelCase used in CSV headers  (→ {label}RealTime, {label}CPUTime)


@dataclass
class SystemDescriptor:
    """Everything the engine needs to know about a benchmarked system."""

    name: str
    display_name: str
    category: str  # db | logic | hybrid
    protocol: str  # connector type: psycopg2 | mysqlclient | duckdb | neo4j | pymongo |
    #                 subprocess | clingo_python | souffle_subprocess | alda_subprocess
    timing_phases: list[TimingPhase]
    input_format: str  # tsv | lp | facts | pickle
    modes: list[str]
    rule_extension: str
    flags: dict[str, Any]
    execution: dict[str, Any]  # protocol-specific execution config
    descriptor_path: Path
    rules_dir: Path
    credentials: dict[str, Any]
    enabled: bool = True
    version: str = "Unknown"

    @property
    def csv_headers(self) -> list[str]:
        """Auto-generated CSV column headers from timing phases."""
        headers: list[str] = []
        for phase in self.timing_phases:
            headers.append(f'{phase.label}RealTime')
            headers.append(f'{phase.label}CPUTime')
            headers.append(f'{phase.label}MaxRAM_MB')
        return headers

    @property
    def system_dir(self) -> Path:
        return self.descriptor_path.parent

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'display_name': self.display_name,
            'category': self.category,
            'protocol': self.protocol,
            'timing_phases': [{'id': p.id, 'label': p.label} for p in self.timing_phases],
            'input_format': self.input_format,
            'modes': self.modes,
            'rule_extension': self.rule_extension,
            'flags': self.flags,
            'execution': self.execution,
            'enabled': self.enabled,
            'version': self.version,
            'csv_headers': self.csv_headers,
        }


@dataclass
class GraphTypeDescriptor:
    """Describes a graph topology that the benchmark can generate."""

    name: str
    display_name: str
    description: str
    generator: str  # dotted Python path to a DataGenerator method
    parameters: dict[str, Any]

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'display_name': self.display_name,
            'description': self.description,
            'generator': self.generator,
            'parameters': self.parameters,
        }


@dataclass
class QueryParameter:
    """A single query parameter declared in a domain descriptor."""

    name: str
    type: str  # int | float | str
    required: bool = True
    default: Any = None
    description: str = ''


@dataclass
class DomainDescriptor:
    """
    Describes a query domain (e.g. transitive, shortest_path, reachability).

    Loaded from  domains/<name>/descriptor.yaml  at runtime.
    The engine uses this to:
      - Validate that rule files exist for the declared modes
      - Supply query parameter defaults
      - Document data-generation requirements
    """

    name: str
    display_name: str
    description: str
    category: str
    modes: list[str]  # valid recursion modes for this domain
    query_parameters: list[QueryParameter]
    output_schema: list[dict[str, str]]
    data_requirements: dict[str, Any]
    descriptor_path: Path

    @property
    def domain_dir(self) -> Path:
        return self.descriptor_path.parent

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'display_name': self.display_name,
            'description': self.description,
            'category': self.category,
            'modes': self.modes,
            'query_parameters': [
                {
                    'name': p.name,
                    'type': p.type,
                    'required': p.required,
                    'default': p.default,
                    'description': p.description,
                }
                for p in self.query_parameters
            ],
            'output_schema': self.output_schema,
            'data_requirements': self.data_requirements,
        }


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


class DescriptorLoader:
    """
    Discovers and loads system/graph-type descriptors from the filesystem.

    Directory layout expected:
        <base_dir>/systems/<system_name>/descriptor.yaml
        <base_dir>/systems/<system_name>/credentials.yaml   (optional, gitignored)
        <base_dir>/graph_types/<graph_name>.yaml
    """

    def __init__(self, base_dir: Optional[Path] = None, config_path: Optional[Path] = None):
        self.base_dir = Path(base_dir) if base_dir else Path(__file__).parent.parent
        self.config_path = config_path or (self.base_dir / 'config.yaml')
        self._global_config: Optional[dict] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load_systems(self, names: Optional[list[str]] = None) -> list[SystemDescriptor]:
        """Load all (or specific) system descriptors."""
        systems_dir = self.base_dir / 'systems'
        descriptors: list[SystemDescriptor] = []

        if not systems_dir.exists():
            log.warning(f'systems/ directory not found at {systems_dir}')
            return descriptors

        for desc_file in sorted(systems_dir.glob('*/descriptor.yaml')):
            try:
                descriptor = self._parse_system(desc_file)
                if names and descriptor.name not in names:
                    continue
                descriptors.append(descriptor)
                log.info(f'Loaded system descriptor: {descriptor.name}')
            except Exception as e:
                log.error(f'Failed to load descriptor {desc_file}: {e}')

        return descriptors

    def load_graph_types(self, names: Optional[list[str]] = None) -> list[GraphTypeDescriptor]:
        """Load all (or specific) graph type descriptors."""
        graph_types_dir = self.base_dir / 'graph_types'
        descriptors: list[GraphTypeDescriptor] = []

        if not graph_types_dir.exists():
            log.warning(f'graph_types/ directory not found at {graph_types_dir}')
            return descriptors

        for desc_file in sorted(graph_types_dir.glob('*.yaml')):
            try:
                descriptor = self._parse_graph_type(desc_file)
                if names and descriptor.name not in names:
                    continue
                descriptors.append(descriptor)
                log.info(f'Loaded graph type descriptor: {descriptor.name}')
            except Exception as e:
                log.error(f'Failed to load graph type {desc_file}: {e}')

        return descriptors

    def load_domains(self, names: Optional[list[str]] = None) -> list[DomainDescriptor]:
        """Load all (or specific) domain descriptors from domains/*/descriptor.yaml."""
        domains_dir = self.base_dir / 'domains'
        descriptors: list[DomainDescriptor] = []

        if not domains_dir.exists():
            log.debug(f'domains/ directory not found at {domains_dir} — no custom domains loaded')
            return descriptors

        for desc_file in sorted(domains_dir.glob('*/descriptor.yaml')):
            try:
                descriptor = self._parse_domain(desc_file)
                if names and descriptor.name not in names:
                    continue
                descriptors.append(descriptor)
                log.info(f'Loaded domain descriptor: {descriptor.name}')
            except Exception as e:
                log.error(f'Failed to load domain {desc_file}: {e}')

        return descriptors

    def get_domain(self, name: str) -> Optional[DomainDescriptor]:
        """Load a single domain descriptor by name."""
        desc_file = self.base_dir / 'domains' / name / 'descriptor.yaml'
        if not desc_file.exists():
            return None
        return self._parse_domain(desc_file)

    def load_global_config(self) -> dict:
        """Load config.yaml (or config.json as fallback)."""
        if self._global_config is not None:
            return self._global_config

        if self.config_path.exists() and self.config_path.suffix == '.yaml':
            with open(self.config_path) as f:
                self._global_config = yaml.safe_load(f) or {}
        else:
            # Fallback: legacy config.json
            legacy_path = self.base_dir / 'config.json'
            if legacy_path.exists():
                with open(legacy_path) as f:
                    self._global_config = json.load(f)
            else:
                self._global_config = {}

        return self._global_config

    def get_system(self, name: str) -> Optional[SystemDescriptor]:
        """Load a single system descriptor by name."""
        desc_file = self.base_dir / 'systems' / name / 'descriptor.yaml'
        if not desc_file.exists():
            return None
        return self._parse_system(desc_file)

    def save_system_credentials(self, name: str, credentials: dict) -> None:
        """Persist credentials to systems/<name>/credentials.yaml."""
        cred_path = self.base_dir / 'systems' / name / 'credentials.yaml'
        with open(cred_path, 'w') as f:
            yaml.dump(credentials, f, default_flow_style=False)
        log.info(f'Saved credentials for {name}')

    def save_descriptor(self, descriptor: SystemDescriptor) -> None:
        """Persist a modified descriptor back to disk."""
        data = {
            'name': descriptor.name,
            'display_name': descriptor.display_name,
            'category': descriptor.category,
            'protocol': descriptor.protocol,
            'timing_phases': [{'id': p.id, 'label': p.label} for p in descriptor.timing_phases],
            'input_format': descriptor.input_format,
            'modes': descriptor.modes,
            'rule_extension': descriptor.rule_extension,
            'flags': descriptor.flags,
            'execution': descriptor.execution,
        }
        with open(descriptor.descriptor_path, 'w') as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        log.info(f'Saved descriptor for {descriptor.name}')

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _parse_system(self, path: Path) -> SystemDescriptor:
        with open(path) as f:
            data = yaml.safe_load(f)

        # Load credentials from credentials.yaml if it exists
        credentials = self._load_credentials(path.parent, data.get('name', path.parent.name))

        timing_phases = [TimingPhase(id=p['id'], label=p['label']) for p in data.get('timing_phases', [])]

        rules_dir = path.parent / 'rules'

        return SystemDescriptor(
            name=data['name'],
            display_name=data.get('display_name', data['name']),
            category=data.get('category', 'db'),
            protocol=data.get('protocol', 'subprocess'),
            timing_phases=timing_phases,
            input_format=data.get('input_format', 'tsv'),
            modes=data.get('modes', ['right_recursion', 'left_recursion']),
            rule_extension=data.get('rule_extension', '.sql'),
            flags=data.get('flags', {}),
            execution=data.get('execution', {}),
            descriptor_path=path,
            rules_dir=rules_dir,
            credentials=credentials,
            version=get_system_version(data['name']),
        )

    def _parse_graph_type(self, path: Path) -> GraphTypeDescriptor:
        with open(path) as f:
            data = yaml.safe_load(f)
        return GraphTypeDescriptor(
            name=data['name'],
            display_name=data.get('display_name', data['name']),
            description=data.get('description', ''),
            generator=data.get('generator', ''),
            parameters=data.get('parameters', {}),
        )

    def _parse_domain(self, path: Path) -> DomainDescriptor:
        with open(path) as f:
            data = yaml.safe_load(f)

        raw_params = data.get('query_parameters', [])
        if isinstance(raw_params, dict):
            # Support old dict style: {source_node: int, ...}
            params = [
                QueryParameter(name=k, type=v if isinstance(v, str) else 'str')
                for k, v in raw_params.items()
            ]
        else:
            params = [
                QueryParameter(
                    name=p['name'],
                    type=p.get('type', 'str'),
                    required=p.get('required', True),
                    default=p.get('default'),
                    description=p.get('description', ''),
                )
                for p in (raw_params or [])
            ]

        return DomainDescriptor(
            name=data['name'],
            display_name=data.get('display_name', data['name']),
            description=data.get('description', ''),
            category=data.get('category', 'general'),
            modes=data.get('modes', []),
            query_parameters=params,
            output_schema=data.get('output_schema', []),
            data_requirements=data.get('data_requirements', {}),
            descriptor_path=path,
        )

    def _load_credentials(self, system_dir: Path, system_name: str) -> dict:
        """
        Load credentials from two possible sources (first wins):
        1. systems/<name>/credentials.yaml
        2. global config.yaml / config.json (legacy)
        """
        cred_file = system_dir / 'credentials.yaml'
        if cred_file.exists():
            with open(cred_file) as f:
                return yaml.safe_load(f) or {}

        # Fall back to global config
        global_cfg = self.load_global_config()
        return global_cfg.get(system_name, {})
