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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml

log = logging.getLogger(__name__)


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

    @property
    def csv_headers(self) -> list[str]:
        """Auto-generated CSV column headers from timing phases."""
        headers: list[str] = []
        for phase in self.timing_phases:
            headers.append(f'{phase.label}RealTime')
            headers.append(f'{phase.label}CPUTime')
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
