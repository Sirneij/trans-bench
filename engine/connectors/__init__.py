"""
engine/connectors/__init__.py

Maps protocol strings (from descriptor.yaml) to connector classes.

There are two ways to add a new protocol:

  Option A — Edit this file (existing workflow):
    1. Implement a class extending BaseConnector in a new file
    2. Import it here and add it to PROTOCOL_REGISTRY

  Option B — Drop-in connector file (zero edits to this file):
    1. Place systems/<name>/connector.py next to the system's descriptor.yaml
    2. Define a single class that ends in "Connector" and extends BaseConnector
    3. Set  protocol: <your_protocol_name>  in descriptor.yaml
    The engine auto-discovers and registers it at startup.
"""

import importlib.util
import logging
import sys
from pathlib import Path

from engine.connectors.base import BaseConnector
from engine.connectors.duckdb_conn import DuckDBConnector
from engine.connectors.mongodb_conn import MongoDBConnector
from engine.connectors.neo4j_conn import Neo4jConnector
from engine.connectors.rdbms import (
    CockroachDBConnector,
    MariaDBConnector,
    PostgreSQLConnector,
)
from engine.connectors.subprocess_conn import (
    AldaConnector,
    ClingoConnector,
    SouffleConnector,
    XSBConnector,
)

log = logging.getLogger(__name__)

PROTOCOL_REGISTRY: dict[str, type[BaseConnector]] = {
    'psycopg2': PostgreSQLConnector,
    'mysqlclient': MariaDBConnector,
    'cockroachdb': CockroachDBConnector,
    'duckdb': DuckDBConnector,
    'neo4j': Neo4jConnector,
    'pymongo': MongoDBConnector,
    'subprocess': XSBConnector,
    'clingo_python': ClingoConnector,
    'souffle_subprocess': SouffleConnector,
    'alda_subprocess': AldaConnector,
}


def _load_plugin_connectors(systems_dir: Path) -> None:
    """
    Auto-discover drop-in connector files placed at systems/<name>/connector.py.

    For each such file found:
      - The file is imported dynamically.
      - Any class whose name ends with 'Connector' (and is not BaseConnector) is
        extracted.
      - The protocol name is determined from the system's descriptor.yaml
        ``protocol`` field.  If the descriptor is missing or the protocol is
        already registered, the file is skipped with a warning.

    This function is called once at module import time, so all auto-discovered
    connectors are available before the first experiment runs.
    """
    import yaml  # local import keeps startup cost minimal if yaml not installed

    if not systems_dir.exists():
        return

    for connector_file in systems_dir.glob('*/connector.py'):
        system_dir = connector_file.parent
        descriptor_file = system_dir / 'descriptor.yaml'

        if not descriptor_file.exists():
            log.warning(f'Skipping drop-in connector {connector_file}: ' f'no descriptor.yaml found in {system_dir}')
            continue

        try:
            with open(descriptor_file) as f:
                descriptor_data = yaml.safe_load(f) or {}
        except Exception as e:
            log.warning(f'Skipping drop-in connector {connector_file}: cannot read descriptor ({e})')
            continue

        protocol = descriptor_data.get('protocol', '')
        if not protocol:
            log.warning(f'Skipping drop-in connector {connector_file}: ' f'descriptor.yaml has no "protocol" field')
            continue

        if protocol in PROTOCOL_REGISTRY:
            log.debug(f'Protocol "{protocol}" already registered — skipping {connector_file}')
            continue

        try:
            module_name = f'_plugin_connector_{system_dir.name}'
            spec = importlib.util.spec_from_file_location(module_name, connector_file)
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)

            # Find the first class ending in 'Connector' that isn't BaseConnector
            cls = None
            for attr_name in dir(module):
                obj = getattr(module, attr_name)
                if (
                    isinstance(obj, type)
                    and attr_name.endswith('Connector')
                    and attr_name != 'BaseConnector'
                    and issubclass(obj, BaseConnector)
                ):
                    cls = obj
                    break

            if cls is None:
                log.warning(
                    f'Drop-in connector {connector_file} has no class ending '
                    f'in "Connector" that subclasses BaseConnector — skipped'
                )
                continue

            PROTOCOL_REGISTRY[protocol] = cls
            log.info(
                f'Registered drop-in connector: protocol="{protocol}" ' f'→ {cls.__name__} (from {connector_file})'
            )

        except Exception as e:
            log.error(f'Failed to load drop-in connector {connector_file}: {e}')


# Run auto-discovery once at import time.
# BASE_DIR is two levels up: engine/connectors/__init__.py → engine → project root
_BASE_DIR = Path(__file__).parent.parent.parent
_load_plugin_connectors(_BASE_DIR / 'systems')


def get_connector(protocol: str) -> type[BaseConnector]:
    """Return the connector class for the given protocol string."""
    cls = PROTOCOL_REGISTRY.get(protocol)
    if cls is None:
        raise ValueError(
            f"Unknown protocol '{protocol}'. "
            f"Registered protocols: {list(PROTOCOL_REGISTRY.keys())}. "
            f"To add a new protocol without editing this file, place a "
            f"connector.py in your system directory (see engine/connectors/__init__.py)."
        )
    return cls


__all__ = [
    'BaseConnector',
    'PROTOCOL_REGISTRY',
    'get_connector',
    '_load_plugin_connectors',
]
