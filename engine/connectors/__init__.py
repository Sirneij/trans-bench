"""
engine/connectors/__init__.py

Maps protocol strings (from descriptor.yaml) to connector classes.
Adding a new protocol only requires:
  1. Implementing a class that extends BaseConnector
  2. Registering it in PROTOCOL_REGISTRY below
"""

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


def get_connector(protocol: str) -> type[BaseConnector]:
    """Return the connector class for the given protocol string."""
    cls = PROTOCOL_REGISTRY.get(protocol)
    if cls is None:
        raise ValueError(f"Unknown protocol '{protocol}'. " f"Registered protocols: {list(PROTOCOL_REGISTRY.keys())}")
    return cls


__all__ = [
    'BaseConnector',
    'PROTOCOL_REGISTRY',
    'get_connector',
]
