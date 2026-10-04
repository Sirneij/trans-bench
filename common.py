"""
Provide the base class of the systems' operations modules (systems/<name>/__init__.py).

The connectors in engine/connectors/ open the connection and hand it to the operations class of
the rule file; that class keeps the global configuration through Base. The old harness also kept
its own connection code and runner here; they now live in the engine.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional


class Base:
    """Configuration shared by every operations class of a system."""

    def __init__(self, config: dict[str, Any]) -> None:
        """Keep the configuration (config.yaml, with the run's query bindings)."""
        self.config = config
        self.driver: Any = None
        self.db_path: Optional[Path] = None

    def close(self) -> None:
        """Close a driver opened by a subclass, and remove its database file if it made one."""
        if self.driver:
            self.driver.close()
            self.driver = None
        if self.db_path and self.db_path.exists():
            os.remove(self.db_path)
