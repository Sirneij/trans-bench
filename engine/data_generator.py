"""
Re-export the graph generators under the import path used by graph_types/*.yaml.

Import path for the graph generators referenced by graph_types/*.yaml (`generator:` field). The
generators are the `generate_<graph_type>_graph` methods of `DataGenerator` in generate_db.py,
which is what generate_db.py (and therefore every benchmark input) actually uses; add new
generators there. This module only re-exports the class so that the dotted paths resolve.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from generate_db import DataGenerator  # noqa: E402  # pylint: disable=wrong-import-position

__all__ = ['DataGenerator']
