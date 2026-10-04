"""
Hold what every page of the Web UI shares: the descriptor loader, system versions and plot styles.

create_app() (ui/app.py) makes one Site and keeps it in app.extensions; the view modules reach it
through site().
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from flask import current_app

from engine.loader import DescriptorLoader, GraphTypeDescriptor, SystemDescriptor
from engine.plot_style import SYSTEM_STYLE
from ui import data as uidata

BASE_DIR = Path(__file__).resolve().parent.parent
EXTENSION_KEY = 'trans-bench'

# matplotlib marker -> (Chart.js pointStyle, rotation)
_SHAPES = {
    'x': ('crossRot', 0),
    's': ('rect', 0),
    'v': ('triangle', 180),
    '^': ('triangle', 0),
    '<': ('triangle', 270),
    '>': ('triangle', 90),
    'D': ('rectRot', 0),
    'o': ('circle', 0),
    'P': ('cross', 0),
    '*': ('star', 0),
    'p': ('rectRounded', 0),
    'h': ('dash', 0),
}
# The colours of engine/plot_style.py as hex (matplotlib's values), so that the Web UI does not
# need matplotlib. A colour that is missing here is converted by matplotlib, if it is installed.
_HEX = {
    'black': '#000000',
    'tab:blue': '#1f77b4',
    'tab:orange': '#ff7f0e',
    'tab:green': '#2ca02c',
    'tab:red': '#d62728',
    'tab:purple': '#9467bd',
    'tab:brown': '#8c564b',
    'tab:pink': '#e377c2',
    'tab:gray': '#7f7f7f',
    'tab:olive': '#bcbd22',
    'tab:cyan': '#17becf',
    'goldenrod': '#daa520',
    'salmon': '#fa8072',
}


def to_hex(color: str) -> str:
    """Return a matplotlib colour name as #rrggbb."""
    if color in _HEX:
        return _HEX[color]
    from matplotlib.colors import to_hex as mpl_to_hex

    return mpl_to_hex(color)


def plot_styles() -> dict[str, dict[str, Any]]:
    """Return each system's colour and point shape for the charts (engine/plot_style.py), as Chart.js options."""
    styles = {}
    for name, (label, color, marker, linestyle) in SYSTEM_STYLE.items():
        hex_color = to_hex(color)
        styles[name] = {
            'label': label,
            # black (XSB) is drawn in the theme's ink colour, which works on light and dark backgrounds
            'color': None if hex_color == '#000000' else hex_color,
            'pointStyle': _SHAPES[marker][0],
            'rotation': _SHAPES[marker][1],
            'dashed': linestyle != '-',
            'marker': marker,
        }
    return styles


class Site:
    """The loader and the version cache of one running Web UI."""

    def __init__(self, base_dir: Path = BASE_DIR):
        """Load descriptors from `base_dir`; versions are detected once, in the background."""
        self.base_dir = base_dir
        self.loader = DescriptorLoader(base_dir=base_dir, detect_versions=False)
        self.versions = uidata.VersionCache()

    def systems(self) -> list[SystemDescriptor]:
        """Return every system, with its version once the background detection has found it."""
        systems = self.loader.load_systems()
        try:
            self.versions.start([s.name for s in systems])
            for s in systems:
                s.version = self.versions.get(s.name) or 'detecting…'
        except Exception:  # descriptors without a name (the tests use mocks)
            pass
        return systems

    def system(self, name: str) -> Optional[SystemDescriptor]:
        """Return one system, or None."""
        system = self.loader.get_system(name)
        if system is not None:
            system.version = self.versions.get(system.name) or 'detecting…'
        return system

    def graph_types(self) -> list[GraphTypeDescriptor]:
        """Return every graph type."""
        return self.loader.load_graph_types()

    @staticmethod
    def system_ui(systems: list[SystemDescriptor]) -> dict[str, dict]:
        """Map each system to {label, color, marker}, the identity marks of the paper's plot style."""
        styles = plot_styles()
        return {
            s.name: styles.get(s.name, {'label': s.display_name, 'color': None, 'pointStyle': 'circle'})
            for s in systems
            if isinstance(getattr(s, 'name', None), str)
        }

    def transitive_modes(self) -> list[str]:
        """Return the standard modes plus system-specific ones (e.g. DuckDB's doublerecurring_recursion)."""
        modes = ['right_recursion', 'left_recursion', 'double_recursion']
        for s in self.systems():
            modes += [m for m in s.modes if m not in modes]
        return modes

    def analyzed_campaign(self) -> Optional[Path]:
        """Return the newest campaign that has an analysis (its summary.csv), or None."""
        return next((d for d in uidata.campaign_dirs(self.base_dir) if (d / 'analysis' / 'summary.csv').exists()), None)

    def previews(self, graph_types: list[GraphTypeDescriptor]) -> dict[str, Optional[dict]]:
        """Return a drawable small instance of every graph type."""
        return {g.name: uidata.graph_preview(str(self.base_dir), g.name) for g in graph_types}


def site() -> Site:
    """Return the Site of the running app."""
    return current_app.extensions[EXTENSION_KEY]


def read_only() -> bool:
    """Return whether this is the public read-only deployment (no runs, no edits)."""
    return bool(current_app.config.get('READ_ONLY'))
