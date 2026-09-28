"""
engine/plot_style.py — how every system is drawn in every figure of the suite.

Two rules, used by analyze_verified.py and analyze.py (and checked by the tests):

1. One style per system, the same in every figure: colour, marker and line style. The marker is
   what tells the systems apart in black-and-white print, so no two systems share one, and a
   system's marker never depends on which other systems appear in a figure or in which order
   they are plotted.
2. Legend order = order of the last data points: a legend lists the systems top to bottom in the
   order in which their curves end (highest last point first), so that each label can be matched
   to the curve next to it. A curve that ends in a failure marker (placed at the time limit) ends
   at the top; ties are broken by the x of the last point (further right first), then by the
   fixed order of SYSTEM_STYLE.
"""

from __future__ import annotations

# name -> (legend label, colour, matplotlib marker, line style)
SYSTEM_STYLE: dict[str, tuple[str, str, str, str]] = {
    'xsb': ('XSB', 'black', 'x', '-'),
    'postgres': ('PostgreSQL', 'tab:blue', 's', '-'),
    'mariadb': ('MariaDB', 'tab:red', 'v', '-'),
    'duckdb': ('DuckDB', 'goldenrod', '^', '-'),
    'cockroachdb': ('CockroachDB', 'tab:purple', 'D', '-'),
    'mongodb': ('MongoDB', 'tab:green', 'o', '--'),
    'neo4j': ('Neo4j', 'tab:cyan', 'P', '--'),
    'singlestore': ('SingleStore', 'tab:brown', '*', ':'),
    'mariadb_tuned': ('MariaDB (4 GB tmp)', 'salmon', 'p', ':'),
    'clingo': ('Clingo', 'tab:olive', '<', '-.'),
    'souffle': ('Soufflé', 'tab:pink', '>', '-.'),
    'alda': ('Alda', 'tab:gray', 'h', '-.'),
}


def style(series: str) -> tuple[str, str, str, str]:
    """(label, colour, marker, line style) of a system or series (e.g. mariadb_tuned)."""
    if series in SYSTEM_STYLE:
        return SYSTEM_STYLE[series]
    base = max((s for s in SYSTEM_STYLE if series.startswith(s + '_')), key=len, default=None)
    if base is None:
        raise KeyError(f'no plot style for {series!r}: add it to engine/plot_style.py SYSTEM_STYLE')
    return SYSTEM_STYLE[base]


def legend_order(last_points: dict[str, tuple[float, float]]) -> list[str]:
    """Series names ordered by their last data point (x, y): highest y first (see module docstring)."""
    rank = {s: i for i, s in enumerate(SYSTEM_STYLE)}
    return sorted(last_points, key=lambda s: (-last_points[s][1], -last_points[s][0], rank.get(s, len(rank)), s))
