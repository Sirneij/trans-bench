"""The figure rules of the suite (engine/plot_style.py): one marker per system in every figure, and
legends in the order of the systems' last data points."""

import re
from pathlib import Path
from unittest.mock import patch

import pytest

from engine.plot_style import SYSTEM_STYLE, legend_order, style

BASE = Path(__file__).resolve().parent.parent


def test_every_system_has_its_own_marker_and_colour():
    markers = [m for _, _, m, _ in SYSTEM_STYLE.values()]
    colours = [c for _, c, _, _ in SYSTEM_STYLE.values()]
    assert len(set(markers)) == len(markers)
    assert len(set(colours)) == len(colours)
    assert style('mariadb_tuned') == SYSTEM_STYLE['mariadb_tuned'] and style('xsb_other') == SYSTEM_STYLE['xsb']
    with pytest.raises(KeyError):
        style('nosuchsystem')


def test_legend_order_is_the_order_of_the_last_points():
    # the reviewer's example (original Fig. 4a): PostgreSQL ends above Neo4j, so it is listed first
    last = {'cockroachdb': (1000, 18), 'neo4j': (1000, 11), 'postgres': (1000, 12.5), 'xsb': (1000, 5),
            'duckdb': (1000, 2.4)}
    assert legend_order(last) == ['cockroachdb', 'postgres', 'neo4j', 'xsb', 'duckdb']
    # failure markers (at the time limit) end at the top; the one further right first
    last = {'mongodb': (200, 600), 'mariadb': (1000, 600), 'duckdb': (1000, 2)}
    assert legend_order(last) == ['mariadb', 'mongodb', 'duckdb']


@pytest.fixture(scope='module')
def published_figures():
    """All 28 figures of the published campaign, as matplotlib Figure objects."""
    import matplotlib.pyplot as plt

    import analyze_verified as av

    av.RESULTS = BASE / 'results' / 'verified_2026'
    rows = av.summarize(av.load(av.RESULTS))
    av.apply_agreement(rows)
    figs = []
    with patch.object(av.plt, 'close', lambda f: figs.append(f)), \
            plt.rc_context({'figure.max_open_warning': 0}):  # the test keeps all 28 figures open
        for g in av.GRAPHS:
            for cpu in (False, True):
                av.plot_graph(rows, g, list(range(100, 1001, 100)), Path('/nonexistent'), cpu=cpu, formats=())
        for g, sizes in (('scale_free', range(10000, 90001, 10000)), ('barabasi_albert', range(10000, 100001, 10000))):
            for cpu in (False, True):
                av.plot_graph(rows, g, list(sizes), Path('/nonexistent'), cpu=cpu, formats=())
    yield figs
    for f in figs:
        plt.close(f)


def test_published_figures_follow_the_rules(published_figures):
    assert len(published_figures) == 28
    label_to_system = {label: name for name, (label, _, _, _) in SYSTEM_STYLE.items()}
    for fig in published_figures:
        for ax in fig.axes:
            last, marker_of = {}, {}
            for line in ax.get_lines():
                system = label_to_system.get(line.get_label())
                if system is None:  # failure marker: same marker, hollow; belongs to the preceding curve
                    assert line.get_marker() == marker_of[prev] and line.get_markerfacecolor() == 'none'
                    last[prev] = (line.get_xdata()[-1], line.get_ydata()[-1])
                    continue
                assert line.get_marker() == style(system)[2], (ax.get_title(), system)  # rule 1
                marker_of[system] = line.get_marker()
                if len(line.get_xdata()):
                    last[system] = (line.get_xdata()[-1], line.get_ydata()[-1])
                prev = system
            labels = [t.get_text() for t in ax.get_legend().get_texts()]
            assert labels == [SYSTEM_STYLE[s][0] for s in legend_order(last)], ax.get_title()  # rule 2


def test_analyze_py_uses_the_same_rules():
    import pandas as pd

    import analyze

    df = pd.DataFrame([dict(environment=e, size=n, real_time=t) for e, pts in {
        'cockroachdb': [(100, 1), (1000, 18)], 'postgres': [(100, 1), (1000, 12.5)], 'neo4j': [(100, 2), (1000, 11)],
        'xsb': [(100, .1), (1000, 5)], 'duckdb': [(100, .1), (1000, 2.4)]}.items() for n, t in pts])
    tex = analyze.generate_pgfplots(df, 'max_acyclic', 'left_recursion', 'real_time', 20)
    assert re.findall(r'addlegendentry\{([^}]*)\}', tex) == ['CockroachDB', 'PostgreSQL', 'Neo4j', 'XSB', 'DuckDB']
    assert re.findall(r'mark=mpl-(\S+?),', tex) == ['D', 's', 'P', 'x', '^']
    assert '\\addplot+' not in tex  # no cycle list: the marker never depends on the plot order
