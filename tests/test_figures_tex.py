"""Tests for engine/figures_tex.py (matplotlib figure -> standalone pgfplots/TikZ document)."""

import re
from pathlib import Path

import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa: E402
import pytest  # noqa: E402

from engine.figures_tex import compile_tex, figure_to_tex, find_engine  # noqa: E402


def sample_figure():
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.9), sharey=True)
    for ax in axes:
        ax.plot(
            [100, 200, 300],
            [0.001, 0.02, 3.0],
            color='tab:blue',
            marker='s',
            linestyle='-',
            markersize=3.5,
            linewidth=1.1,
            label='PostgreSQL',
        )
        ax.plot(
            [100, 200],
            [0.005, 0.5],
            color='tab:cyan',
            marker='P',
            linestyle='--',
            markersize=3.5,
            linewidth=1.1,
            label='Neo4j',
        )
        ax.plot([300], [600], color='tab:cyan', marker='P', markersize=6, markerfacecolor='none', linestyle='')
        ax.set_yscale('log')
        ax.set_title('Cyc: left recursion', fontsize=9)
        ax.grid(True, which='major', linewidth=0.3)
    axes[0].set_ylabel('Elapsed time (s, log scale)', fontsize=8)
    h, lab = axes[0].get_legend_handles_labels()
    fig.legend(h, lab, loc='lower center', ncol=2, fontsize=7, frameon=False)
    fig.tight_layout(rect=(0, 0.1, 1, 1))
    return fig


def test_transcription_contents():
    fig = sample_figure()
    tex = figure_to_tex(fig)
    plt.close(fig)
    assert tex.count('\\begin{axis}') == 2 and 'ymode=log' in tex
    assert '(100,0.001) (200,0.02) (300,3)' in tex  # the data, unchanged
    assert 'mark=mpl-s' in tex and 'mark=mpl-P-open' in tex  # filled and hollow markers
    assert 'dash pattern=on 4.07bp off 1.76bp' in tex  # matplotlib's '--' at line width 1.1
    assert '\\useasboundingbox (0in,0in) rectangle (7.2in,2.9in);' in tex  # same page size
    assert re.search(r'rotate=90[^]]*\] at [^{]*\{Elapsed time \(s, log scale\)\}', tex)
    assert '{PostgreSQL}' in tex and '{Neo4j}' in tex and '{Cyc: left recursion}' in tex
    assert '\\mplchar{2212}' in tex  # the minus of the 10^{-3} tick label, placed glyph by glyph
    assert tex.count('\\definecolor') == 4  # grid grey, tab:blue, tab:cyan, black (text)


def test_unsupported_artists_are_refused():
    fig, ax = plt.subplots()
    ax.bar([1, 2], [3, 4])
    with pytest.raises(NotImplementedError):
        figure_to_tex(fig)
    plt.close(fig)


@pytest.mark.skipif(find_engine() is None, reason='no LaTeX engine installed')
def test_compiles_to_a_page_of_the_figure_size(tmp_path):
    from pypdf import PdfReader

    fig = sample_figure()
    tex = tmp_path / 'fig.tex'
    tex.write_text(figure_to_tex(fig))
    plt.close(fig)
    assert compile_tex([tex]) == {tex: None}
    # a relative path (analyze_verified.py --out results/...) compiles too
    import os

    cwd = os.getcwd()
    try:
        os.chdir(tmp_path.parent)
        rel = Path(tmp_path.name) / 'fig.tex'
        (tmp_path / 'fig.pdf').unlink()
        assert compile_tex([rel]) == {rel: None} and (tmp_path / 'fig.pdf').exists()
    finally:
        os.chdir(cwd)
    box = PdfReader(tmp_path / 'fig.pdf').pages[0].mediabox
    assert (round(float(box.width) / 72, 2), round(float(box.height) / 72, 2)) == (7.2, 2.9)
