"""
engine/figures_tex.py — LaTeX (pgfplots/TikZ) versions of matplotlib figures.

`figure_to_tex(fig)` transcribes a matplotlib figure, after its layout has been computed, into a
standalone LaTeX document that draws the same figure with pgfplots: the page size, the position
and size of every axes, axis limits, log scales, major and minor ticks, grids, every line (data,
colour, width, dash pattern, marker shape, size, fill), every text (tick labels, titles, axis
labels, legend) at the baseline point where matplotlib's PDF backend draws it (recorded by drawing
the figure once through a recording PDF renderer), in the same font (DejaVu Sans, the matplotlib
default) and size; math text (e.g. 10^{-3} tick labels) glyph by glyph, as laid out by mathtext. The LaTeX figure is therefore not a second plotting script that has
to be kept in sync: whatever the matplotlib code draws is what the LaTeX file draws.

Supported: 2-D axes with linear or log scales, Line2D artists (lines and/or markers o s ^ v < > D x + P * p h),
text, and figure/axes legends with line handles. That is what analyze_verified.py draws; other
artists (bars, patches, images) raise NotImplementedError instead of being silently dropped.

`compile_tex(paths)` compiles the documents with the first LaTeX engine found (tectonic, lualatex,
xelatex or pdflatex); the preamble works with all of them.
"""

from __future__ import annotations

import io
import math
import re
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import matplotlib
from matplotlib import cbook
from matplotlib.backends.backend_pdf import PdfFile, RendererPdf
from matplotlib.colors import to_hex, to_rgba
from matplotlib.lines import Line2D

# DejaVu Sans for text and math (matplotlib's default font and mathtext font set). Unicode engines
# use the TrueType/OpenType files through fontspec; pdfTeX uses the Type 1 version.
PREAMBLE = r"""\documentclass[border=0pt]{standalone}
\usepackage{iftex}
\ifPDFTeX
  \usepackage[T1]{fontenc}
  \usepackage{DejaVuSans}
  \renewcommand*\familydefault{\sfdefault}
  \usepackage[italic]{mathastext}
\else
  \usepackage{unicode-math}
  \setmainfont{DejaVuSans.ttf}[BoldFont=DejaVuSans-Bold.ttf,ItalicFont=DejaVuSans-Oblique.ttf,
    BoldItalicFont=DejaVuSans-BoldOblique.ttf]
  \setmathfont{texgyredejavu-math.otf}
\fi
\ifPDFTeX  % one glyph of matplotlib mathtext, by Unicode code point (hex)
  \def\mplchar#1{\ifnum"#1="2212 \ensuremath{-}\else\ifnum"#1="D7 \texttimes\else\char"#1\relax\fi\fi}
\else
  \def\mplchar#1{\char"#1\relax}
\fi
\frenchspacing  % matplotlib puts a normal space after punctuation
\usepackage{pgfplots}
\pgfplotsset{compat=1.18}
"""

# matplotlib marker shapes as pgfplots marks (also used by analyze.py)
MARK_DEFINITIONS = r"""% matplotlib marker shapes; \pgfplotmarksize = half the matplotlib marker size
\pgfdeclareplotmark{mpl-o}{\pgfpathcircle{\pgfpointorigin}{\pgfplotmarksize}\pgfusepathqfillstroke}
\pgfdeclareplotmark{mpl-o-open}{\pgfpathcircle{\pgfpointorigin}{\pgfplotmarksize}\pgfusepathqstroke}
\pgfdeclareplotmark{mpl-s}{\pgfpathrectangle{\pgfpoint{-\pgfplotmarksize}{-\pgfplotmarksize}}{\pgfpoint{2\pgfplotmarksize}{2\pgfplotmarksize}}\pgfusepathqfillstroke}
\pgfdeclareplotmark{mpl-s-open}{\pgfpathrectangle{\pgfpoint{-\pgfplotmarksize}{-\pgfplotmarksize}}{\pgfpoint{2\pgfplotmarksize}{2\pgfplotmarksize}}\pgfusepathqstroke}
\def\mpltriangle#1{\pgfpathmoveto{\pgfpoint{0pt}{#1\pgfplotmarksize}}\pgfpathlineto{\pgfpoint{-\pgfplotmarksize}{-#1\pgfplotmarksize}}\pgfpathlineto{\pgfpoint{\pgfplotmarksize}{-#1\pgfplotmarksize}}\pgfpathclose}
\pgfdeclareplotmark{mpl-^}{\mpltriangle{}\pgfusepathqfillstroke}
\pgfdeclareplotmark{mpl-^-open}{\mpltriangle{}\pgfusepathqstroke}
\pgfdeclareplotmark{mpl-v}{\mpltriangle{-}\pgfusepathqfillstroke}
\pgfdeclareplotmark{mpl-v-open}{\mpltriangle{-}\pgfusepathqstroke}
\def\mpldiamond{\pgfpathmoveto{\pgfpoint{0pt}{1.41421\pgfplotmarksize}}\pgfpathlineto{\pgfpoint{-1.41421\pgfplotmarksize}{0pt}}\pgfpathlineto{\pgfpoint{0pt}{-1.41421\pgfplotmarksize}}\pgfpathlineto{\pgfpoint{1.41421\pgfplotmarksize}{0pt}}\pgfpathclose}
\pgfdeclareplotmark{mpl-D}{\mpldiamond\pgfusepathqfillstroke}
\pgfdeclareplotmark{mpl-D-open}{\mpldiamond\pgfusepathqstroke}
\pgfdeclareplotmark{mpl-x}{\pgfpathmoveto{\pgfpoint{-\pgfplotmarksize}{-\pgfplotmarksize}}\pgfpathlineto{\pgfpoint{\pgfplotmarksize}{\pgfplotmarksize}}\pgfpathmoveto{\pgfpoint{-\pgfplotmarksize}{\pgfplotmarksize}}\pgfpathlineto{\pgfpoint{\pgfplotmarksize}{-\pgfplotmarksize}}\pgfsetbuttcap\pgfusepathqstroke}
\pgfdeclareplotmark{mpl-+}{\pgfpathmoveto{\pgfpoint{-\pgfplotmarksize}{0pt}}\pgfpathlineto{\pgfpoint{\pgfplotmarksize}{0pt}}\pgfpathmoveto{\pgfpoint{0pt}{-\pgfplotmarksize}}\pgfpathlineto{\pgfpoint{0pt}{\pgfplotmarksize}}\pgfsetbuttcap\pgfusepathqstroke}
\def\mplplus{\pgfpathmoveto{\pgfpoint{-0.33333\pgfplotmarksize}{-\pgfplotmarksize}}\pgfpathlineto{\pgfpoint{0.33333\pgfplotmarksize}{-\pgfplotmarksize}}\pgfpathlineto{\pgfpoint{0.33333\pgfplotmarksize}{-0.33333\pgfplotmarksize}}\pgfpathlineto{\pgfpoint{\pgfplotmarksize}{-0.33333\pgfplotmarksize}}\pgfpathlineto{\pgfpoint{\pgfplotmarksize}{0.33333\pgfplotmarksize}}\pgfpathlineto{\pgfpoint{0.33333\pgfplotmarksize}{0.33333\pgfplotmarksize}}\pgfpathlineto{\pgfpoint{0.33333\pgfplotmarksize}{\pgfplotmarksize}}\pgfpathlineto{\pgfpoint{-0.33333\pgfplotmarksize}{\pgfplotmarksize}}\pgfpathlineto{\pgfpoint{-0.33333\pgfplotmarksize}{0.33333\pgfplotmarksize}}\pgfpathlineto{\pgfpoint{-\pgfplotmarksize}{0.33333\pgfplotmarksize}}\pgfpathlineto{\pgfpoint{-\pgfplotmarksize}{-0.33333\pgfplotmarksize}}\pgfpathlineto{\pgfpoint{-0.33333\pgfplotmarksize}{-0.33333\pgfplotmarksize}}\pgfpathclose}
\pgfdeclareplotmark{mpl-P}{\mplplus\pgfusepathqfillstroke}
\pgfdeclareplotmark{mpl-P-open}{\mplplus\pgfusepathqstroke}
\def\mplstar{\pgfpathmoveto{\pgfpointpolar{90}{\pgfplotmarksize}}%
  \foreach \i in {1,...,4} {\pgfpathlineto{\pgfpointpolar{90+72*\i-36}{0.381966\pgfplotmarksize}}\pgfpathlineto{\pgfpointpolar{90+72*\i}{\pgfplotmarksize}}}%
  \pgfpathlineto{\pgfpointpolar{54}{0.381966\pgfplotmarksize}}\pgfpathclose}
\pgfdeclareplotmark{mpl-*}{\mplstar\pgfusepathqfillstroke}
\pgfdeclareplotmark{mpl-*-open}{\mplstar\pgfusepathqstroke}
\def\mpltriside#1{\pgfpathmoveto{\pgfpoint{#1\pgfplotmarksize}{0pt}}\pgfpathlineto{\pgfpoint{-#1\pgfplotmarksize}{\pgfplotmarksize}}\pgfpathlineto{\pgfpoint{-#1\pgfplotmarksize}{-\pgfplotmarksize}}\pgfpathclose}
\pgfdeclareplotmark{mpl-<}{\mpltriside{-}\pgfusepathqfillstroke}
\pgfdeclareplotmark{mpl-<-open}{\mpltriside{-}\pgfusepathqstroke}
\pgfdeclareplotmark{mpl->}{\mpltriside{}\pgfusepathqfillstroke}
\pgfdeclareplotmark{mpl->-open}{\mpltriside{}\pgfusepathqstroke}
\def\mplpolygon#1{\pgfpathmoveto{\pgfpointpolar{90}{\pgfplotmarksize}}\foreach \i in {1,...,#1} {\pgfpathlineto{\pgfpointpolar{90+360/#1*\i}{\pgfplotmarksize}}}\pgfpathclose}
\pgfdeclareplotmark{mpl-p}{\mplpolygon{5}\pgfusepathqfillstroke}
\pgfdeclareplotmark{mpl-p-open}{\mplpolygon{5}\pgfusepathqstroke}
\pgfdeclareplotmark{mpl-h}{\mplpolygon{6}\pgfusepathqfillstroke}
\pgfdeclareplotmark{mpl-h-open}{\mplpolygon{6}\pgfusepathqstroke}
"""
PREAMBLE += MARK_DEFINITIONS


FILLED_MARKERS = {'o', 's', '^', 'v', '<', '>', 'D', 'P', '*', 'p', 'h'}
LINE_MARKERS = {'x', '+'}


def _f(v: float) -> str:
    return f'{v:.4f}'.rstrip('0').rstrip('.') if v != 0 else '0'


def _num(v: float) -> str:
    return f'{v:.10g}'


def _latex_text(s: str) -> str:
    """Plain matplotlib text -> LaTeX (special characters escaped)."""
    s = s.replace('\\', r'\textbackslash{}')
    s = re.sub(r'([&%#_{}$])', r'\\\1', s)
    s = s.replace('~', r'\textasciitilde{}').replace('^', r'\textasciicircum{}')
    return s.replace('\u2212', r'\ensuremath{-}')


class _TextRecorder(RendererPdf):
    """PDF renderer that records every text it is asked to draw, at the baseline point matplotlib
    computed for it (in points); everything else is drawn into a throwaway PDF."""

    def __init__(self, pdf: PdfFile, width_pt: float, height_pt: float):
        super().__init__(pdf, 72, height_pt, width_pt)
        self.texts: list[dict] = []

    def draw_text(self, gc, x, y, s, prop, angle, ismath=False, mtext=None):
        if ismath == 'TeX':
            raise NotImplementedError('usetex text is not supported by figures_tex')
        self.texts.append(dict(x=x, y=y, s=s, size=prop.get_size_in_points(), angle=angle, ismath=bool(ismath),
                               prop=prop, rgb=gc.get_rgb(), mtext=mtext))


def _record_texts(fig) -> list[dict]:
    """Draw the figure as the PDF backend does (72 dpi) and return the recorded texts."""
    w, h = fig.get_size_inches()
    with cbook._setattr_cm(fig, dpi=72):
        pdf = PdfFile(io.BytesIO())
        pdf.newPage(w * 72, h * 72)
        rec = _TextRecorder(pdf, w * 72, h * 72)
        fig.draw(rec)
        pdf.finalize()
    return rec.texts


def _text_tex(t: dict, colors: '_Colors') -> list[str]:
    """TikZ nodes for one recorded text (x, y = start of the baseline, in points)."""
    color = colors(t['rgb'])
    angle = round(t['angle']) % 360
    rot = f', rotate={angle}' if angle else ''

    def node(x_pt, y_pt, size, content):
        return (f'\\node[anchor=base west, inner sep=0pt{rot}, text={color}, '
                f'font=\\fontsize{{{_f(size)}bp}}{{{_f(size * 1.2)}bp}}\\selectfont] '
                f'at ({_f(x_pt / 72)}in,{_f(y_pt / 72)}in) {{{content}}};')

    if not t['ismath']:
        return [node(t['x'], t['y'], t['size'], _latex_text(t['s']))]
    parse = matplotlib.mathtext.MathTextParser('path').parse(t['s'], 72, t['prop'])
    out = []
    ca, sa = math.cos(math.radians(angle)), math.sin(math.radians(angle))
    for glyph in parse.glyphs:
        _, size, code, _, ox, oy = glyph
        gx, gy = t['x'] + ox * ca - oy * sa, t['y'] + ox * sa + oy * ca
        out.append(node(gx, gy, size, f'\\mplchar{{{code:X}}}'))
    for bx, by, bw, bh in parse.rects:  # e.g. fraction bars
        if angle:
            raise NotImplementedError('rotated math rules are not supported by figures_tex')
        x0, y0 = (t['x'] + bx) / 72, (t['y'] + by) / 72
        out.append(f'\\fill[{color}] ({_f(x0)}in,{_f(y0)}in) rectangle ++({_f(bw / 72)}in,{_f(bh / 72)}in);')
    return out


class _Colors:
    def __init__(self):
        self.names: dict[str, str] = {}

    def __call__(self, c) -> str | None:
        if c is None or (isinstance(c, str) and c.lower() == 'none') or to_rgba(c)[3] == 0:
            return None
        h = to_hex(c).upper()[1:]
        if h not in self.names:
            self.names[h] = f'mplc{len(self.names)}'
        return self.names[h]

    def definitions(self) -> str:
        return ''.join(f'\\definecolor{{{n}}}{{HTML}}{{{h}}}\n' for h, n in self.names.items())


def _dash(linestyle: str, lw: float) -> str | None:
    """TikZ dash pattern of a matplotlib line style (None = solid)."""
    rc = matplotlib.rcParams
    patterns = {'--': rc['lines.dashed_pattern'], ':': rc['lines.dotted_pattern'], '-.': rc['lines.dashdot_pattern']}
    if linestyle not in patterns:
        return None
    scale = lw if rc['lines.scale_dashes'] else 1.0
    seq = [x * scale for x in patterns[linestyle]]
    return ' '.join(f'{"on" if i % 2 == 0 else "off"} {_f(x)}bp' for i, x in enumerate(seq))


def _mark_options(line: Line2D, colors: _Colors) -> tuple[str | None, list[str]]:
    """(mark name, TikZ options for the mark) of a Line2D, or (None, [])."""
    m = line.get_marker()
    if m in (None, 'None', 'none', '', ' '):
        return None, []
    if m not in FILLED_MARKERS | LINE_MARKERS:
        raise NotImplementedError(f'marker {m!r} is not supported by figures_tex')
    edge = colors(line.get_markeredgecolor())
    face = colors(line.get_markerfacecolor()) if m in FILLED_MARKERS else None
    name = f'mpl-{m}' if (m in LINE_MARKERS or face) else f'mpl-{m}-open'
    opts = ['solid', f'line width={_f(line.get_markeredgewidth())}bp', f'draw={edge}' if edge else 'draw=none']
    if face:
        opts.append(f'fill={face}')
    return name, [f'mark={name}', f'mark size={_f(line.get_markersize() / 2)}bp', f'mark options={{{", ".join(opts)}}}']


def _line_style(line: Line2D, colors: _Colors) -> list[str]:
    ls, lw = line.get_linestyle(), line.get_linewidth()
    if ls in ('None', 'none', '', ' '):
        return ['only marks']
    opts = [f'color={colors(line.get_color())}', f'line width={_f(lw)}bp', 'line join=round']
    dash = _dash(ls, lw)
    opts += [f'dash pattern={dash}', 'line cap=butt'] if dash else ['solid', 'line cap=rect']
    return opts


def _ticks_in_view(axis, lo: float, hi: float, minor: bool):
    eps = 1e-9 * max(abs(lo), abs(hi), 1)
    locs = axis.get_minorticklocs() if minor else axis.get_majorticklocs()
    # Tick objects are created lazily; ask for one per location so that zip() drops none
    ticks = axis.get_minor_ticks(len(locs)) if minor else axis.get_major_ticks(len(locs))
    return [(loc, tick) for loc, tick in zip(locs, ticks) if lo - eps <= loc <= hi + eps]


def _axes_tex(ax, colors: _Colors, fig_w: float, fig_h: float) -> list[str]:
    for artist in ax.get_children():
        kind = type(artist).__name__
        if kind in ('Rectangle', 'Polygon', 'PathPatch', 'AxesImage', 'PathCollection', 'PolyCollection',
                    'BarContainer', 'LineCollection') and artist is not ax.patch and artist.get_visible():
            raise NotImplementedError(f'{kind} artists are not supported by figures_tex')
    pos = ax.get_position()
    x0, x1 = sorted(ax.get_xlim())
    y0, y1 = sorted(ax.get_ylim())
    opts = [f'at={{({_f(pos.x0 * fig_w)}in,{_f(pos.y0 * fig_h)}in)}}', 'anchor=south west', 'scale only axis',
            f'width={_f(pos.width * fig_w)}in', f'height={_f(pos.height * fig_h)}in',
            f'xmin={_num(x0)}', f'xmax={_num(x1)}', f'ymin={_num(y0)}', f'ymax={_num(y1)}',
            'enlargelimits=false', 'clip=true', 'axis on top=false',
            'axis line style={line width=0.8bp, black}', 'tick align=outside', 'tick pos=left',
            'major tick length=3.5bp', 'minor tick length=2bp',
            'major tick style={line width=0.8bp, black}', 'minor tick style={line width=0.6bp, black}',
            'xticklabels={}', 'yticklabels={}', 'scaled x ticks=false', 'scaled y ticks=false',
            'xlabel={}', 'ylabel={}', 'title={}']
    grid_opts = []
    for name, axis, lo, hi in (('x', ax.xaxis, x0, x1), ('y', ax.yaxis, y0, y1)):
        if axis.get_scale() == 'log':
            opts += [f'{name}mode=log', f'log basis {name}=10']
        elif axis.get_scale() != 'linear':
            raise NotImplementedError(f'{axis.get_scale()} scale is not supported by figures_tex')
        major = _ticks_in_view(axis, lo, hi, minor=False)
        minor = _ticks_in_view(axis, lo, hi, minor=True)
        opts.append(f'{name}tick={{{",".join(_num(v) for v, t in major if t.tick1line.get_visible())}}}')
        opts.append(f'minor {name}tick={{{",".join(_num(v) for v, t in minor if t.tick1line.get_visible())}}}')
        if any(t.gridline.get_visible() for _, t in major):
            g = major[0][1].gridline
            grid_opts.append(name)
            grid_style = f'line width={_f(g.get_linewidth())}bp, draw={colors(g.get_color())}'
    if grid_opts:
        opts += [f'{n}majorgrids' for n in grid_opts] + [f'major grid style={{{grid_style}}}']
    body = [f'\\begin{{axis}}[{", ".join(opts)}]']
    for line in ax.get_lines():
        if not line.get_visible():
            continue
        xs, ys = line.get_xdata(orig=False), line.get_ydata(orig=False)
        pts = [(x, y) for x, y in zip(xs, ys) if not (math.isnan(float(x)) or math.isnan(float(y)))]
        if not pts:
            continue
        _, mark = _mark_options(line, colors)
        style = _line_style(line, colors) + mark
        coords = ' '.join(f'({_num(x)},{_num(y)})' for x, y in pts)
        body.append(f'\\addplot[{", ".join(style)}] coordinates {{{coords}}};')
    body.append('\\end{axis}')
    return body


def _legend_handles_tex(leg, colors: _Colors, text_pos: dict) -> list[str]:
    """Legend handles (line + one marker), placed relative to the recorded position of each entry's text.

    matplotlib's HandlerLine2D draws the handle over handlelength x fontsize, ending handletextpad x
    fontsize before the text, at 0.35 x fontsize above the text baseline, with one marker in the middle.
    """
    if leg is None or not leg.get_visible():
        return []
    frame = leg.get_frame()
    if leg.get_frame_on() and frame.get_visible() and to_rgba(frame.get_edgecolor())[3] > 0:
        raise NotImplementedError('legend frames are not supported by figures_tex (use frameon=False)')
    fs = leg._fontsize
    out = []
    for text, handle in zip(leg.get_texts(), leg.legend_handles):
        if id(text) not in text_pos:
            continue
        if not isinstance(handle, Line2D):
            raise NotImplementedError(f'legend handle {type(handle).__name__} is not supported by figures_tex')
        tx, ty = text_pos[id(text)]
        x_end = (tx - leg.handletextpad * fs) / 72
        x_start = x_end - leg.handlelength * fs / 72
        y = (ty + 0.35 * fs) / 72
        style = _line_style(handle, colors)
        if 'only marks' not in style:
            out.append(f'\\draw[{", ".join(style)}] ({_f(x_start)}in,{_f(y)}in) -- ({_f(x_end)}in,{_f(y)}in);')
        name, mark = _mark_options(handle, colors)
        if name:
            mopts = mark[2][len('mark options={'):-1]
            out.append(f'\\begin{{scope}}[{mopts}]\\pgftransformshift{{\\pgfpoint{{{_f((x_start + x_end) / 2)}in}}{{{_f(y)}in}}}}'
                       f'\\pgfsetplotmarksize{{{_f(handle.get_markersize() / 2)}bp}}\\pgfuseplotmark{{{name}}}\\end{{scope}}')
    return out


def figure_to_tex(fig) -> str:
    """Standalone LaTeX document drawing `fig` with pgfplots (see the module docstring)."""
    texts = _record_texts(fig)  # also finalizes layout, ticks and legend positions
    w, h = fig.get_size_inches()
    colors = _Colors()
    body = [f'\\useasboundingbox (0in,0in) rectangle ({_f(w)}in,{_f(h)}in);']
    for ax in fig.axes:
        if ax.get_visible():
            body += _axes_tex(ax, colors, w, h)
    text_pos = {id(t['mtext']): (t['x'], t['y']) for t in texts if t['mtext'] is not None}
    for leg in [ax.get_legend() for ax in fig.axes] + list(fig.legends):
        body += _legend_handles_tex(leg, colors, text_pos)
    for t in texts:
        body += _text_tex(t, colors)
    return (PREAMBLE + colors.definitions() + '\\begin{document}\n\\begin{tikzpicture}\n'
            + '\n'.join(body) + '\n\\end{tikzpicture}\n\\end{document}\n')


# ─────────────────────────────────────────────────────────────────────────────
# Compilation
# ─────────────────────────────────────────────────────────────────────────────

ENGINES = ('tectonic', 'lualatex', 'xelatex', 'pdflatex')


def find_engine() -> str | None:
    return next((e for e in ENGINES if shutil.which(e)), None)


def _compile_one(engine: str, tex: Path) -> tuple[Path, str | None]:
    tex = Path(tex).resolve()  # the engine runs in the figure's directory; relative paths would break
    if engine == 'tectonic':
        cmd = [engine, '-X', 'compile', '--outdir', str(tex.parent), str(tex)]
    else:
        cmd = [engine, '-interaction=nonstopmode', '-halt-on-error', '-output-directory', str(tex.parent), str(tex)]
    p = subprocess.run(cmd, capture_output=True, text=True, cwd=tex.parent)
    for ext in ('.aux', '.log'):
        tex.with_suffix(ext).unlink(missing_ok=True)
    if p.returncode != 0:
        return tex, ' '.join((p.stdout + p.stderr).split())[-800:]
    return tex, None


def compile_tex(paths: list[Path], engine: str | None = None, jobs: int = 4) -> dict[Path, str | None]:
    """Compile standalone documents to PDF next to them; returns {path: error or None}."""
    engine = engine or find_engine()
    if engine is None:
        raise RuntimeError(f'no LaTeX engine found (tried {", ".join(ENGINES)})')
    with ThreadPoolExecutor(max_workers=jobs) as pool:
        return dict(zip(paths, (err for _, err in pool.map(lambda p: _compile_one(engine, Path(p)), paths))))
