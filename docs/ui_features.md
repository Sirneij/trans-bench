# Web UI

`python transitive.py --ui` (add `--ui-port 5055` on macOS, where AirPlay Receiver uses port 5000) serves the UI from
`ui/app.py`. Templates are in `ui/templates/`, the design system in `ui/static/css/app.css` (colour, type, spacing and
motion tokens with light and dark values) and the shared behaviour in `ui/static/js/app.js` (`window.TB`). Data shaping
for the pages lives in `ui/data.py`.

## Verified campaigns (`/campaigns`)

A campaign is a directory `results/<name>/` with one `<series>/runs.jsonl` per system, written by `benchmark.py`, and
the `analysis/` that `analyze_verified.py` produces from it. Each campaign page has:

- **Overview**: runs executed, completed, failed (by kind: timeout, out of memory, unsupported, iteration limit, error)
  and skipped after a failure, per series; whether every completed run returned the correct closure
  (`verification.json`), cross-system agreement on the scale-free and Barabási–Albert graphs, and the captured versions.
- **Scaling**: mean time or memory against n for one topology and mode, one curve per system in its paper colour and
  marker, failures drawn as ✕ at the time limit, legend in the order the curves end (`engine/plot_style.py`).
- **Matrix**: a heat map of every topology × system at one size (or every size × system for the scale-free and
  Barabási–Albert graphs), log-scaled, with TO/OOM/unsupported cells, skipped cells marked with their cause, the fastest
  system per row outlined and incorrect results flagged. Clicking a cell opens its curve.
- **Figures**: the matplotlib and pgfplots PDFs of the paper, rendered with pdf.js, with a keyboard-navigable viewer.
- **Failures**: `failures.csv`, filterable by kind and text, sortable, with the full error message on click.
- **Files**: README, versions, pip freeze, the code patch the campaign ran with (as a diff), `summary.csv`,
  `failures.csv`, `verification.json` and the LaTeX tables.

JSON behind these views: `/api/campaigns/<name>/matrix` and `/api/campaigns/<name>/series`.

## Experiments started from the UI

- **New experiment** (`/experiment/new`): systems → topologies (each with a drawing) → settings (domain, modes, sizes,
  runs) → review. The review shows the number of runs and the equivalent `transitive.py` command, and a `benchmark.py`
  loop for a verified campaign. Links from a system or topology page preselect it (`?systems=`, `?graphs=`).
- **Live monitor** (`/experiment/live`): progress ring, elapsed and remaining time, the configuration being run,
  configurations finished per system, and the output stream with level filters, search, follow, download and clear.
  **Stop** ends the run after the configuration that is running (`ExperimentRunner(should_stop=...)`).
- **Results explorer** (`/results`): the timing CSVs of `transitive.py` runs. Compare systems phase by phase (stacked
  bars per mode) or as a trend of one phase, in real time, CPU time or memory; open a file to see each run and the
  average.

## Configuration

- **Systems** (`/systems`, `/systems/<name>`): connector, detected version, modes and credential status; the timing
  phases with the one reported as query time highlighted; editors for `descriptor.yaml` (validated before saving), the
  rule files and `credentials.yaml` (hidden until revealed). ⌘S saves; unsaved editors are marked and guarded.
- **Topologies** (`/graphs`, `/graphs/<name>`): every graph family drawn from its own generator; on a topology page the
  size can be changed and the pairs the transitive closure adds can be overlaid; the formal definition (KaTeX) and the
  generator's source.
- **Register a system**, **Add a topology**, **Domains**: forms that bootstrap a system from an existing one, a graph
  descriptor (with a generator stub to implement) or a query domain from a template.

## Interaction

- ⌘K or `/` opens a command palette (pages, actions, systems, topologies, campaigns); `g` then `d`/`s`/`t`/`c`/`r`/`l`
  jumps to a page, `n` starts a new experiment, `t` cycles the theme (system, light, dark).
- Confirmations and messages are in-page dialogs and toasts; the top bar shows when an experiment is running.
- Motion (page transitions, staggered reveals, count-ups, drawn edges, animated tabs and steps) respects
  `prefers-reduced-motion`. The layout works down to phone width, with the navigation in a drawer.
