# Web interface

The web interface is started with `python transitive.py --ui`. On macOS, `--ui-port 5055` should be
added, since AirPlay Receiver already listens on port 5000. The application is built in `ui/app.py`
from four groups of views: `ui/pages.py` (landing page, overview, systems and topologies),
`ui/results.py` (campaigns and the results explorer), `ui/experiments.py` (the wizard and the live
monitor) and `ui/editing.py` (every view that writes a file). The data each page needs is prepared in
`ui/data.py` and `ui/site.py`, while the templates are in `ui/templates/`, the design tokens (colour, type,
spacing and motion, with light and dark values) in `ui/static/css/app.css`, and the shared scripts in
`ui/static/js/app.js`.

A campaign started from the interface runs on the same engine as `benchmark.py` and
`transitive.py --campaign` (`engine/campaign.py`). Hence a run started with the Start button gives the
same `runs.jsonl` records as one started from a terminal, and either can be resumed from the other.

## Landing page (`/`)

The landing page introduces the suite to a first-time reader, and every drawing on it is computed
from data. In the hero, breadth-first waves spread from one start node over a random graph, in the way
a semi-naive evaluation adds the pairs of each iteration; a counter beside the rule `tc(X, Z) :-
tc(X, Y), e(Y, Z)` reports the pairs derived so far. Further down the page, the following sections
appear as they are scrolled into view:

1. how a closure grows, where a small fixed graph is evaluated pair by pair next to the closure
   drawn as a matrix;
2. the fourteen topologies, each drawn from its own generator and linked to its page;
3. a race on the complete, cycle, path or grid graph at its largest size in the latest analyzed
   campaign, where bars grow on a log-scale clock until each system's mean query time, and a system
   that failed is shown with the size where it first failed;
4. the verification figures: runs executed, results checked, the wrong results found, and the
   failures by kind;
5. a short account of the engine that the site and the command line share.

The page has its own template (`landing.html`), style sheet (`landing.css`) and script (`landing.js`).
With `prefers-reduced-motion`, each drawing shows its final state at once.

## Overview (`/overview`)

The overview is the working home of the interface. It shows the fastest systems at the largest
graphs of the latest campaign (the fastest completed system for each structured topology, with left
and right recursion), the outcome and verification of that campaign, the topologies with their edge
and closure counts, the registered systems, and the commands that start a campaign from a terminal.

## Campaigns (`/campaigns`)

A campaign is a directory `results/<name>/` with one `<series>/runs.jsonl` per system and the
`analysis/` that `analyze_verified.py` writes from those records. Each campaign page has seven tabs.

| Tab | Content |
| --- | --- |
| Overview | runs executed, completed, failed by kind (timeout, out of memory, unsupported, iteration limit, error) and skipped after a failure, per series; whether every completed result was correct; agreement between systems on the scale-free and Barabási-Albert graphs; the captured versions |
| Race | a bar race through the sizes of one topology and mode, on one log scale for all n; a system that fails stops at the time limit, with the size where it failed |
| Scaling | mean time or memory against n, one curve per system in its paper colour and marker, failures drawn at the time limit |
| Matrix | a heat map of every topology and system at one size (or every size and system for the two random families), with failed, skipped and incorrect cells marked; a click opens the curve |
| Figures | the matplotlib and pgfplots PDFs of the paper, shown with pdf.js |
| Failures | `failures.csv`, filtered by kind or text and sorted by any column |
| Files | README, versions, pip freeze, the code patch the campaign ran with, `summary.csv`, `failures.csv`, `verification.json` and the LaTeX tables |

The Matrix and Scaling tabs read `/api/campaigns/<name>/matrix` and `/api/campaigns/<name>/series`.
The legend of every chart lists the systems in the order in which their curves end
(`engine/plot_style.py`), as in the paper.

## Results explorer (`/results`)

This page reads the timing files of every campaign. On the left, a tree is ordered by campaign,
series, topology and mode, with one leaf per size; it is sent to the browser as JSON and built only
when a branch is opened, which keeps the page small even for the 2026 campaigns. A leaf opens the
phases of each run and their mean. The Compare tab sets systems against one another, either phase by
phase as stacked bars or as the trend of one phase over n, in real time, CPU time or memory.

## Starting a campaign

The wizard (`/experiment/new`) asks four questions in turn: the systems, the topologies (each with
its drawing), the settings (domain, query mode, modes, sizes, number of runs, campaign name and time
limit per run), and a review. At the end, the review gives the number of runs and the equivalent `transitive.py`
and `benchmark.py` commands. A link from a system or topology page selects that system or topology in
advance (`?systems=` or `?graphs=`).

The live monitor (`/experiment/live`) follows the campaign while it runs. It shows a progress ring,
the elapsed and remaining time, the configuration being run, the configurations finished per system,
and a mosaic with one tile per configuration (system, topology, size and mode). A tile pulses while
its configuration runs and then takes the colour of its outcome: a shade from fast to slow if it
completed, or the mark of a failure, a skip or an unsupported mode. Below the mosaic is the output of
the engine, with filters by level, search, follow, download and clear. The stream is replayed from
the start of the campaign (`/experiment/stream` with `Last-Event-ID`), so a page opened late shows
everything, and several pages can follow one campaign. Stop ends the run in progress at once; that run
is not recorded, and the same campaign resumes from it.

## Systems and topologies

On a system's page (`/systems/<name>`) the connector, the detected version, the supported modes and
the state of the credentials are shown, together with the timing phases, among which the phase
reported as query time is marked. The descriptor, the rule files and `credentials.yaml` can be edited
there; a descriptor is validated before it is saved, the credentials stay hidden until they are
revealed, and ⌘S saves the open editor.

Each topology page (`/graphs/<name>`) is a small laboratory for the closure. The fixpoint view plays
the evaluation one iteration at a time, with a histogram of the pairs each iteration adds. Linear
recursion (left or right) adds at iteration k the pairs whose shortest path has k edges, while double
recursion doubles the path length per iteration and so needs about log₂ of the longest shortest
path; both counts are shown. The reach view lights up everything one node reaches, wave by wave. The
size can be changed, and the definition of the family and the source of its generator are given
below the drawing.

New systems, topologies and query domains are registered from `/systems/new`, `/graphs/new` and
`/domains/new`. Each form copies a template or an existing entry, so that only the differences have
to be written.

## Read-only mode

On a public deployment, the interface runs read-only. This mode is set by `TRANS_BENCH_READ_ONLY=1`,
which the Docker image defines, and it is also turned on wherever `RAILWAY_PROJECT_ID` is defined,
that is, on Railway. In this mode:

- every request that would write a file or start a run is refused with status 403;
- the validation endpoints, which would connect to the databases, are refused as well;
- credentials are never sent to the browser;
- a banner on every page says that the copy only shows the published campaigns, and that
  benchmarks are run from a clone of the repository.

The one POST that stays open is `/api/compare/trends`, which only reads; its body carries the
selection. Every response carries the headers `X-Content-Type-Options`, `X-Frame-Options` and
`Referrer-Policy`, and `/healthz` answers the health check of the platform. The deployment itself is
described in the README.

## Keyboard and motion

⌘K or `/` opens a command palette with the pages, actions, systems, topologies and campaigns. The
key `g` followed by `h`, `d`, `s`, `t`, `c`, `r` or `l` opens the landing page, the overview, the
systems, the topologies, the campaigns, the results explorer or the live monitor; `n` starts a new
campaign, and `t` cycles through the themes (system, light, dark). Confirmations and messages appear
inside the page, and the top bar shows when a campaign is running.

Pages that share an element, such as the drawing of a topology or the title of a campaign, change
into each other with a view transition, and sections rise into place as they are scrolled into view.
All of this motion is dropped under `prefers-reduced-motion`. The layout works down to the width of
a phone, where the navigation moves into a drawer.
