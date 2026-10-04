# Requirements of the extension work, and where they stand

When trans-bench was turned from a transitive-closure harness into a suite that others can extend,
its requirements were written down in this file. They are kept here with their present status,
because several were met in a different way than first planned, and two were dropped when the code
they concerned was removed.

## Domains and the layout of results

The suite was to accept recursive domains other than the transitive closure (`shortest_path`,
`same_generation` and so on). This is in place: a domain is a descriptor under `domains/`, a rule
file is named after its domain and mode, and the engine runs only the modes that both the system and
the domain declare. No domain other than the transitive closure has rule files yet, and a weighted
domain still has to settle the name of its input file ([EXTENSION_GUIDE.md](EXTENSION_GUIDE.md)).

Results were to be separated by domain, under `timing/<domain>/<system>/<graph>/`. That layout
survives inside each series of a campaign (`results/<campaign>/<series>/timing/<domain>/...`), but
the record of a run is now its line in `runs.jsonl`; the old top-level `timing/` directory was
retired. Timing rows record every phase separately, with real time, CPU time and memory.

## Memory

The first plan was to poll the resident memory (RSS) of every process involved. In trials, this
proved unreliable for servers that serve every run, since their allocators keep freed memory across
runs. As a result, the suite samples what each system can report about one query: the RSS of a
process that lives for one run (DuckDB, XSB, a PostgreSQL backend), or the server's own accounting
(MariaDB, CockroachDB, Neo4j). MongoDB and SingleStore have no usable probe.
[VERIFICATION.md](VERIFICATION.md) lists the probes and their limits.

## Clingo and demand-driven queries

Clingo was to run in its own process, so that its memory is measured apart from the harness. It does:
`engine/connectors/clingo_runner.py` runs it, and since October 2026 the runner writes one pair per
line, so Clingo's results are checked like those of every other system. For demand-driven
queries, `generate_db.py` writes a random start node next to every input (`queries_<n>.csv`), and the
connectors pass it to the rules.

## The web interface

The wizard was to offer a choice of domain and of query mode (full materialization or demand-driven);
it does, together with the campaign name and the time limit. The charts were to compare time and
memory, and they do so on the campaign pages and in the results explorer, which is organized by
campaign, series, topology, mode and size. The fixed order XSB, Clingo, Souffle for charts was
replaced by one rule for every figure: each system keeps its own colour and marker, and legends list
the systems in the order in which their curves end (`engine/plot_style.py`).

## Scripts for LaTeX output

`generate_plot_table.py` and `generate_scale_free_table.py` were to follow the new layout of the
results. Both were retired instead. `analyze_verified.py` writes every table row and every figure of
a campaign, the figures both as matplotlib PDF and as pgfplots/TikZ documents, from the records in
`runs.jsonl`.
