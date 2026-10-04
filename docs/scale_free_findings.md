# Scale-free and Barabási-Albert graphs

Besides the twelve families of regular graphs, the campaigns measured two families that approximate
real networks: directed scale-free graphs (networkx `scale_free_graph`, 10,000 to 90,000 nodes) and
Barabási-Albert graphs (`barabasi_albert_graph` with m = 2, 10,000 to 100,000 nodes). Both have the
power-law degree distribution of social and biological networks and of the Web, with a few hubs of
very high degree and short paths between most nodes (Barabási and Albert, 1999). This note
summarizes how the systems fared on them in the paper's campaign (`results/verified_2026_v2`, left
and right recursion, five runs, a 600 s limit per run). The figures and tables of the paper give the
full picture; `results/verified_2026_v2/analysis/summary.csv` holds every value.

## Barabási-Albert graphs

On the Barabási-Albert graphs, every system except MongoDB completed every size up to 100,000 nodes.
DuckDB was by far the fastest, with a mean of 0.8 s (left recursion) at 100,000 nodes, followed by XSB
with right recursion (1.9 s) and PostgreSQL (9.5 s). CockroachDB needed 88 s and MariaDB 150 s. MongoDB
exceeded the limit from 40,000 nodes. MariaDB in its default configuration returned one pair too few
at 100,000 nodes with left recursion. This case is described in [SYSTEMS.md](SYSTEMS.md), where the
same query was found to be correct once the temporary tables were raised to 4 GB.

## Scale-free graphs

The scale-free graphs are much harder. At 20,000 nodes, the closure of a scale-free graph already has
19.1 million pairs, more than twice the 7.8 million pairs of the Barabási-Albert graph with 100,000
nodes. Only XSB with right recursion completed every size, taking 96 s at 90,000 nodes; with left
recursion it exceeded the limit at 70,000 nodes. In other words, for XSB the place of the recursive
call in the rule decides whether the largest graphs can be computed at all.

DuckDB completed up to 80,000 nodes (67 s with left recursion) and ran out of memory at 90,000,
because its intermediate results must fit in memory. PostgreSQL completed up to 50,000 nodes and
then exceeded the limit, as did CockroachDB from 20,000 (left) or 30,000 nodes (right) and MongoDB
from 30,000 nodes. MariaDB exceeded the limit from 20,000 nodes with left recursion; with right
recursion, its result at 20,000 nodes was incomplete. Neo4j, whose pruned breadth-first search
needed only 28 s at 20,000 nodes, exhausted its transaction memory at 30,000. SingleStore, which
must enumerate every path, ran out of memory on the smallest scale-free graph.

## Reading the two families together

The two families separate the systems that hold their intermediate results in memory from those
that spill them to disk. DuckDB and Neo4j were fast while their data fitted in memory and failed with
out-of-memory errors when they no longer did. PostgreSQL, CockroachDB and MariaDB rarely failed for
lack of memory, but they paid for it in time and met the 600 s limit instead. XSB's tabled right
recursion was the only formulation that carried through the largest scale-free graphs within the
limit.

Barabási, A.-L., & Albert, R. (1999). Emergence of scaling in random networks. *Science*, 286(5439),
509-512.
