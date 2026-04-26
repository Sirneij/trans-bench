# Empirical Findings on Transitive Closure Computation for Scale-Free Graphs

## 1. Execution Time Analysis (PostgreSQL)
We evaluated the query execution times (`ExecuteQueryRealTime`) for computing transitive closure on scale-free graphs using PostgreSQL, testing graph sizes from 10,000 to 60,000 nodes. The findings demonstrate a non-linear, aggressive increase in execution time as graph size scales, establishing a clear computational bottleneck for traditional relational database systems on recursive workloads.

* **Left Recursion:** Execution time grew from **8.96s** (10k nodes) to **1095.05s** (60k nodes).
* **Right Recursion:** Execution time grew from **8.09s** (10k nodes) to **718.51s** (60k nodes).

**Observation:** Right recursion consistently outperforms left recursion in PostgreSQL for scale-free graphs. However, both strategies exhibit severe temporal degradation. By $N=60,000$, query times reach 12–18 minutes, making PostgreSQL computationally intractable for significantly larger scale-free datasets.

## 2. Temporal Efficiency vs. Memory Constraints (DuckDB Comparison)
By contrast, DuckDB exhibits superior temporal efficiency for the same workloads. For instance, at 60,000 nodes, DuckDB executes left recursion in **~25.7s** and right recursion in **~25.3s**, operating orders of magnitude faster than PostgreSQL. 

Despite its execution speed, DuckDB suffers from a fundamental structural limitation: **memory exhaustion**.

When scaling to $N \ge 90,000$ using right recursion, DuckDB encounters a fatal Out-of-Memory (OOM) error:
> `Out of Memory Error: failed to allocate data of size 8.0 GiB (11.4 GiB/14.3 GiB used)`

**Observation:** While PostgreSQL is bottlenecked by CPU time and execution inefficiency, DuckDB's vectorized execution engine aggressively consumes memory to maintain intermediate states during recursive Common Table Expressions (CTEs). Consequently, DuckDB reaches a hard memory wall before it encounters temporal degradation.

## 3. Conclusion
Our benchmarking of recursive queries on scale-free graphs reveals a distinct dichotomy between disk-oriented and in-memory analytical DBMS architectures:
1. **PostgreSQL** suffers from extreme **temporal constraints**, rendering it impractically slow for large-scale recursive tasks ($>60,000$ nodes), yet it avoids catastrophic memory failure by relying on disk spilling.
2. **DuckDB** offers highly optimized **temporal execution** but is strictly bounded by **memory capacity**, failing completely at sizes $N \ge 90,000$ due to the immense intermediate memory overhead required for recursive joins on dense scale-free networks. 

These findings underscore the trade-offs between execution speed and memory resilience in modern SQL engines when handling deep recursion.
