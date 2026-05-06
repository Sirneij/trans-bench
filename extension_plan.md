# Extending Trans-Bench: Generalizing for Advanced Recursive Benchmarking

This document outlines the conceptual approach for evolving Trans-Bench from a pure transitive closure benchmark into a generalized, advanced recursive query benchmarking suite. It addresses three major functional requests: complex logic programs, demand-driven queries, and space measurement.

---

## 1. Complex Logic Programs (Negation, Aggregation, Multiple Predicates)

**Feasibility:** Highly Feasible  
**Goal:** Support benchmarks like Shortest Path (Aggregation), Same Generation (Multiple Predicates), and Reachability with Avoidance (Negation).

### Implementation Approach:
- **Introduce the "Benchmark Domain" Abstraction:**
  Currently, rules and queries are hardcoded to assume standard reachability. We will introduce a new layer, `BenchmarkDomain`. A domain dictates the schema of the generated graph, the logic required, and the variants.
  - *Domain Examples*: `transitive_closure`, `shortest_path`, `same_generation`.
- **Expand `DataGenerator` (`generate_db.py`):**
  - **Aggregation**: Graph generation must output edge weights (e.g., `edge(source, target, weight)`).
  - **Multiple Predicates**: The generator might output a multi-relational dataset (e.g., `parent.csv` and `person.csv`).
- **System Rule Configurations:**
  Instead of looking strictly for `transitive_left_recursion.P`, the framework will look for rule files mapped to the specific domain. For example, in SQL systems, this means constructing recursive CTEs that utilize `MIN() / SUM()` for aggregation, or `EXCEPT` clauses for negation.

---

## 2. Demand-Driven Queries (Top-Down / Magic Sets vs Bottom-Up)

**Feasibility:** Very Feasible  
**Goal:** Allow the benchmarking of query performance when searching for specific sub-graphs (e.g., "is node Y reachable from node X?" or "find all nodes reachable from X") rather than computing the full graph materialization.

### Implementation Approach:
- **Query Node Generation:**
  The `DataGenerator` will be updated to output a secondary `queries.txt` or `queries.csv` file alongside the graph. This file contains a sample of randomized "bound variables" (e.g., starting nodes). Care must be taken to sample nodes with varying reachability distributions to prevent skewed query times.
- **Parametrized Connectors (`subprocess_conn.py`, etc.):**
  The `run_experiment` method will accept a new parameter: `query_bindings`.
  - *For Databases (SQL/Graph)*: The connector injects a specific `WHERE` or `MATCH` clause based on the binding (e.g., `SELECT * FROM shortest_path WHERE source = 5`).
  - *For Logic Systems (XSB/Soufflé)*: The connector passes the query via command-line injection (e.g., `?- path(5, Y)`) instead of materializing all outputs.
- **UI Integrations:**
  Introduce a new parameter in the "New Experiment" wizard to toggle between "Full Materialization" and "Demand-Driven".

---

## 3. Measuring Space (Memory and Disk Usage)

**Feasibility:** Feasible (requires a hybrid profiling architecture)  
**Goal:** Track memory consumption and disk footprint in addition to Real/CPU time.

### The Challenge with "Internal Metrics":
While it is ideal to use a system's internal metrics, this suite tests two fundamentally different types of architectures:
1. **Server Daemons (Databases)**: Systems like PostgreSQL or Neo4j run continuously. Using OS-level memory profiling on the server process is inaccurate because it includes background noise, shared buffers, and connection overhead. For these, we **must** use internal metrics (e.g., `EXPLAIN ANALYZE`, `pg_stat_activity`, or JMX MBeans).
2. **Ephemeral Binaries (Logic Engines)**: Systems like XSB, Soufflé, or Clingo are executed as raw, standalone processes that terminate upon completion. They typically do not expose internal telemetry servers or standard memory APIs. For these, OS-level metrics are the *only* reliable way to measure peak consumption.

### Implementation Approach (Hybrid Profiling):
- **For Ephemeral Binaries (OS-Level Tracking)**:
  Python's `psutil` library or the Unix `/usr/bin/time -v` command will be used to track the Maximum Resident Set Size (Peak RAM) of the spawned subprocess.
- **For Server Daemons (Internal Telemetry)**:
  The connector will explicitly execute internal telemetry queries before, during, and after the benchmark (or use `EXPLAIN ANALYZE` equivalents) to isolate the exact memory and disk footprint allocated to the query.
- **CSV Logging & UI Analytics**:
  - Update the CSV generator to append `PhaseMaxRAM_MB` and `PhaseDiskWrite_MB` alongside existing temporal metrics.
  - The Result UI will receive a new toggle allowing users to switch the Y-axis of the "Phase Trends" line chart from "Time (s)" to "Memory (MB)", enabling immediate visual comparison of memory overheads.

---

## 4. Future-Proofing: Dynamic Configuration Sweeping

**Feasibility:** Very Feasible  
**Goal:** Ensure the suite is generic enough to automatically test dynamic performance fine-tuning (e.g., buffer sizes, thread counts, compilation flags) to find optimal configurations.

### Implementation Approach:
- **Configuration Parameter Grids:**
  The `descriptor.yaml` schema will be extended to support `parameter_sweeps`. Instead of a static flag like `threads: 4`, users can define `threads: [1, 2, 4, 8]` or `work_mem: ["64MB", "256MB", "1GB"]`.
- **Auto-Tuning Orchestration:**
  The experiment wizard will expand the benchmark matrix to multiply graph sizes by the parameter grid. The engine will automatically restart or reconfigure the target system for each parameter permutation.
- **Configuration-Aware UI:**
  The Results page will introduce a "Configuration Impact" chart, mapping parameter values (X-axis) against performance metrics (Y-axis) to visually identify bottlenecks or optimal tuning sweet spots.

---

### Conclusion

By integrating the "Benchmark Domain" concept, parameterizing query bindings in the connectors, and adding `psutil`/telemetry wrappers to the execution lifecycle, Trans-Bench will seamlessly transition into a world-class, generalized benchmark suite capable of thoroughly testing the limits of any recursive system.
