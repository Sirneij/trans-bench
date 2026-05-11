# Trans-Bench Extension: Requirements & Specifications

This document outlines the requirements and architectural standards established for generalizing Trans-Bench into a multi-domain recursive query benchmarking suite where it must be easily extensible by anyone without requiring Python knowledge.

## 1. Domain Generalization & Core Architecture
- **Multi-Domain Support**: The system must support arbitrary recursive logic domains (e.g., `transitive`, `shortest_path`, `same_generation`) rather than being hardcoded to reachability.
- **Domain-Aware Directory Structure**: Results must be partitioned by domain to prevent data collision.
  - Standard: `timing/{domain}/{system}/{graph}/{mode}_graph_{size}.csv`
- **Standardized Data Reporting**: 
  - Transition from single-file outputs to structured CSVs.
  - Decouple temporal metrics (Time) from resource metrics (Memory).
  - Support multi-phase measurement (e.g., LoadFacts, Querying, Writing).

## 2. Hybrid Resource Profiling (Memory Tracking)
- **Peak RAM Measurement**: Implement OS-level process polling to capture "Peak RSS" (Resident Set Size) for all ephemeral logic engines.
- **System-Wide Integration**: Integrate `psutil` wrappers into the `BaseConnector` to ensure uniform memory tracking across:
  - **XSB**: Capturing internal Prolog heap/stack consumption via OS polling.
  - **Soufflé**: Monitoring the memory footprint of the compiled C++ binary.
  - **Clingo**: Isolating engine memory from the Python orchestrator.
  - **Databases**: Preparing for internal telemetry (e.g., JMX, `pg_stat_activity`) for server-side profiling.

## 3. System-Specific Modernization
- **Clingo Sandbox Isolation**: Refactor `ClingoConnector` to execute in an isolated `clingo_runner.py` subprocess. This is critical for measuring Clingo's standalone memory footprint without interference from the Flask server process.
- **Demand-Driven Query Logic**: 
  - The `DataGenerator` must support generating randomized query bindings (e.g., specific start/end nodes).
  - Connectors must support `query_bindings` injection to test Top-Down vs. Bottom-Up execution.

## 4. UI/UX & Visualization Standards
- **Dynamic Configuration Wizard**: 
  - Add a "Benchmark Domain" selector to the experiment launch flow.
  - Allow toggling between "Full Materialization" and "Demand-Driven" benchmarks.
- **Performance Trend Analytics**:
  - The UI must support dual-axis or toggleable charts to compare **Time (s)** and **Memory (MB)** trends.
  - Implement a logical system ordering in charts: **XSB → Clingo → Soufflé**.
- **Data Exploration**: The "Data Explorer" sidebar must be hierarchical: `Domain > System > Graph > Size/Mode`.

## 5. Script & Tooling Synchronization
- **LaTeX Generation**: Update `generate_plot_table.py` and `generate_scale_free_table.py` to:
  - Handle the new directory hierarchy (`parts[2]` for system).
  - Support the removal of the legacy `timing_` filename prefix.
  - Enforce the XSB-Clingo-Soufflé ordering in generated LaTeX documents.
