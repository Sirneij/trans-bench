# Trans-Bench Platform Features

The Trans-Bench Web UI provides a comprehensive, end-to-end platform for orchestrating, executing, and analyzing complex transitive closure benchmarks across diverse database and logic systems. 

## 1. Experiment Orchestration & Execution
- **Wizard-Based Setup**: Seamlessly define benchmark parameters including graph topologies (Path, Cycle, Clique, Tree, etc.), target node sizes, and timeout limits.
- **Variant Selection**: Benchmark different rule evaluation strategies by selecting between Left, Right, and Double recursion modes.
- **Cross-Engine Benchmarking**: Run simultaneous benchmarks across Graph Databases (Neo4j), Relational Databases (PostgreSQL, DuckDB, MariaDB), and Datalog/Prolog engines (Soufflé, XSB, Clingo, Alda).

## 2. Live Benchmark Monitoring
- **Real-Time Execution Tracking**: Monitor the live status of running benchmarks with progress indicators tracking distinct phases (Data Generation, Loading, Indexing, and Querying).
- **Live Terminal Stream**: Inspect standard output directly from the underlying engine instances during execution to trace exact commands and quickly catch runtime failures or timeouts.

## 3. System Configuration & Query Management
- **Integrated Rule IDE**: An embedded code editor allows you to directly view, modify, and save system-specific queries and rules (`.sql`, `.py`, `.P`, `.dl`) without leaving the browser.
- **Credential Management**: Securely configure and update connection strings, user credentials, and engine-specific flags via an integrated YAML editor.
- **System Architecture Mapping**: View exactly which protocols, input formats, and timing phases map to each individual benchmarking target.

## 4. Advanced Result Analysis & Visualization
- **Scaling Performance Comparison**: Dynamically generate stacked bar charts to analyze how different systems handle increasing graph sizes.
- **Side-by-Side Variant Analysis**: Compare multiple recursion modes simultaneously (e.g., Left vs. Right recursion side-by-side) to evaluate query optimizer behavior under varying algorithmic constraints.
- **Phase Trend Tracking**: Isolate specific execution phases (e.g., Query Time vs. Data Load Time) to see performance trajectories via multi-line charts. 
- **Intelligent Phase Mapping**: The system automatically normalizes metrics across fundamentally different architectures (e.g., intelligently mapping Neo4j's "Write Result" to a Datalog engine's "Evaluation Time" for fair Query-phase comparisons).

## 5. Graph Topology Explorer
- **Mathematical Definitions**: Review the exact set-theoretic formulas and structural rules defining each graph type.
- **Implementation Inspection**: Dynamically extract and inspect the raw Python code responsible for generating the synthetic graph datasets directly from the `DataGenerator` core.