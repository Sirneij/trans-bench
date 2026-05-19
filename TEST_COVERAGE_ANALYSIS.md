# Trans-Bench Engine Module - Test Coverage Analysis

## Overview

This document identifies test coverage gaps in the trans-bench engine module. The analysis examined:

- 5 core engine source files
- 7 test files
- ~2,500 lines of source code
- ~1,200 lines of test code

**Coverage Summary**: ~60% of public methods/functions have tests. Multiple critical gaps identified.

---

## CRITICAL GAPS

### 1. **ExperimentRunner.\_run_single() - Exception Handling**

**File**: [engine/runner.py](engine/runner.py#L115-L155)  
**What it does**: Orchestrates single experiment runs with connector lifecycle (connect → run → close)  
**Not Tested**:

- Exception handling path (the `except Exception` block at L148)
- Error logging and error emission via `_emit_log()`
- Connector failure recovery
- Proper cleanup (`connector.close()`) when errors occur
- Memory collection through `gc.collect()`

**Why Critical**: This is the core timing measurement execution point. Errors here could silently fail and corrupt results.

**Should be in**: `test_runner.py`

**Priority**: **HIGH**

---

### 2. **ExperimentRunner.\_generate_input_data() - Subprocess Failure Modes**

**File**: [engine/runner.py](engine/runner.py#L245-L275)  
**What it does**: Runs `generate_db.py` as subprocess to create missing graph inputs  
**Not Tested**:

- Non-zero return codes from `generate_db.py`
- Stderr parsing and error emission
- Empty stdout/stderr
- Missing graphs after subprocess completion
- Partial failures (some graphs generated, others missing)

**Why Critical**: Data generation failures can cause silent test skips or corrupt benchmark runs.

**Should be in**: `test_runner.py`

**Priority**: **HIGH**

---

### 3. **ExperimentRunner.\_all_modes_exist()**

**File**: [engine/runner.py](engine/runner.py#L220-L222)  
**What it does**: Checks if all required mode result files exist for a (system, graph, size) combo  
**Not Tested**:

- Partial mode existence (some modes exist, others missing)
- Empty modes list
- Symlinks or permission issues

**Why Critical**: Skips entire benchmark runs; corruption here is silent.

**Should be in**: `test_runner.py`

**Priority**: **MEDIUM**

---

### 4. **ExperimentRunner.\_resolve_rule_path() - All Fallback Modes**

**File**: [engine/runner.py](engine/runner.py#L164-L198)  
**What it does**: Multi-level fallback rule file resolution for domain/mode combinations  
**Not Tested**:

- Full domain-prefixed resolution (new: `transitive_closure_right_recursion.P`)
- Shortened domain resolution (`transitive_right_recursion.P`)
- Plain mode fallback (`right_recursion.P`)
- All combinations of the above
- Returns `None` correctly when no files found
- Correct warning messages

**Why Critical**: Rule file not found is a silent failure (just logs warning); benchmark silently skipped.

**Should be in**: `test_runner.py`

**Priority**: **HIGH**

---

### 5. **ExperimentRunner.run() - Complete Lifecycle with All Edge Cases**

**File**: [engine/runner.py](engine/runner.py#L105-L160)  
**What it does**: Full experiment orchestration loop  
**Currently Tested**: Basic happy path with mocked filesystem  
**Gaps**:

- Empty systems/graphs/modes lists
- Size range edge cases (empty, single size, reverse order)
- All modes being skipped (none match system.modes)
- Multiple consecutive skips
- Progress callback being None
- Domain descriptor not found (falls back to loader)
- Domain declaring invalid modes that don't intersect with requested modes

**Should be in**: `test_runner.py`

**Priority**: **MEDIUM**

---

### 6. **DescriptorLoader - Comprehensive API Coverage**

**File**: [engine/loader.py](engine/loader.py#L220-L400)  
**What it does**: Discovers and parses YAML descriptors from filesystem  
**Not Tested**:

- `load_systems()` with specific `names` parameter filtering
- `load_graph_types()` with specific `names` parameter filtering
- `load_domains()` with specific `names` parameter filtering
- `load_global_config()` caching behavior (second call returns cached)
- `save_system_credentials()` - file actually written and re-readable
- `save_descriptor()` - descriptor round-trips correctly
- `get_system()` returning `None` for nonexistent system
- `get_domain()` returning `None` for nonexistent domain
- Empty credentials.yaml files
- Descriptor files with missing optional fields

**Currently Tested**: Only basic happy path and error handling

**Should be in**: `test_loader.py`

**Priority**: **MEDIUM**

---

### 7. **RuleValidator.validate_system() - All Check Branches**

**File**: [engine/validation.py](engine/validation.py#L30-L60)  
**What it does**: Validates all aspects of a system descriptor  
**Not Tested**:

- System not found (returns False correctly)
- Individual check failures:
  - `_check_descriptor()` each required field empty
  - `_check_rule_files()` with legacy non-prefixed modes
  - `_check_credentials()` when requires_credentials=false
  - `_check_credentials()` when file missing but not required
- Mixed pass/fail scenarios

**Currently Tested**: Mostly success path and one-off failures

**Should be in**: `test_validation.py`

**Priority**: **MEDIUM**

---

### 8. **RuleValidator.\_live_dry_run() - All Protocol Branches**

**File**: [engine/validation.py](engine/validation.py#L345-L400)  
**What it does**: Attempts to validate rule syntax by dry-running against actual system  
**Not Tested**:

- Protocol not supported (early return True)
- System descriptor not found
- Missing credentials in descriptor
- Connection failures (timeouts, auth errors)
- Dry-run parsing succeeded (EXPLAIN worked)
- Import errors (psycopg2 or duckdb not installed)

**Currently Tested**: Only DuckDB and PostgreSQL success paths

**Should be in**: `test_validation.py`

**Priority**: **MEDIUM**

---

### 9. **get_connector() - Invalid Protocol**

**File**: [engine/connectors/**init**.py](engine/connectors/__init__.py#L120-L135)  
**What it does**: Retrieves connector class by protocol string  
**Not Tested**:

- Unknown protocol raises `ValueError` with helpful message
- Message includes list of valid protocols
- Unknown protocol doesn't return None or default

**Currently Tested**: Only implicitly (via concrete connector tests)

**Should be in**: `test_connectors.py` (or new dedicated test)

**Priority**: **LOW** (but simple to add)

---

### 10. **\_load_plugin_connectors() - Drop-in Connector Discovery**

**File**: [engine/connectors/**init**.py](engine/connectors/__init__.py#L47-L110)  
**What it does**: Auto-discovers custom connector.py files in systems/ directories  
**Not Tested** (at all):

- Successful discovery and registration
- Multiple connectors registered
- Missing descriptor.yaml (should skip with warning)
- Missing protocol field in descriptor (should skip with warning)
- Protocol already registered (should skip with debug log)
- Invalid connector file (ImportError, syntax error)
- Connector class not ending in "Connector"
- Connector not subclassing BaseConnector
- Actual protocol name used from descriptor

**Currently Tested**: Not tested

**Should be in**: `test_connectors.py`

**Priority**: **HIGH** (core feature, zero coverage)

---

### 11. **Subprocess Connectors - Query Patching and Binding**

**Files**:

- [engine/connectors/subprocess_conn.py](engine/connectors/subprocess_conn.py#L90-L130) (ClingoConnector.\_patch_rule_file)
- [engine/connectors/subprocess_conn.py](engine/connectors/subprocess_conn.py#L180-L220) (SouffleConnector.\_patch_rule_file)

**What it does**: Dynamically patches rule files to inject custom query predicates  
**Not Tested**:

- Query pattern matching (regex finds the query)
- Query pattern matching failure (returns None)
- Query substitution in rule content
- Temp file cleanup in both success and exception paths
- Multiple query patterns in same file
- Malformed queries

**Currently Tested**: Not tested

**Should be in**: `test_connectors.py`

**Priority**: **HIGH** (complex logic, affects results)

---

### 12. **Subprocess Connectors - Timing Extraction Edge Cases**

**File**: [engine/connectors/subprocess_conn.py](engine/connectors/subprocess_conn.py)  
**What it does**: Parses timing strings from subprocess stdout (e.g., "LoadRuleTime: 1.2")  
**Not Tested**:

- Missing timing keys in stdout (should return 0.0)
- Malformed numbers (e.g., "LoadRuleTime: abc")
- Scientific notation in output (e.g., "1.2e-3")
- No match for regex (should return 0.0)
- Multiple entries of same key (which one wins?)
- Negative values in output

**Currently Tested**: Only basic happy path

**Should be in**: `test_connectors.py`

**Priority**: **MEDIUM**

---

### 13. **BaseConnector.\_substitute_query_bindings()**

**File**: [engine/connectors/base.py](engine/connectors/base.py#L29-L44)  
**What it does**: Replaces `?param_name` placeholders with actual values  
**Currently Tested**: Only basic case  
**Gaps**:

- Multiple occurrences of same placeholder
- Placeholder not in content (no substitution)
- `?param` without underscore vs `?param_name`
- Special characters in values (quotes, semicolons)
- None values
- Empty string values
- `query_bindings=None` (already tested)

**Should be in**: `test_connectors.py`

**Priority**: **LOW**

---

### 14. **RuleValidator.\_static_check() - Language-Specific Checks**

**File**: [engine/validation.py](engine/validation.py#L280-L340)  
**What it does**: Performs syntax analysis on rule files (SQL, Datalog, Cypher, Prolog, DistAlgo, JS)  
**Gaps**:

- Each language type gets routed to correct checker (only `.sql` fully tested)
- Each language-specific checker (`_check_sql`, `_check_datalog`, etc.) has individual edge cases:
  - **SQL**: Multiline statements, comments, escaped quotes
  - **Datalog**: Multiple rules, facts without rules, queries
  - **Cypher**: Multiple patterns, complex returns
  - **Prolog**: Multiple clauses
  - **DistAlgo**: Multiple functions, class definitions
  - **JavaScript**: Arrow functions, async/await

**Currently Tested**: Only basic happy path for each language

**Should be in**: `test_validation.py`

**Priority**: **MEDIUM**

---

### 15. **ExperimentRunner.\_timing_path() and \_prepare_output_folder()**

**File**: [engine/runner.py](engine/runner.py#L223-L240)  
**What it does**: Constructs filesystem paths and creates directories  
**Not Tested**:

- Directory creation with deep nesting
- Permission errors during mkdir
- Path construction with special characters in names
- Symlink handling

**Currently Tested**: Only via integration in other tests

**Should be in**: `test_runner.py`

**Priority**: **LOW** (simple, mostly tested indirectly)

---

### 16. **ExperimentRunner.\_write_timing() and \_append_average()**

**File**: [engine/runner.py](engine/runner.py#L239-L270)  
**What it does**: Writes and aggregates CSV timing data  
**Not Tested**:

- Writing to nonexistent file (should create)
- Appending to existing file
- Average calculation with 0 runs
- Average calculation with mixed missing/present values
- CSV escaping of special characters
- Float rounding edge cases
- Large numbers in timing

**Currently Tested**: Only via integration test (and partially mocked)

**Should be in**: `test_runner.py`

**Priority**: **MEDIUM**

---

### 17. **ExperimentRunner.\_resolve_input_path() - All Format Types**

**File**: [engine/runner.py](engine/runner.py#L199-L210)  
**What it does**: Maps system input_format to correct input file path  
**Not Tested**:

- Each format type (tsv, lp, facts, pickle, unknown)
- Unknown format fallback
- Path construction correctness for each

**Currently Tested**: Only indirectly via integration

**Should be in**: `test_runner.py`

**Priority**: **MEDIUM**

---

### 18. **BootstrapManager - All Methods**

**File**: [engine/bootstrap.py](engine/bootstrap.py)  
**What it does**: Scaffolds new systems, domains, and graph types  
**Currently Tested**: Basic happy path only  
**Gaps**:

- `bootstrap_system()` with different templates
- `bootstrap_domain()` with different templates
- `bootstrap_graph()` parameter validation
- `copy_rule_template()` actually creates file
- `list_templates()` returns correct categories
- Error cases: duplicate names, missing templates, permission errors
- YAML output formatting

**Should be in**: `test_bootstrap.py`

**Priority**: **LOW** (scaffolding tool, less critical)

---

### 19. **ParameterSweep.expand() - Edge Cases**

**File**: [engine/parameter_sweep.py](engine/parameter_sweep.py#L50-L110)  
**What it does**: Generates all combinations of parameter values  
**Currently Tested**: Basic case with multiple parameters  
**Gaps**:

- Single parameter sweep
- Overlapping/conflicting parameters
- Very large sweeps (performance)
- Parameter type conversions (int→str, float→str)
- Order preservation in combinations
- Empty parameter lists

**Should be in**: `test_parameter_sweep.py`

**Priority**: **LOW**

---

### 20. **ParameterSweep.count()**

**File**: [engine/parameter_sweep.py](engine/parameter_sweep.py#L112-L135)  
**What it does**: Calculates total combinations without expanding  
**Not Tested**:

- Empty sweep (should be 1)
- Single parameter
- Zero values in any parameter list

**Should be in**: `test_parameter_sweep.py`

**Priority**: **LOW**

---

---

## HIGH PRIORITY GAPS

### Summary Table

| #   | Component                   | Gap                     | Test File          | Est. Effort |
| --- | --------------------------- | ----------------------- | ------------------ | ----------- |
| 1   | `_run_single()`             | Exception handling      | test_runner.py     | 2h          |
| 2   | `_generate_input_data()`    | Subprocess failures     | test_runner.py     | 1h          |
| 4   | `_resolve_rule_path()`      | All 3 fallback modes    | test_runner.py     | 1.5h        |
| 10  | `_load_plugin_connectors()` | Complete feature        | test_connectors.py | 3h          |
| 11  | Query patching              | Regex/temp file cleanup | test_connectors.py | 1.5h        |

**Total HIGH Priority Effort**: ~9 hours

---

## MEDIUM PRIORITY GAPS

### Summary Table (Selection)

| #   | Component               | Gap               | Est. Effort |
| --- | ----------------------- | ----------------- | ----------- |
| 5   | `run()`                 | Edge cases        | 2h          |
| 6   | `DescriptorLoader`      | API completeness  | 2h          |
| 7   | `validate_system()`     | Check branches    | 1.5h        |
| 8   | `_live_dry_run()`       | Protocol branches | 2h          |
| 12  | Timing extraction       | Edge cases        | 1h          |
| 14  | Static checks           | Language coverage | 3h          |
| 16  | CSV operations          | Edge cases        | 1.5h        |
| 17  | `_resolve_input_path()` | All formats       | 1h          |

**Total MEDIUM Priority Effort**: ~14.5 hours

---

## LOW PRIORITY GAPS

### Summary Table (Selection)

| #   | Component         | Gap              | Est. Effort |
| --- | ----------------- | ---------------- | ----------- |
| 9   | `get_connector()` | Invalid protocol | 0.5h        |
| 13  | Query binding     | Edge cases       | 0.5h        |
| 15  | Path/mkdir        | Edge cases       | 1h          |
| 18  | BootstrapManager  | All methods      | 2h          |
| 19  | ParameterSweep    | Edge cases       | 1h          |
| 20  | `count()`         | Edge cases       | 0.5h        |

**Total LOW Priority Effort**: ~5.5 hours

---

## RECOMMENDED TEST ORDER

### Phase 1 (Immediate - Critical Data Integrity)

1. `_resolve_rule_path()` - prevents silent test skips
2. `_run_single()` exception handling - prevents corrupted results
3. `_generate_input_data()` subprocess failures - prevents missing data

**Est. Effort**: 4.5 hours

### Phase 2 (High Impact - Core Features)

4. `_load_plugin_connectors()` - enables entire connector discovery feature
5. Query patching in subprocess connectors - affects custom queries
6. `run()` lifecycle edge cases - prevents logic errors

**Est. Effort**: 6.5 hours

### Phase 3 (Data Quality - Validation/Analysis)

7. `DescriptorLoader` API completeness
8. `RuleValidator` branches
9. CSV operations edge cases

**Est. Effort**: 5 hours

### Phase 4 (Nice to Have)

10. Remaining LOW priority items
11. Performance/stress tests

**Est. Effort**: 5.5 hours

---

## TESTING PATTERNS TO ADOPT

### Pattern 1: Mock Filesystem for Path Tests

```python
@pytest.fixture
def tmp_engine(tmp_path):
    # Create minimal engine structure
    return create_engine_fixture(tmp_path)
```

### Pattern 2: Exception Handling Tests

```python
def test_connector_exception_handling():
    # Arrange: Mock connector that raises exception
    # Act: Call _run_single()
    # Assert: close() called, error logged, results not written
```

### Pattern 3: Subprocess Output Parsing

```python
@patch('engine.connectors.base.BaseConnector.timed_subprocess')
def test_timing_extraction_edge_cases(mock_timed_subproc):
    # Test with missing keys, malformed numbers, etc.
```

### Pattern 4: Filesystem Traversal Tests

```python
def test_load_descriptors_with_filter(tmp_path):
    # Create multiple descriptors, test names filtering
```

---

## ESTIMATED TOTAL EFFORT

| Priority  | Hours    | Notes                    |
| --------- | -------- | ------------------------ |
| Critical  | 4.5      | Prevents test corruption |
| HIGH      | 9        | Core functionality       |
| MEDIUM    | 14.5     | Data quality             |
| LOW       | 5.5      | Nice to have             |
| **TOTAL** | **33.5** | **~1 week at 5h/day**    |

---

## FILES MOST IN NEED OF TESTS

1. **engine/connectors/**init**.py** - 0% coverage for plugin discovery
2. **engine/connectors/subprocess_conn.py** - ~30% coverage (patching/timing untested)
3. **engine/runner.py** - ~40% coverage (exception paths, edge cases)
4. **engine/validation.py** - ~50% coverage (live dry-run, language checks)
5. **engine/loader.py** - ~60% coverage (API filtering, persistence)

---

## NOTES FOR IMPLEMENTATION

- **Mock Strategy**: Use `@patch` + `unittest.mock.MagicMock` for external systems (filesystem, subprocesses, DB connections)
- **Fixture Strategy**: Create reusable `tmp_engine` fixture with minimal valid structure
- **Parametrize**: Use `@pytest.mark.parametrize` for multi-mode tests (SQL/Cypher/Datalog, multiple formats, etc.)
- **Integration Tests**: Keep existing integration tests; add new unit tests for specific gaps
- **CI/CD**: Ensure all new tests pass in automated pipeline
