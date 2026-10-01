"""
engine/failures.py — why a benchmark run failed.

benchmark.py records the kind of every failed run in runs.jsonl (`failure`), and
analyze_verified.py uses the same rules for records written before that field existed.
"""

from __future__ import annotations

# Why a run failed, from its error messages (searched case-insensitively, first match wins).
FAILURE_PATTERNS = (
    ('oom', ('out of memory', 'outofmemory', "'maximum_memory' setting", 'memory budget exceeded',
             'exceeded memory limit', 'memoryerror', 'cannot allocate memory', 'failed to allocate',
             'memory exhausted', 'insufficient memory')),
    ('unsupported', ('must not appear more than once', 'restrictions imposed', 'not supported')),
    ('iteration_limit', ('max number of iterations', 'max_recursive_iterations', 'max_recursive_cte_iterations')),
)


def classify_failure(status: str, exit_code, errors: list[str]) -> str | None:
    """None for a successful run; otherwise timeout, oom, unsupported, iteration_limit, killed
    (ended by a signal that the driver did not send, e.g. the operating system's memory killer), or error."""
    if status == 'ok':
        return None
    if status == 'timeout':
        return 'timeout'
    text = ' '.join(errors).lower()
    for kind, patterns in FAILURE_PATTERNS:
        if any(pt.lower() in text for pt in patterns):
            return kind
    if isinstance(exit_code, int) and exit_code < 0:
        return 'killed'
    return 'error'
