#!/usr/bin/env bash
# The test suite with coverage, in parallel. The end-to-end tests use DuckDB and need no server;
# they share one DuckDB database file, so they are kept on one worker (--dist loadgroup).
#
#   scripts/test.sh                              # with the python on PATH
#   PYTHON=./virtualenv/bin/python scripts/test.sh -k campaign
set -euo pipefail
cd "$(dirname "$0")/.."
PY=${PYTHON:-python}

$PY -m pytest -n auto --dist loadgroup --cov --cov-report=term-missing:skip-covered "$@"
