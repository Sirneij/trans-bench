#!/usr/bin/env bash
# Static checks of trans-bench: formatting (black, isort), linting (prospector), security (bandit)
# and types (mypy). GitHub Actions runs the same script (.github/workflows/ci.yml).
#
#   scripts/static_validation.sh                 # with the python on PATH
#   PYTHON=./virtualenv/bin/python scripts/static_validation.sh
set -euo pipefail
cd "$(dirname "$0")/.."
PY=${PYTHON:-python}

echo "== black"
$PY -m black --check .
echo "== isort"
$PY -m isort --check-only --diff .
echo "== prospector"
$PY -m prospector --profile=.prospector.yml
echo "== bandit"
$PY -m bandit -c pyproject.toml -r . -ll -q
echo "== mypy"
$PY -m mypy engine ui benchmark.py transitive.py analyze_verified.py generate_db.py
