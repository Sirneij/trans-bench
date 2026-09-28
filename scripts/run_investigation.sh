#!/bin/bash
# Re-runs the MariaDB wrong-result cases with default settings, standard-compliant CTEs, and 4 GB
# in-memory temporary tables; one JSON line per case (preceded by a header line) in $1.
# Start MariaDB alone first (see docs/REPRODUCING.md).
cd "$(dirname "$0")/.."
O=$1; : > $O
PY=${PYTHON:-./virtualenv/bin/python}
for args in "barabasi_albert 100000 left" "scale_free 20000 right"; do
  for cfg in "STD_CTE=0" "STD_CTE=1" "TMP_BYTES=4294967296"; do
    echo "{\"env\": \"$cfg\", \"args\": \"$args\", \"started\": \"$(date -u +%FT%TZ)\"}" >> $O
    env $cfg $PY scripts/investigate_mariadb.py $args 2>&1 | tail -1 >> $O
  done
done
echo INVESTIGATION_DONE >> $O
