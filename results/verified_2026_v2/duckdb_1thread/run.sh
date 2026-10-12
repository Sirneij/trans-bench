#!/bin/sh
# The commands that produced this series (October 2026), run from the repository root.
set -e
OUT=results/verified_2026_v2/duckdb_1thread
COMMON="--systems duckdb --modes left_recursion right_recursion --runs 5 --out $OUT --label threads=1 --config-file $OUT/duckdb_settings.yaml --no-analysis"
python benchmark.py $COMMON --graphs complete max_acyclic cycle_with_shortcuts cycle path multi_path grid binary_tree reverse_binary_tree x y w \
    --sizes 100 200 300 400 500 600 700 800 900 1000
python benchmark.py $COMMON --graphs barabasi_albert --sizes 10000 20000 30000 40000 50000 60000 70000 80000 90000 100000
python benchmark.py $COMMON --graphs scale_free --sizes 10000 20000 30000 40000 50000 60000 70000 80000 90000
