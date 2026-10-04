"""
Check transitive-closure results independently of the systems under test.

A result is summarized by (number of pairs, order-independent 64-bit hash of the pairs), where the
hash is the sum modulo 2^64 of splitmix64((x << 32) | y) over all output lines. Duplicated,
missing, or wrong pairs change the count or the hash. The expected summary is computed from the
edge file with a plain breadth-first search in Python, independent of every system under test.

The hash is a checksum, not a proof: two different sets of pairs with the same count collide with
probability about 2^-64. It is used because it can be computed in one streaming pass over result
files of several GB, from any of the systems' output formats (CSV with or without header, TSV,
XSB's "x y" text), without holding the result in memory.
"""

import json
import re
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np

MASK = np.uint64(0xFFFFFFFFFFFFFFFF)


def splitmix64(v: np.ndarray) -> np.ndarray:
    """Apply the splitmix64 finalizer to every element (a well-mixed 64-bit hash of a 64-bit key)."""
    with np.errstate(over='ignore'):
        z = v + np.uint64(0x9E3779B97F4A7C15)
        z = (z ^ (z >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
        z = (z ^ (z >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
        return z ^ (z >> np.uint64(31))


def summarize_pairs(xs: np.ndarray, ys: np.ndarray) -> tuple[int, int]:
    """Return (number of pairs, sum of their hashes modulo 2^64) for the pairs (xs[i], ys[i])."""
    keys = (xs.astype(np.uint64) << np.uint64(32)) | ys.astype(np.uint64)
    with np.errstate(over='ignore'):
        h = int(np.sum(splitmix64(keys), dtype=np.uint64))
    return int(len(keys)), h


_DROP = bytes.maketrans(b',\t"', b'   ')


def summarize_file(path: Path, chunk_bytes: int = 64 << 20) -> tuple[int, int]:
    """Stream a result file (CSV/TSV/XSB text, optional header line) into (count, hash)."""
    count, total = 0, 0
    with open(path, 'rb') as f:
        first = f.readline()
        pending = b'' if re.search(rb'[A-Za-z]', first) else first  # skip a header line
        while True:
            block = f.read(chunk_bytes)
            data = pending + block
            if not block:
                pending = b''
            else:
                cut = data.rfind(b'\n') + 1
                data, pending = data[:cut], data[cut:]
            if data.strip():
                nums = np.array(data.translate(_DROP).split(), dtype=np.int64)
                if len(nums) % 2:
                    raise ValueError(f'odd number of integers in {path}')
                c, h = summarize_pairs(nums[0::2], nums[1::2])
                count += c
                total = (total + h) & 0xFFFFFFFFFFFFFFFF
            if not block:
                break
    return count, total


def read_edges(edge_file: Path) -> np.ndarray:
    """Read a tab-separated edge file as an array of shape (edges, 2)."""
    return np.loadtxt(edge_file, dtype=np.int64, ndmin=2)


def expected_summary(edge_file: Path) -> tuple[int, int]:
    """Compute (count, hash) of the transitive closure of an edge file by a search from every node."""
    adj = defaultdict(set)
    for a, b in read_edges(edge_file).tolist():
        adj[a].add(b)
    count, total = 0, 0
    for s in list(adj):
        seen, stack = set(), list(adj[s])
        while stack:
            v = stack.pop()
            if v not in seen:
                seen.add(v)
                stack.extend(adj.get(v, ()))
        ys = np.fromiter(seen, dtype=np.int64, count=len(seen))
        c, h = summarize_pairs(np.full(len(ys), s, dtype=np.int64), ys)
        count += c
        total = (total + h) & 0xFFFFFFFFFFFFFFFF
    return count, total


DEFAULT_CACHE = Path('input') / 'expected_closures.json'


def cached_expected(edge_file: Path, cache_file: Path = DEFAULT_CACHE) -> dict:
    """
    Return the expected {'count', 'hash'} of an edge file, cached by its path.

    The cache is safe because the inputs are deterministic, and their bytes are checked against
    input/SHA256SUMS by scripts/verify_inputs.py.
    """
    cache_file = Path(cache_file)
    cache = json.loads(cache_file.read_text(encoding='utf-8')) if cache_file.exists() else {}
    key = str(edge_file)
    if key not in cache:
        count, digest = expected_summary(edge_file)
        cache[key] = {'count': count, 'hash': f'{digest:016x}'}
        cache_file.write_text(json.dumps(cache, indent=1, sort_keys=True), encoding='utf-8')
    return cache[key]


def main(argv: list[str]) -> None:
    """Check one result file against its edge file and print the verdict as JSON."""
    if len(argv) != 2:
        sys.exit('usage: python -m engine.verify <input/souffle/<graph>/<n>/edge.facts> <result file>')
    expected = cached_expected(Path(argv[0]))
    count, digest = summarize_file(Path(argv[1]))
    got = {'count': count, 'hash': f'{digest:016x}'}
    print(json.dumps({'expected': expected, 'got': got, 'correct': expected == got}))


if __name__ == '__main__':
    main(sys.argv[1:])
