r"""
Check that the benchmark inputs are byte-identical to the ones used for the verified runs.

    python scripts/verify_inputs.py                 # check every file listed in input/SHA256SUMS
    python scripts/verify_inputs.py --graphs cycle  # only some graph types

input/SHA256SUMS lists the SHA-256 of every input file of the September 2026 campaign
(results/verified_2026). The files themselves are not all in git (about 200 MB); create them with

    python generate_db.py --sizes 100 1001 100 --graph-types complete max_acyclic cycle \
        cycle_with_shortcuts path multi_path grid binary_tree reverse_binary_tree x y w
    python generate_db.py --sizes 10000 100001 10000 --graph-types scale_free barabasi_albert

with the pinned requirements (networkx 3.7: the seeded scale-free/Barabási-Albert generators are
not guaranteed to give the same graph in other networkx versions). Exit code 1 if any file is
missing or different.
"""

import argparse
import hashlib
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent


def sha256(path: Path) -> str:
    """Return the SHA-256 of a file, read in blocks of 1 MB."""
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(1 << 20), b''):
            h.update(block)
    return h.hexdigest()


def main() -> int:
    """Check every file of the manifest; return the exit code."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--manifest', default=str(BASE / 'input' / 'SHA256SUMS'))
    ap.add_argument('--graphs', nargs='*', help='only check these graph types')
    ap.add_argument('--root', default=str(BASE), help='directory that contains input/ (default: repository)')
    a = ap.parse_args()

    ok = missing = bad = 0
    for line in Path(a.manifest).read_text(encoding='utf-8').splitlines():
        digest, rel = line.split(maxsplit=1)
        graph = Path(rel).parts[2]  # input/<souffle|clingo_xsb>/<graph>/...
        if a.graphs and graph not in a.graphs:
            continue
        path = Path(a.root) / rel
        if not path.exists():
            missing += 1
            print(f'MISSING   {rel}')
        elif sha256(path) != digest:
            bad += 1
            print(f'DIFFERENT {rel}')
        else:
            ok += 1
    print(f'{ok} identical, {bad} different, {missing} missing')
    return 0 if bad == missing == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
