# Harness of the first 2026 campaign

The first verified campaign ran on upstream `main` at commit 92ecc775, the original harness with
`analyze_dbs.py` and `analyze_logic_systems.py`, together with the commits stored here as patches.
The patches were written by `git format-patch`; their file names are the commit subjects, and they
are applied in the order of those names.

Patches 0006 and 0007 are left out. They only added the input files of the scale-free and
Barabási-Albert graphs, about 90 MB of diff, and `generate_db.py` writes the same files byte for
byte (`scripts/verify_inputs.py` checks them against `input/SHA256SUMS`). The other 15 patches apply
cleanly without them and give the exact code that produced the records:

```sh
cp -R results/verified_2026/harness /tmp/harness-2026   # the directory does not exist at 92ecc775
git checkout -b rerun-2026 92ecc775
git am /tmp/harness-2026/*.patch
```

The copy is needed because `git checkout` removes this directory. How each change appears in the
present code is described in
[docs/REPRODUCING.md](../../../docs/REPRODUCING.md#the-2026-harness-and-where-its-changes-live-now).
