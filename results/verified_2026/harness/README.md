# Harness changes used for the 2026 campaign

The campaign ran on upstream `main` at commit 92ecc775 (the original harness, with `analyze_dbs.py`
and `analyze_logic_systems.py`), plus the commits in this directory, applied in file-name order
(the file names are the commit subjects). The patches are the output
of `git format-patch` and apply with `git am` on 92ecc775. Patches 0006 and 0007 are not included:
they only added the scale-free/Barabási-Albert input files (about 90 MB of diff), which
`generate_db.py` regenerates byte for byte (`scripts/verify_inputs.py`, `input/SHA256SUMS`). The
15 patches apply cleanly without them and give exactly the code that produced the records:

```sh
cp -R results/verified_2026/harness /tmp/harness-2026   # this directory does not exist at 92ecc775
git checkout -b rerun-2026 92ecc775
git am /tmp/harness-2026/*.patch
```

How each change maps to the current code: [docs/REPRODUCING.md](../../../docs/REPRODUCING.md#harness-used-in-2026-and-how-it-maps-to-this-code).
