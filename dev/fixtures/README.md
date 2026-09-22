# Reference fixture generation

These Apache-2.0 Rust generators are maintained here as the canonical
standalone test support. The shared PCO, Zstd, ALP, zoned-statistics and
default-writer sources originated in the Iceberg integration. The independent
corpus, legacy-statistics and nullable-Delta generators are maintained here.
Iceberg imports the shared sources and independent corpus at its pinned reader
revision; registration and date/decimal tooling stays in Iceberg. Generators
construct canonical source arrays and check original values using the Rust
reader. No Go writer is used. `Cargo.lock` pins registry dependencies,
including Pco 1.0.3.

From the repository root, using Rust 1.98.0 and Python with the pinned
Parquet-oracle dependency (`python3 -m pip install -r dev/fixtures/requirements.txt`):

```sh
RUSTUP_TOOLCHAIN=1.98.0 dev/fixtures/generate.sh \
  .cache/fixture-build .cache/regenerated
```

Use a fresh build directory, on disk. Set `VORTEX_BUILD_PROFILE=dev` to use
an unoptimized generator build; fixture semantics are unchanged. Existing
`CARGO_TARGET_DIR` and compiler cache settings remain effective. `VORTEX_GIT_URL` can select a local Git mirror
containing the recorded revision. The script fetches Rust Vortex at
`46a8d39c032b1e9f8ae28a13efe8abc9ebad556f` and applies the checked-in sliced ALP
patch. That patch preserves serialized within-chunk patch offsets when
reopening sliced files.

Compare generated `.vortex` files against `testdata/rust`, and `pco_modes.json`
against `internal/pco/testdata/modes.json`. The small date/decimal fixture
has separate Iceberg-owned tooling documented in `testdata/rust/README.md`.
This script regenerates every other checked-in Vortex and Parquet fixture.
`PYTHON` can select the Python interpreter. Parquet oracles are generated
directly from source formulas, never from decoded Vortex output.

The nullable delta generator additionally writes whole arrays and three slices,
reopens each with Rust, and checks original values and nulls before completing.
The standalone `TestRustNullableDeltaSourceValues` also checks physical delta
metadata so a fixture refresh cannot replace the tested encoding accidentally.

The independent corpus uses fresh scalar, nullable, temporal and float datasets.
Explicit Delta, DateTimeParts, flat ALP patches and the upstream legacy stats
layout preserve physical representation checks alongside default-writer coverage.
