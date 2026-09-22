# Reference fixtures

These inputs are generated from independently authored source arrays using
upstream Rust Vortex revision `46a8d39c032b1e9f8ae28a13efe8abc9ebad556f`,
with the recorded sliced-ALP deserialization fix. All fixture-generation source
is maintained by this project or the Iceberg integration. No third-party Go
reader, writer, or fixture generator supplies these inputs.

| Files | Source | Coverage |
| --- | --- | --- |
| `owned_scalars.vortex`, `owned_nullable.vortex` | `dev/fixtures/src/bin/independent_corpus.rs` | Mixed scalars, UTF-8, integers above 2^53, separate null patterns |
| `owned_temporal_delta.vortex` | same | Forced unsigned Delta and nanosecond/millisecond DateTimeParts |
| `owned_alp_flat.vortex` | same | Required/nullable float32/64, exact IEEE bits, flat ALP patches without chunk metadata |
| `owned_legacy_stats.vortex` | `dev/fixtures/src/bin/independent_stats.rs` | Upstream legacy statistics layout and conservative pruning |
| `owned_*.parquet` | `dev/fixtures/parquet_oracles.py` | Independently generated scalar/nullable value comparisons; never read Vortex to generate these |
| `default_*.vortex` | `dev/fixtures/src/bin/default_writer.rs` | Default writer across supported scalar types and individual projections |
| `alp_patches*.vortex` | `dev/fixtures/src/bin/alp_patches.rs` | Explicit chunked patch offsets and physical slices |
| `delta_nullable*.vortex` | `dev/fixtures/src/bin/delta_nullable.rs` | Nullable Delta whole blocks, slices, and padded tail |
| `zoned_ints*.vortex` | `dev/fixtures/src/bin/zoned_stats.rs` | Zoned statistics, nulls, physical positions, optional-stat fallback |
| `pco_regressions.vortex` | `dev/fixtures/src/bin/pco_regressions.rs` | PCO modes, stored validity and page boundaries |
| `zstd_legacy*.vortex` | `dev/fixtures/src/bin/zstd_legacy.rs` | Upstream legacy Zstd wire representation |
| `date_decimal.vortex` | Iceberg `dev/tpch/writer/generate-types.py`, `boundary.vortex` output | Eight exact Date32/Decimal128 boundary rows |

“Legacy” names identify upstream wire representations, not an inherited Go
implementation. The Rust generators reopen files and check source values; Go
checks original formulas and physical metadata separately. See
`dev/fixtures/README.md` for regeneration instructions and dependency pins.
