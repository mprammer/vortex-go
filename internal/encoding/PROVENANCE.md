# Decoder provenance

This package is independently implemented from the upstream wire declarations
and Rust decoding algorithms below. Reproducible input oracles are maintained
in `../../dev/fixtures`.

The Vortex reference revision is
`46a8d39c032b1e9f8ae28a13efe8abc9ebad556f`. Sources are licensed Apache-2.0,
copyright the Vortex contributors. The root LICENSE includes Apache-2.0.

- `vortex-proto/proto/{dtype,scalar}.proto`: primitive discriminants and scalar wire values.
- `vortex-array/src/arrays/{primitive,bool,constant,struct_,varbin,varbinview,dict,masked,extension,decimal}`:
  metadata fields, child ordering, buffers, and canonical values.
- `vortex-array/src/patches.rs`: absolute patch positions, slice offsets, and optional chunk-offset metadata.
- `encodings/fastlanes/src/{bitpacking,for,rle,delta}`: storage contracts and child types.
- `encodings/{alp,fsst,onpair,zstd,pco,sparse,sequence,runend,zigzag,decimal-byte-parts,datetime-parts}/src`:
  corresponding encoding metadata and reconstruction contracts.
- `vortex-array/src/extension/datetime/{date,time,timestamp,unit}.rs`: temporal extension metadata.

The original algorithm references for FastLanes element mapping, scalar bit
unpacking, delta, and transpose in
`numeric.go` and `temporal_delta.go` are the Apache-2.0
`fastlanes` crate version 0.7.2 (`src/{bitpacking,delta,transpose}.rs`),
copyright the FastLanes/SpiralDB contributors. The scalar implementation uses
bounds-checked Go reads; it does not reproduce the Rust unchecked/SIMD code.

The ALP multiplication order and powers in `numeric.go` follow the Apache-2.0
`alp` crate version 0.0.4, `src/alp/mod.rs`. Float32 rounds after each multiply;
patch values and ALPRD reconstruction preserve the original IEEE bits.

These algorithm-reference versions differ from the compatibility target: the
pinned Rust writer in `../../dev/fixtures/Cargo.lock` resolves `fastlanes` 0.6.1
and `alp` 0.0.2. The nullable-delta validity reconstruction in
`temporal_delta.go` follows the 0.6.1 `src/transpose.rs` bitmap scatter contract;
the Rust-generated full and sliced nullable-delta fixtures check that contract.
The reference versions above record source attribution, not a claim that the
writer uses those newer crate versions.

FSST reconstruction follows the symbol/code wire definition: 0..254 index
little-endian eight-byte symbols of declared length; 255 escapes the next byte.
Zstd decompression uses the separately licensed Go dependency
`github.com/klauspost/compress`; this package checks sizes and reconstructs
Vortex's length-prefixed variable values or packed non-null primitives.
PCO implementation and provenance are in `../pco`.

The independently generated fixtures in `../../testdata/rust` are independent
input oracles. Production decoding does not call Rust, FFI, or a helper process.
