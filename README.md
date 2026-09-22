# vortex-go

A native Go reader for Vortex columnar files. It reads from `io.ReaderAt` and
returns Apache Arrow record batches without cgo or a Rust runtime.

This is an independent implementation derived from the Rust Vortex format,
schemas, and decoding algorithms. Development targets Rust Vortex revision
`46a8d39c032b1e9f8ae28a13efe8abc9ebad556f`; compatibility with arbitrary writer
versions is not implied. The implementation is under active development.

```go
input, err := os.Open("data.vortex")
if err != nil { return err }
defer input.Close()
info, err := input.Stat()
if err != nil { return err }
file, err := vortex.Open(ctx, input, info.Size())
if err != nil { return err }
defer file.Close()
reader, err := file.NewRecordReader(ctx, vortex.ScanOptions{
    Columns: []string{"id", "name"},
})
if err != nil { return err }
defer reader.Release()
for reader.Next() {
    record := reader.RecordBatch()
    // Consume record here. Call Retain to keep it past Next or Release,
    // then release that retained reference when finished.
    _ = record
}
return reader.Err()
```

Import the package as `github.com/mprammer/vortex-go` (package name `vortex`).
The example also uses `os` and a caller-provided context `ctx`.

## Reading and ownership

`Open` reads metadata and exposes `Schema` and `NumRows`. Scans load projected
columns on demand and align batches by physical row ranges. A nil column list
selects every column; an empty list requests row counts without value columns.
The caller owns the source and must keep it open while scans are active.

Each record reader implements Arrow's `array.RecordReader`. `RowOffset` is the
original physical start of its current contiguous batch, including after
pruning. This lets consumers apply positional deletes correctly.

`ScanOptions.Predicates` provides conservative integer min/max zone pruning.
Predicates are ANDed. Pruning can retain nonmatching rows: callers must evaluate
their residual predicate. Unknown or unusable optional statistics retain data.

The initial scope is scalar columns: booleans, numeric primitives, UTF-8, binary,
dates in days, and decimal values up to 128 bits. Nested tables and a writer are
outside this implementation's scope. Unsupported representations return errors.

## Development

Go 1.25 or newer is required.

```sh
CGO_ENABLED=0 go test ./...
go test -race ./...
```

Rust-produced fixtures check exact values and nulls, encoding boundaries, and
file-layout behavior. The reference fixture generators live in `dev/fixtures`.
`testdata/rust/README.md` records the upstream writer pin and the generator for each fixture family.
The module's Go tests require no Rust installation.

Apache-2.0. See `NOTICE` for upstream algorithm and schema attribution.
