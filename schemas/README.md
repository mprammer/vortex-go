# Wire schema provenance

The FlatBuffers definitions and `dtype.proto`/`scalar.proto` originate from
[vortex-data/vortex](https://github.com/vortex-data/vortex/tree/46a8d39c032b1e9f8ae28a13efe8abc9ebad556f)
revision `46a8d39c032b1e9f8ae28a13efe8abc9ebad556f`, under Apache-2.0. Original
copyright and SPDX notices are retained in schemas and generated code.

The protobuf definitions use the `mprammer.vortex` namespace and a Go `go_package`
option. Those name changes prevent global descriptor collisions if another Vortex
implementation is linked in the same process; protobuf field numbers and wire
values are unchanged. The FlatBuffers definitions are unchanged.

Regenerate with `./scripts/generate.sh`. The checked-in files were generated with:

- FlatBuffers `flatc` 25.12.19
- Protocol Buffers `protoc` 33.4
- `protoc-gen-go` from `google.golang.org/protobuf` revision
  `v1.36.12-0.20260120151049-f2248ac996af`

Set `FLATC` and `PROTOC` to compiler paths if they are not on `PATH`; put
`protoc-gen-go` on `PATH`. The protobuf compiler must be able to locate its
standard `google/protobuf/struct.proto` include.

`internal/wire/fb/reserved.go` is a handwritten accessor for reserved schema
fields whose generated Go identifiers are private. It is not overwritten by
regeneration. No native compiler or schema generator is needed to build or use
the reader.
