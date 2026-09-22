// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 the vortex-go contributors.
package vortex

import (
	"context"
	"encoding/binary"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/mprammer/vortex-go/internal/encoding"
	"google.golang.org/protobuf/encoding/protowire"
)

// These fixtures were generated from explicit Rust ALP arrays. Besides checking
// decoded source values, verify that slices retain physical patch offsets; this
// prevents a regenerated fixture from accidentally dropping the regression case.
func TestRustALPPhysicalPatchOffsets(t *testing.T) {
	cases := []struct {
		name             string
		start            int
		within           uint64
		indices, offsets []uint64
	}{
		{"alp_patches", 0, 0, []uint64{1, 4, 5, 31, 1023, 1025, 3072, 4095, 4096, 4102}, []uint64{0, 5, 6, 6, 8}},
		{"alp_patches_slice", 5, 2, []uint64{5, 31, 1023, 1025, 3072, 4095, 4096}, []uint64{0, 5, 6, 6, 8}},
		{"alp_patches_late_slice", 1025, 0, []uint64{1025, 3072, 4095, 4096}, []uint64{5, 6, 6, 8}},
		{"alp_patches_boundary", 1023, 4, []uint64{1023, 1025}, []uint64{0, 5}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, _ := openFixture(t, tc.name)
			for column, field := range f.dtype.Fields {
				if field.Name == "row_id" {
					continue
				}
				plan := &columnPlan{}
				if e := buildFieldPlan(f.root, f.dtype, column, 0, plan); e != nil {
					t.Fatal(e)
				}
				if len(plan.pieces) != 1 {
					t.Fatalf("%s: expected one physical piece", field.Name)
				}
				leaf := plan.pieces[0].node
				if leaf.id != "vortex.flat" || len(leaf.segments) != 1 {
					t.Fatalf("%s: unexpected layout", field.Name)
				}
				node := physicalArrayNode(t, f, leaf)
				if node.Encoding != "vortex.alp" {
					t.Fatalf("%s: expected ALP, got %s", field.Name, node.Encoding)
				}
				patchBytes := wireBytesField(t, node.Metadata, 3)
				if strings.HasSuffix(field.Name, "all_null") {
					if patchBytes != nil {
						t.Fatal("all-null ALP has patches")
					}
					continue
				}
				fields := wireUnsignedFields(t, patchBytes)
				expected := map[protowire.Number]uint64{1: uint64(len(tc.indices)), 2: uint64(tc.start), 3: 3, 4: uint64(len(tc.offsets)), 5: 3, 6: tc.within}
				for tag, want := range expected {
					got, ok := fields[tag]
					if !ok && want != 0 {
						t.Fatalf("%s missing field %d", field.Name, tag)
					}
					if tag == 6 && !ok {
						t.Fatalf("%s missing explicit slice metadata", field.Name)
					}
					if got != want {
						t.Fatalf("%s field %d: got %d want %d", field.Name, tag, got, want)
					}
				}
				if len(node.Children) != 4 {
					t.Fatalf("%s patch children count", field.Name)
				}
				for _, child := range []struct {
					index int
					want  []uint64
				}{{1, tc.indices}, {3, tc.offsets}} {
					a, e := encoding.Decode(context.Background(), node.Children[child.index], encoding.Type{Kind: "u64"}, len(child.want))
					if e != nil {
						t.Fatal(e)
					}
					got := a.(*array.Uint64).Uint64Values()
					if !reflect.DeepEqual(got, child.want) {
						a.Release()
						t.Fatalf("%s child %d: got %v want %v", field.Name, child.index, got, child.want)
					}
					a.Release()
				}
			}
		})
	}
}
func wireBytesField(t *testing.T, b []byte, want protowire.Number) []byte {
	t.Helper()
	for len(b) > 0 {
		tag, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			t.Fatal("bad protobuf tag")
		}
		b = b[n:]
		if tag == want {
			if typ != protowire.BytesType {
				t.Fatal("wrong wire type")
			}
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				t.Fatal("bad protobuf bytes")
			}
			return v
		}
		n = protowire.ConsumeFieldValue(tag, typ, b)
		if n < 0 {
			t.Fatal("bad protobuf value")
		}
		b = b[n:]
	}
	return nil
}
func wireUnsignedFields(t *testing.T, b []byte) map[protowire.Number]uint64 {
	t.Helper()
	out := map[protowire.Number]uint64{}
	for len(b) > 0 {
		tag, typ, n := protowire.ConsumeTag(b)
		if n < 0 || typ != protowire.VarintType {
			t.Fatal("invalid patch protobuf")
		}
		b = b[n:]
		v, n := protowire.ConsumeVarint(b)
		if n < 0 {
			t.Fatal("invalid patch varint")
		}
		out[tag] = v
		b = b[n:]
	}
	return out
}

// Read the physical representation as well as values so a fixture refresh cannot
// silently stop exercising the codec or metadata variant it is intended to cover.
func physicalArrayNode(t *testing.T, f *File, leaf *layout) *encoding.Node {
	t.Helper()
	seg := f.segments[leaf.segments[0]]
	data, e := f.read(context.Background(), seg.offset, seg.length, maxSegment)
	if e != nil {
		t.Fatal(e)
	}
	var tree []byte
	if len(leaf.metadata) > 0 {
		fields, e := protoFields(leaf.metadata)
		if e != nil {
			t.Fatal(e)
		}
		if vals := fields[1]; len(vals) > 0 {
			tree = vals[len(vals)-1].bytes
		}
	}
	if tree == nil {
		if len(data) < 4 {
			t.Fatal("short array")
		}
		size := int(binary.LittleEndian.Uint32(data[len(data)-4:]))
		if size > len(data)-4 {
			t.Fatal("array metadata out of bounds")
		}
		start := len(data) - 4 - size
		tree = data[start : len(data)-4]
		data = data[:start]
	}
	node, e := f.parseArray(tree, data)
	if e != nil {
		t.Fatal(e)
	}
	return node
}

func TestRustPCOPhysicalRepresentation(t *testing.T) {
	f, _ := openFixture(t, "pco_regressions")
	if f.NumRows() != 8000 || len(f.dtype.Fields) != 13 {
		t.Fatal("PCO fixture shape changed")
	}
	for column, field := range f.dtype.Fields {
		plan := &columnPlan{}
		if err := buildFieldPlan(f.root, f.dtype, column, 0, plan); err != nil {
			t.Fatal(err)
		}
		if len(plan.pieces) != 1 {
			t.Fatalf("%s: expected one PCO piece", field.Name)
		}
		piece := plan.pieces[0]
		if piece.end-piece.start != 8000 {
			t.Fatalf("%s: logical length", field.Name)
		}
		node := physicalArrayNode(t, f, piece.node)
		if node.Encoding != "vortex.pco" {
			t.Fatalf("%s: got %s", field.Name, node.Encoding)
		}
		meta, err := protoFields(node.Metadata)
		if err != nil {
			t.Fatal(err)
		}
		if h := wireBytesField(t, node.Metadata, 1); !reflect.DeepEqual(h, []byte{4, 1}) {
			t.Fatalf("%s: header %x", field.Name, h)
		}
		stored := uint64(0)
		for chunkIndex, chunk := range meta[2] {
			fields, err := protoFields(chunk.bytes)
			if err != nil {
				t.Fatal(err)
			}
			for _, page := range fields[1] {
				counts, err := protoFields(page.bytes)
				if err != nil {
					t.Fatal(err)
				}
				if len(counts[1]) != 1 {
					t.Fatal("missing stored count")
				}
				count := counts[1][0].number
				if count == 0 || count > 256 {
					t.Fatalf("page size %d", count)
				}
				stored += count
			}
			if strings.HasPrefix(field.Name, "f") {
				want := byte(2)
				if field.Name == "f64_large" {
					want = 0
				}
				if len(node.Buffers) <= chunkIndex || len(node.Buffers[chunkIndex]) == 0 || node.Buffers[chunkIndex][0]&15 != want {
					t.Fatalf("%s: expected PCO mode %d", field.Name, want)
				}
			}
		}
		want := uint64(8000)
		switch {
		case strings.HasSuffix(field.Name, "_nullable"):
			want = 5333
		case field.Name == "i64_all_null":
			want = 0
		case field.Name == "i64_first" || field.Name == "i64_last":
			want = 1
		case field.Name == "i64_null_runs":
			want = 7452
		}
		if stored != want {
			t.Fatalf("%s: stored %d, want %d", field.Name, stored, want)
		}
	}
}

func TestOwnedTemporalDeltaPhysicalRepresentation(t *testing.T) {
	f, _ := openFixture(t, "owned_temporal_delta")
	for column, want := range []string{"fastlanes.delta", "fastlanes.delta", "fastlanes.delta", "vortex.datetimeparts", "vortex.datetimeparts"} {
		node := deltaFixtureArray(t, f, column)
		if node.Encoding != want {
			t.Fatalf("column %d: expected %s, got %s", column, want, node.Encoding)
		}
	}
}

// Flat patches omit chunk-offset metadata. Keep this representation covered
// alongside the explicitly sliced, chunked patches in the other ALP fixtures.
func TestOwnedALPPhysicalFlatPatches(t *testing.T) {
	f, _ := openFixture(t, "owned_alp_flat")
	if f.NumRows() != 3091 || len(f.dtype.Fields) != 4 {
		t.Fatal("flat ALP fixture shape changed")
	}
	for column, field := range f.dtype.Fields {
		node := deltaFixtureArray(t, f, column)
		if node.Encoding != "vortex.alp" {
			t.Fatalf("%s: expected ALP, got %s", field.Name, node.Encoding)
		}
		p := wireBytesField(t, node.Metadata, 3)
		if p == nil {
			t.Fatalf("%s: missing patches", field.Name)
		}
		fields, err := protoFields(p)
		if err != nil {
			t.Fatal(err)
		}
		if len(fields[4]) != 0 || len(fields[5]) != 0 || len(fields[6]) != 0 {
			t.Fatalf("%s: flat patches acquired chunk metadata", field.Name)
		}
		if len(fields[1]) != 1 || fields[1][0].number != 10 {
			t.Fatalf("%s: expected ten flat patches", field.Name)
		}
		if len(node.Children) != 3 {
			t.Fatalf("%s: expected encoded values, indices, and patches", field.Name)
		}
		want := []uint64{2, 7, 19, 509, 1022, 1024, 1537, 2047, 2048, 3089}
		indices, err := encoding.Decode(context.Background(), node.Children[1], encoding.Type{Kind: "u64"}, len(want))
		if err != nil {
			t.Fatal(err)
		}
		got := indices.(*array.Uint64).Uint64Values()
		equal := reflect.DeepEqual(got, want)
		indices.Release()
		if !equal {
			t.Fatalf("%s: flat patch indices differ", field.Name)
		}
	}
}
