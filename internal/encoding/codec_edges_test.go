// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 the vortex-go contributors.
package encoding

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func scalarEdgeContext(t *testing.T) context.Context {
	t.Helper()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { mem.AssertSize(t, 0) })
	return compute.WithAllocator(t.Context(), mem)
}

// The left child owns ALPRD validity. Exceptions replace numeric bits only;
// both the code and right-part payload at a null row have no logical value.
func TestALPRDNullPayloadAndNumericPatches(t *testing.T) {
	for _, kind := range []Kind{"f32", "f64"} {
		for _, tc := range []struct {
			name        string
			code, right uint64
			patchNull   bool
		}{
			{"patched_null", 0, 0, true},
			{"null_code", 65535, 0, false},
			{"null_right_bits", 0, math.MaxUint64, false},
			{"patched_null_payload", 65535, math.MaxUint64, true},
		} {
			t.Run(string(kind)+"/"+tc.name, func(t *testing.T) {
				ctx := scalarEdgeContext(t)
				w, shift, one, wantNaN := 8, uint(48), uint64(0x3ff0), uint64(0xfff8000000001234)
				if kind == "f32" {
					w, shift, one, wantNaN = 4, 16, 0x3f80, 0xffc01234
				}
				left := primitive(2, 0, tc.code, 0)
				left.Children = []*Node{{Encoding: "vortex.bool", Buffers: [][]byte{{5}}}}
				right := primitive(w, 0, tc.right, wantNaN&((uint64(1)<<shift)-1))
				indices, patches := []uint64{2}, []uint64{wantNaN >> shift}
				if tc.patchNull {
					indices, patches = []uint64{1, 2}, []uint64{0x4000, wantNaN >> shift}
				}
				patchMeta := meta(pv(1, uint64(len(indices))), pv(2, 1023), pv(3, 3))
				for i := range indices {
					indices[i] += 1023
				}
				node := &Node{Encoding: "vortex.alprd", Metadata: meta(pv(1, uint64(shift)), pv(2, 1), pv(3, one), pv(4, 1), pb(5, patchMeta)), Children: []*Node{left, right, primitive(8, indices...), primitive(2, patches...)}}
				a, err := Decode(ctx, node, Type{Kind: kind, Nullable: true}, 3)
				if err != nil {
					t.Fatal(err)
				}
				defer a.Release()
				if a.IsNull(0) || !a.IsNull(1) || a.IsNull(2) {
					t.Fatalf("validity: %s", a)
				}
				var first, last uint64
				switch a := a.(type) {
				case *array.Float32:
					first, last = uint64(math.Float32bits(a.Value(0))), uint64(math.Float32bits(a.Value(2)))
				case *array.Float64:
					first, last = math.Float64bits(a.Value(0)), math.Float64bits(a.Value(2))
				}
				if first != one<<shift || last != wantNaN {
					t.Fatalf("float bits: %x %x", first, last)
				}
			})
		}
	}
}

func TestALPRDRejectsInvalidNonNullParts(t *testing.T) {
	for _, kind := range []Kind{"f32", "f64"} {
		for _, mutation := range []string{"code", "right_width", "signed_left", "short_right", "patch_range", "extra_child"} {
			t.Run(string(kind)+"/"+mutation, func(t *testing.T) {
				w, shift, one := 8, uint(48), uint64(0x3ff0)
				if kind == "f32" {
					w, shift, one = 4, 16, 0x3f80
				}
				node := &Node{Encoding: "vortex.alprd", Metadata: meta(pv(1, uint64(shift)), pv(2, 1), pv(3, one), pv(4, 1)), Children: []*Node{primitive(2, 0), primitive(w, 0)}}
				switch mutation {
				case "code":
					node.Children[0] = primitive(2, 65535)
				case "right_width":
					node.Children[1] = primitive(w, uint64(1)<<shift)
				case "signed_left":
					node.Metadata = append(node.Metadata, pv(4, 5)...)
				case "short_right":
					node.Children[1] = primitive(w)
				case "patch_range":
					node.Metadata = append(node.Metadata, pb(5, meta(pv(1, 1), pv(3, 0)))...)
					node.Children = append(node.Children, primitive(1, 1), primitive(2, 0x4000))
				case "extra_child":
					node.Children = append(node.Children, primitive(2, 0))
				}
				assertEdgeError(t, node, Type{Kind: kind}, 1)
			})
		}
	}
}

// Source rows 1023..1027 have patches at both slice ends and the middle.
// Sparse patches replace whole logical values, including nulls, unlike ALPRD.
func TestSparseSliceFillAndNullPatches(t *testing.T) {
	for _, nullFill := range []bool{false, true} {
		t.Run(fmt.Sprintf("null_fill_%v", nullFill), func(t *testing.T) {
			fill := pv(3, 14) // sint64(7)
			if nullFill {
				fill = nil
			}
			patches := primitive(8, uint64(1)<<63, 0, math.MaxInt64)
			patches.Children = []*Node{{Encoding: "vortex.bool", Buffers: [][]byte{{5}}}}
			node := &Node{Encoding: "vortex.sparse", Metadata: pb(1, meta(pv(1, 3), pv(2, 1023), pv(3, 3))), Buffers: [][]byte{fill}, Children: []*Node{primitive(8, 1023, 1025, 1027), patches}}
			a, err := Decode(scalarEdgeContext(t), node, Type{Kind: "i64", Nullable: true}, 5)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Release()
			want := []int64{math.MinInt64, 7, 0, 7, math.MaxInt64}
			for i, value := range want {
				valid := i != 2 && (!nullFill || i == 0 || i == 4)
				if a.IsValid(i) != valid || valid && a.(*array.Int64).Value(i) != value {
					t.Fatalf("row %d: %s", i, a)
				}
			}
		})
	}
}

func TestSparseRejectsInvalidPatchComponents(t *testing.T) {
	for _, mutation := range []string{"signed_indices", "unsorted", "before_slice", "past_slice", "short_values", "null_indices"} {
		t.Run(mutation, func(t *testing.T) {
			patchMeta := meta(pv(1, 2), pv(2, 10), pv(3, 0))
			indices, values := primitive(1, 10, 12), primitive(8, 1, 2)
			switch mutation {
			case "signed_indices":
				patchMeta = append(patchMeta, pv(3, 4)...)
			case "unsorted":
				indices = primitive(1, 12, 10)
			case "before_slice":
				indices = primitive(1, 9, 12)
			case "past_slice":
				indices = primitive(1, 10, 13)
			case "short_values":
				values = primitive(8, 1)
			case "null_indices":
				indices.Children = []*Node{{Encoding: "vortex.bool", Buffers: [][]byte{{1}}}}
			}
			node := &Node{Encoding: "vortex.sparse", Metadata: pb(1, patchMeta), Buffers: [][]byte{pv(3, 14)}, Children: []*Node{indices, values}}
			assertEdgeError(t, node, Type{Kind: "i64"}, 3)
		})
	}
}

func edgeRLE() *Node {
	indices := make([]uint64, 2048)
	indices[1022], indices[1023], indices[1024], indices[1025] = 0, 65535, 1, 0
	validity := make([]byte, 256)
	for i := range validity {
		validity[i] = 255
	}
	validity[1023/8] &^= 1 << uint(1023%8)
	codes := primitive(2, indices...)
	codes.Children = []*Node{{Encoding: "vortex.bool", Buffers: [][]byte{validity}}}
	return &Node{Encoding: "fastlanes.rle", Metadata: meta(pv(1, 4), pv(2, 2048), pv(3, 1), pv(4, 2), pv(5, 3), pv(6, 1022)), Children: []*Node{primitive(8, 101, 102, 201, 202), codes, primitive(8, 0, 2)}}
}

func TestRLESliceAcrossChunkBoundary(t *testing.T) {
	a, err := Decode(scalarEdgeContext(t), edgeRLE(), Type{Kind: "i64", Nullable: true}, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Release()
	for i, want := range []int64{101, 0, 202, 201} {
		if a.IsValid(i) != (i != 1) || i != 1 && a.(*array.Int64).Value(i) != want {
			t.Fatalf("row %d: %s", i, a)
		}
	}
}

func TestRLERejectsInvalidComponents(t *testing.T) {
	for _, mutation := range []string{"offset", "signed_indices", "signed_offsets", "offset_order", "offset_span", "cross_chunk_code", "short_values", "null_value"} {
		t.Run(mutation, func(t *testing.T) {
			node := edgeRLE()
			switch mutation {
			case "offset":
				node.Metadata = append(node.Metadata, pv(6, 1024)...)
			case "signed_indices":
				node.Metadata = append(node.Metadata, pv(3, 5)...)
			case "signed_offsets":
				node.Metadata = append(node.Metadata, pv(5, 7)...)
			case "offset_order":
				node.Children[2] = primitive(8, 2, 0)
			case "offset_span":
				node.Children[2] = primitive(8, 0, 5)
			case "cross_chunk_code":
				node.Children[1].Buffers[0][1022*2] = 2
			case "short_values":
				node.Children[0] = primitive(8, 101, 102, 201)
			case "null_value":
				node.Children[0].Children = []*Node{{Encoding: "vortex.bool", Buffers: [][]byte{{14}}}}
			}
			assertEdgeError(t, node, Type{Kind: "i64", Nullable: true}, 4)
		})
	}
}

func assertEdgeError(t *testing.T, node *Node, typ Type, length int) {
	t.Helper()
	a, err := Decode(scalarEdgeContext(t), node, typ, length)
	if a != nil {
		a.Release()
	}
	if err == nil {
		t.Fatal("accepted invalid components")
	}
}
