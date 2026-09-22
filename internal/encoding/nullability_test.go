// SPDX-License-Identifier: Apache-2.0

package encoding

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestCanonicalNullability(t *testing.T) {
	validity := func(bits byte) *Node { return &Node{Encoding: "vortex.bool", Buffers: [][]byte{{bits}}} }
	type testCase struct {
		name string
		typ  Type
		node func(byte) *Node
	}
	cases := []testCase{
		{"bool", Type{Kind: "bool"}, func(mask byte) *Node {
			return &Node{Encoding: "vortex.bool", Buffers: [][]byte{{5}}, Children: []*Node{validity(mask)}}
		}},
		{"bytebool", Type{Kind: "bool"}, func(mask byte) *Node {
			return &Node{Encoding: "vortex.bytebool", Buffers: [][]byte{{1, 0, 1}}, Children: []*Node{validity(mask)}}
		}},
		{"masked", Type{Kind: "i64"}, func(mask byte) *Node {
			return &Node{Encoding: "vortex.masked", Children: []*Node{primitive(8, 10, 20, 30), validity(mask)}}
		}},
		{"dictionary_nullable_codes", Type{Kind: "i64"}, func(mask byte) *Node {
			codes := primitive(1, 0, 1, 0)
			codes.Children = []*Node{validity(mask)}
			return &Node{Encoding: "vortex.dict", Metadata: meta(pv(1, 2), pv(3, 1)), Children: []*Node{codes, primitive(8, 10, 20)}}
		}},
		{"struct", Type{Kind: "struct", Fields: []Field{{Name: "n", Type: Type{Kind: "i64"}}}}, func(mask byte) *Node {
			return &Node{Encoding: "vortex.struct", Children: []*Node{validity(mask), primitive(8, 10, 20, 30)}}
		}},
		{"decimal", Type{Kind: "decimal", Precision: 10}, func(mask byte) *Node {
			return &Node{Encoding: "vortex.decimal", Buffers: [][]byte{{10, 20, 30}}, Children: []*Node{validity(mask)}}
		}},
		{"extension_nullable_storage", Type{Kind: "extension", Extension: "vortex.date", Metadata: []byte{4}, Storage: &Type{Kind: "i32", Nullable: true}}, func(mask byte) *Node {
			child := primitive(4, 10, 20, 30)
			child.Children = []*Node{validity(mask)}
			return &Node{Encoding: "vortex.ext", Children: []*Node{child}}
		}},
	}
	for _, kind := range []Kind{"i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "f16", "f32", "f64"} {
		typ := Type{Kind: kind}
		cases = append(cases, testCase{string(kind), typ, func(mask byte) *Node {
			n := primitive(typ.width(), 10, 20, 30)
			n.Children = []*Node{validity(mask)}
			return n
		}})
	}
	for _, kind := range []Kind{"binary", "utf8"} {
		cases = append(cases, testCase{string(kind), Type{Kind: kind}, func(mask byte) *Node {
			return &Node{Encoding: "vortex.varbin", Buffers: [][]byte{[]byte("abc")}, Children: []*Node{primitive(1, 0, 1, 2, 3), validity(mask)}}
		}})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer alloc.AssertSize(t, 0)
			ctx := compute.WithAllocator(context.Background(), alloc)
			for _, nullable := range []bool{false, true} {
				typ := tc.typ
				typ.Nullable = nullable
				for _, mask := range []byte{5, 7} {
					a, err := Decode(ctx, tc.node(mask), typ, 3)
					if !nullable && mask == 5 {
						if a != nil {
							a.Release()
							t.Error("nonnullable decode returned an array")
						}
						if err == nil || !strings.Contains(err.Error(), "nonnullable") {
							t.Errorf("expected nonnullable rejection, got %v", err)
						}
						continue
					}
					if err != nil {
						t.Fatalf("nullable=%t mask=%b: %v", nullable, mask, err)
					}
					for row := 0; row < 3; row++ {
						if a.IsValid(row) != (mask&(1<<row) != 0) {
							t.Errorf("row %d validity changed", row)
						}
					}
					a.Release()
				}
			}
		})
	}
}

func TestCanonicalNullabilityChecksComposedChildren(t *testing.T) {
	for _, parentNullable := range []bool{false, true} {
		child := primitive(8, 10, 20, 30)
		child.Children = []*Node{{Encoding: "vortex.bool", Buffers: [][]byte{{5}}}}
		typ := Type{Kind: "struct", Nullable: parentNullable, Fields: []Field{{Name: "required", Type: Type{Kind: "i64"}}}}
		a, err := Decode(context.Background(), &Node{Encoding: "vortex.struct", Children: []*Node{child}}, typ, 3)
		if a != nil {
			a.Release()
			t.Error("invalid child escaped through a struct")
		}
		if err == nil || !strings.Contains(err.Error(), "vortex.struct: vortex.primitive:") || !strings.Contains(err.Error(), "nonnullable") {
			t.Errorf("expected child nullability error, got %v", err)
		}
	}
	// Null is intrinsically null, including a field in an otherwise required struct.
	typ := Type{Kind: "struct", Fields: []Field{{Name: "null", Type: Type{Kind: "null"}}}}
	n := &Node{Encoding: "vortex.struct", Children: []*Node{{Encoding: "vortex.null"}}}
	a, err := Decode(context.Background(), n, typ, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Release()
	if a.NullN() != 0 || a.(*array.Struct).Field(0).NullN() != 3 {
		t.Fatal("intrinsically null field changed")
	}
	// An empty required array has no false validity entries to reject.
	empty := primitive(8)
	empty.Children = []*Node{{Encoding: "vortex.bool", Buffers: [][]byte{{}}}}
	b, err := Decode(context.Background(), empty, Type{Kind: "i64"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Release()
	if b.Len() != 0 || b.NullN() != 0 {
		t.Fatal("empty required array changed")
	}
}
