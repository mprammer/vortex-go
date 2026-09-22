// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 the vortex-go contributors.
package encoding

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"google.golang.org/protobuf/encoding/protowire"
)

func zigzag(v int64) uint64 { return uint64(protowire.EncodeZigZag(v)) }

// A sequence serializes its step with the step's own signedness, not the
// array's: Vortex normalizes the step to i64 whenever it fits, so an unsigned
// sequence routinely carries a signed step.
func TestSequenceStepSignednessIsIndependentOfArrayType(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  Kind
		step []byte
		want []uint64
	}{
		{"unsigned array, signed step", "u32", pv(3, zigzag(2)), []uint64{10, 12, 14, 16}},
		{"unsigned array, negative step", "u32", pv(3, zigzag(-2)), []uint64{10, 8, 6, 4}},
		{"unsigned array, unsigned step", "u32", pv(4, 2), []uint64{10, 12, 14, 16}},
		{"signed array, signed step", "i32", pv(3, zigzag(-2)), []uint64{10, 8, 6, 4}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := pv(4, 10)
			if tc.typ == "i32" {
				base = pv(3, zigzag(10))
			}
			node := &Node{Encoding: "vortex.sequence", Metadata: meta(pb(1, base), pb(2, tc.step))}
			a, e := Decode(context.Background(), node, Type{Kind: tc.typ}, len(tc.want))
			if e != nil {
				t.Fatal(e)
			}
			for i, want := range tc.want {
				var got uint64
				switch v := a.(type) {
				case *array.Uint32:
					got = uint64(v.Value(i))
				case *array.Int32:
					got = uint64(v.Value(i))
				default:
					t.Fatalf("unexpected array %T", a)
				}
				if got != want {
					t.Fatalf("row %d: got %d want %d", i, got, want)
				}
			}
		})
	}
}

// A step that leaves the array's range is rejected by the range check on the
// last element, not by the scalar's own width.
func TestSequenceRejectsStepThatOverflowsTheArrayType(t *testing.T) {
	node := &Node{Encoding: "vortex.sequence", Metadata: meta(pb(1, pv(4, 10)), pb(2, pv(3, zigzag(-11))))}
	if _, e := Decode(context.Background(), node, Type{Kind: "u32"}, 4); e == nil {
		t.Fatal("expected an overflow error for a sequence that runs below zero")
	}
}

func onpairNode() *Node {
	// Dictionary "abcde" holds tokens "ab" and "cde". Row 0 is codes [0 1],
	// row 1 is code [0].
	return &Node{
		Encoding: "vortex.onpair",
		Metadata: meta(pv(1, 2), pv(3, 2), pv(4, 3), pv(5, 2), pv(6, 1), pv(7, 2)),
		Buffers:  [][]byte{[]byte("abcde")},
		Children: []*Node{
			primitive(4, 0, 2, 5),
			primitive(2, 0, 1, 0),
			primitive(4, 0, 2, 3),
			primitive(4, 5, 2),
		},
	}
}

func TestOnPairDecodesTokenRuns(t *testing.T) {
	a, e := Decode(context.Background(), onpairNode(), Type{Kind: "utf8"}, 2)
	if e != nil {
		t.Fatal(e)
	}
	got := a.(*array.String)
	for i, want := range []string{"abcde", "ab"} {
		if got.Value(i) != want {
			t.Fatalf("row %d: got %q want %q", i, got.Value(i), want)
		}
	}
}

func TestOnPairRejectsMalformedInput(t *testing.T) {
	for _, tc := range []struct {
		name   string
		break_ func(*Node)
	}{
		{"code past the dictionary", func(n *Node) { n.Children[1] = primitive(2, 0, 9, 0) }},
		{"row boundary past the code stream", func(n *Node) { n.Children[2] = primitive(4, 0, 2, 4) }},
		{"dictionary offset past the blob", func(n *Node) { n.Children[0] = primitive(4, 0, 2, 9) }},
		{"decoded length disagrees", func(n *Node) { n.Children[3] = primitive(4, 4, 2) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := onpairNode()
			tc.break_(n)
			if _, e := Decode(context.Background(), n, Type{Kind: "utf8"}, 2); e == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

// Rows the validity child marks null carry no codes and are not decoded.
func TestOnPairSkipsNullRows(t *testing.T) {
	n := onpairNode()
	n.Children[2] = primitive(4, 0, 2, 2)
	n.Children[3] = primitive(4, 5, 0)
	n.Children = append(n.Children, &Node{Encoding: "vortex.bool", Buffers: [][]byte{{0x01}}})
	a, e := Decode(context.Background(), n, Type{Kind: "utf8", Nullable: true}, 2)
	if e != nil {
		t.Fatal(e)
	}
	got := a.(*array.String)
	if got.IsNull(0) || got.Value(0) != "abcde" || !got.IsNull(1) {
		t.Fatalf("got %v null=%v,%v", got.Value(0), got.IsNull(0), got.IsNull(1))
	}
}
