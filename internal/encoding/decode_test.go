// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 the vortex-go contributors.
package encoding

import (
	"context"
	"encoding/binary"
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/protobuf/encoding/protowire"
)

func pv(tag protowire.Number, value uint64) []byte {
	return protowire.AppendVarint(protowire.AppendTag(nil, tag, protowire.VarintType), value)
}
func pb(tag protowire.Number, value []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(nil, tag, protowire.BytesType), value)
}
func meta(parts ...[]byte) []byte {
	var b []byte
	for _, p := range parts {
		b = append(b, p...)
	}
	return b
}
func primitive(w int, values ...uint64) *Node {
	b := make([]byte, w*len(values))
	for i, v := range values {
		writeWord(b[i*w:], w, v)
	}
	return &Node{Encoding: "vortex.primitive", Buffers: [][]byte{b}}
}
func TestBitpackedKnownTranspositionAndSlice(t *testing.T) {
	// For u64, the first packed word is lane zero, with row-order values.
	// Set rows 0,1,8,32: their logical positions are 0,128,64,16.
	raw := make([]byte, 128)
	binary.LittleEndian.PutUint64(raw, 1|1<<1|1<<8|1<<32)
	node := &Node{Encoding: "fastlanes.bitpacked", Metadata: pv(1, 1), Buffers: [][]byte{raw}}
	for _, span := range [][2]int{{0, 1024}, {5, 170}, {128, 129}} {
		n := *node
		n.Metadata = meta(pv(1, 1), pv(2, uint64(span[0])))
		a, e := Decode(context.Background(), &n, Type{Kind: "u64"}, span[1]-span[0])
		if e != nil {
			t.Fatal(e)
		}
		got := a.(*array.Uint64)
		for i := 0; i < a.Len(); i++ {
			row := span[0] + i
			want := uint64(0)
			if row == 0 || row == 128 || row == 64 || row == 16 {
				want = 1
			}
			if got.Value(i) != want {
				t.Fatalf("row %d: got %d want %d", row, got.Value(i), want)
			}
		}
		a.Release()
	}
}
func TestALPPatchesUseAbsoluteSliceIndices(t *testing.T) {
	patches := meta(pv(1, 2), pv(2, 1023), pv(3, 3), pv(4, 2), pv(5, 3), pv(6, 4))
	node := &Node{Encoding: "vortex.alp", Metadata: pb(3, patches), Children: []*Node{
		primitive(8, 1, 2, 3), primitive(8, 1023, 1025), primitive(8, 0x8000000000000000, 0x7ff8000000001234), primitive(8, 0, 5)}}
	a, e := Decode(context.Background(), node, Type{Kind: "f64"}, 3)
	if e != nil {
		t.Fatal(e)
	}
	defer a.Release()
	want := []uint64{0x8000000000000000, math.Float64bits(2), 0x7ff8000000001234}
	for i, w := range want {
		if got := math.Float64bits(a.(*array.Float64).Value(i)); got != w {
			t.Fatalf("row %d got %x want %x", i, got, w)
		}
	}
}
func TestMaskedDictionaryIgnoresNullCodePayload(t *testing.T) {
	codes := primitive(1, 1, 255, 0)
	codes.Children = []*Node{{Encoding: "vortex.bool", Buffers: [][]byte{{5}}}}
	node := &Node{Encoding: "vortex.dict", Metadata: meta(pv(1, 2), pv(2, 0), pv(3, 1)), Children: []*Node{codes, primitive(8, 123, 456)}}
	a, e := Decode(context.Background(), node, Type{Kind: "i64", Nullable: true}, 3)
	if e != nil {
		t.Fatal(e)
	}
	defer a.Release()
	if a.IsNull(0) || !a.IsNull(1) || a.IsNull(2) || a.(*array.Int64).Value(0) != 456 || a.(*array.Int64).Value(2) != 123 {
		t.Fatal(a)
	}
}
func TestDateDecimalExactnessAndAllocator(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(context.Background(), alloc)
	defer alloc.AssertSize(t, 0)
	storage := Type{Kind: "i32", Nullable: true}
	dt := Type{Kind: "extension", Extension: "vortex.date", Metadata: []byte{4}, Storage: &storage, Nullable: true}
	p := primitive(4, 0x80000000, 0xffffffff, 0, 1, 0x7fffffff)
	p.Children = []*Node{{Encoding: "vortex.bool", Buffers: [][]byte{{0x1b}}}}
	a, e := Decode(ctx, &Node{Encoding: "vortex.ext", Children: []*Node{p}}, dt, 5)
	if e != nil {
		t.Fatal(e)
	}
	defer a.Release()
	for i, w := range []int32{math.MinInt32, -1, 0, 1, math.MaxInt32} {
		if i == 2 {
			if !a.IsNull(i) {
				t.Fatal("missing null")
			}
		} else if int32(a.(*array.Date32).Value(i)) != w {
			t.Fatalf("date row %d", i)
		}
	}
	max := decimal128.GetMaxValue(38)
	for _, num := range []decimal128.Num{max, max.Negate(), decimal128.New(123, 456), decimal128.New(-124, ^uint64(455))} {
		raw := make([]byte, 16)
		binary.LittleEndian.PutUint64(raw, num.LowBits())
		binary.LittleEndian.PutUint64(raw[8:], uint64(num.HighBits()))
		b, e := Decode(ctx, &Node{Encoding: "vortex.decimal", Metadata: pv(1, 4), Buffers: [][]byte{raw}}, Type{Kind: "decimal", Precision: 38, Scale: 2}, 1)
		if e != nil {
			t.Fatal(e)
		}
		if b.(*array.Decimal128).Value(0) != num {
			t.Fatal("decimal bits changed")
		}
		b.Release()
	}
	for _, num := range []decimal128.Num{decimal128.GetScaleMultiplier(38), decimal128.GetScaleMultiplier(38).Negate(), decimal128.New(math.MinInt64, 0)} {
		raw := make([]byte, 16)
		binary.LittleEndian.PutUint64(raw, num.LowBits())
		binary.LittleEndian.PutUint64(raw[8:], uint64(num.HighBits()))
		b, e := Decode(ctx, &Node{Encoding: "vortex.decimal", Metadata: pv(1, 4), Buffers: [][]byte{raw}}, Type{Kind: "decimal", Precision: 38, Scale: 2}, 1)
		if b != nil {
			b.Release()
		}
		if e == nil || !strings.Contains(e.Error(), "exceeds declared precision") {
			t.Fatalf("expected precision error, got %v", e)
		}
	}
}
func TestMalformedArrays(t *testing.T) {
	cases := []struct {
		name string
		node *Node
		typ  Type
		n    int
	}{
		{"primitive truncation", primitive(1, 1), Type{Kind: "i64"}, 1},
		{"boolean truncation", &Node{Encoding: "vortex.bool", Buffers: [][]byte{{}}, Metadata: pv(1, 7)}, Type{Kind: "bool"}, 2},
		{"boolean offset", &Node{Encoding: "vortex.bool", Buffers: [][]byte{{255}}, Metadata: pv(1, 8)}, Type{Kind: "bool"}, 1},
		{"packed width", &Node{Encoding: "fastlanes.bitpacked", Metadata: pv(1, 65), Buffers: [][]byte{{}}}, Type{Kind: "u64"}, 1},
		{"varbin offset", &Node{Encoding: "vortex.varbin", Metadata: pv(1, 3), Buffers: [][]byte{{1}}, Children: []*Node{primitive(8, 0, 2)}}, Type{Kind: "binary"}, 1},
		{"view index", &Node{Encoding: "vortex.varbinview", Buffers: [][]byte{{13, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}}}, Type{Kind: "binary"}, 1},
		{"runend uncovered", &Node{Encoding: "vortex.runend", Metadata: meta(pv(1, 3), pv(2, 1)), Children: []*Node{primitive(8, 1), primitive(8, 123)}}, Type{Kind: "i64"}, 2},
		{"invalid decimal width", &Node{Encoding: "vortex.decimal", Metadata: pv(1, 63), Buffers: [][]byte{{}}}, Type{Kind: "decimal", Precision: 15, Scale: 2}, 1},
		{"truncated protobuf", &Node{Encoding: "vortex.bool", Metadata: []byte{0x80}, Buffers: [][]byte{{}}}, Type{Kind: "bool"}, 1},
		{"allocation limit", &Node{Encoding: "vortex.constant", Buffers: [][]byte{pv(4, 1)}}, Type{Kind: "u64"}, maxRows + 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, e := Decode(context.Background(), tc.node, tc.typ, tc.n)
			if a != nil {
				a.Release()
			}
			if e == nil {
				t.Fatal("expected error")
			}
		})
	}
	cycle := &Node{Encoding: "vortex.zigzag"}
	cycle.Children = []*Node{cycle}
	if _, e := Decode(context.Background(), cycle, Type{Kind: "i64"}, 1); e == nil {
		t.Fatal("expected nested error")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := Decode(ctx, primitive(8, 1), Type{Kind: "i64"}, 1); e != context.Canceled {
		t.Fatal(e)
	}
}
func FuzzDecodeMalformed(f *testing.F) {
	f.Add([]byte{17, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0})
	f.Add([]byte{18, 1, 8, 0, 0x55})
	f.Add([]byte{19, 0, 5, 0, 24, 2})
	f.Add([]byte{3, 1, 8, 127, 12, 128, 255})
	f.Add([]byte{9, 8, 1, 0, 2, 3, 4, 5})
	encodings := []string{"vortex.primitive", "vortex.bool", "vortex.constant", "fastlanes.bitpacked", "vortex.alp", "vortex.alprd", "vortex.dict", "vortex.varbinview", "vortex.varbin", "vortex.fsst", "vortex.runend", "fastlanes.rle", "vortex.sparse", "vortex.decimal", "vortex.zstd", "vortex.datetimeparts", "fastlanes.delta", "vortex.sequence", "vortex.onpair"}
	kinds := []Kind{"i64", "bool", "f64", "u32", "binary", "utf8", "decimal"}
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) < 4 || len(b) > 2048 {
			return
		}
		split := 4 + int(b[3])%(len(b)-3)
		n := &Node{Encoding: encodings[int(b[0])%len(encodings)], Metadata: b[4:split]}
		for i := 0; i < int(b[0])/len(encodings)%4; i++ {
			n.Buffers = append(n.Buffers, b[split:])
		}
		for i := 0; i < int(b[1])/len(kinds)%5; i++ {
			n.Children = append(n.Children, &Node{Encoding: "vortex.primitive", Buffers: [][]byte{b[split:]}})
		}
		typ := Type{Kind: kinds[int(b[1])%len(kinds)], Nullable: true, Precision: 15, Scale: 2}
		d := decoder{ctx: context.Background(), remaining: 1 << 20}
		_, _ = d.decode(n, typ, int(b[2])%65)
	})
}
