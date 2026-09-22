// SPDX-License-Identifier: Apache-2.0

package vortex

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/mprammer/vortex-go/internal/encoding"
	"github.com/mprammer/vortex-go/internal/wire/fb"
	"google.golang.org/protobuf/encoding/protowire"
)

func (f *File) decodeFlat(ctx context.Context, l *layout, t encoding.Type) (out arrow.Array, err error) {
	defer recoverParse(&err)
	if l.id != "vortex.flat" || len(l.children) != 0 || len(l.segments) != 1 {
		return nil, fmt.Errorf("%w: flat layout shape", ErrInvalid)
	}
	if l.rows < 0 || uint64(l.rows) > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("%w: array length", ErrInvalid)
	}
	seg := f.segments[l.segments[0]]
	b, err := f.read(ctx, seg.offset, seg.length, maxSegment)
	if err != nil {
		return nil, err
	}
	var tree []byte
	if len(l.metadata) > 0 {
		fields, e := protoFields(l.metadata)
		if e != nil {
			return nil, e
		}
		if x := fields[1]; len(x) > 0 {
			tree = x[len(x)-1].bytes
		}
	}
	if tree == nil {
		if len(b) < 4 {
			return nil, fmt.Errorf("%w: array trailer", ErrInvalid)
		}
		n := uint64(binary.LittleEndian.Uint32(b[len(b)-4:]))
		if n > uint64(len(b)-4) {
			return nil, fmt.Errorf("%w: array metadata length", ErrInvalid)
		}
		start := len(b) - 4 - int(n)
		tree = b[start : len(b)-4]
		b = b[:start]
	}
	node, err := f.parseArray(tree, b)
	if err != nil {
		return nil, err
	}
	return encoding.Decode(ctx, node, t, int(l.rows))
}
func (f *File) parseArray(tree, b []byte) (*encoding.Node, error) {
	if err := checkRoot(tree); err != nil {
		return nil, err
	}
	a := fb.GetRootAsArray(tree, 0)
	if err := checkCount(a.BuffersLength(), tree, 8); err != nil {
		return nil, err
	}
	buffers := make([][]byte, a.BuffersLength())
	offset := uint64(0)
	for i := range buffers {
		var v fb.Buffer
		a.Buffers(&v, i)
		if v.AlignmentExponent() > 30 {
			return nil, fmt.Errorf("%w: array buffer alignment", ErrInvalid)
		}
		if v.Compression() != fb.CompressionNone {
			return nil, fmt.Errorf("%w: array buffer compression", ErrUnsupported)
		}
		start := offset + uint64(v.Padding())
		end := start + uint64(v.Length())
		if end > uint64(len(b)) {
			return nil, fmt.Errorf("%w: array buffer range", ErrInvalid)
		}
		buffers[i] = b[start:end]
		offset = end
	}
	budget := maxNodes
	return f.parseNode(a.Root(nil), buffers, 0, &budget)
}
func (f *File) parseNode(v *fb.ArrayNode, buffers [][]byte, depth int, budget *int) (*encoding.Node, error) {
	if v == nil {
		return nil, fmt.Errorf("%w: missing array node", ErrInvalid)
	}
	if err := nodeBudget(depth, budget); err != nil {
		return nil, err
	}
	if int(v.Encoding()) >= len(f.arrays) {
		return nil, fmt.Errorf("%w: unknown encoding index", ErrInvalid)
	}
	if err := checkCount(v.ChildrenLength(), v.Table().Bytes, 4); err != nil {
		return nil, err
	}
	if err := checkCount(v.BuffersLength(), v.Table().Bytes, 2); err != nil {
		return nil, err
	}
	out := &encoding.Node{Encoding: f.arrays[v.Encoding()], Metadata: v.MetadataBytes()}
	for i := 0; i < v.BuffersLength(); i++ {
		idx := int(v.Buffers(i))
		if idx >= len(buffers) {
			return nil, fmt.Errorf("%w: unknown array buffer", ErrInvalid)
		}
		out.Buffers = append(out.Buffers, buffers[idx])
	}
	for i := 0; i < v.ChildrenLength(); i++ {
		var child fb.ArrayNode
		v.Children(&child, i)
		c, err := f.parseNode(&child, buffers, depth+1, budget)
		if err != nil {
			return nil, err
		}
		out.Children = append(out.Children, c)
	}
	return out, nil
}

type protoValue struct {
	number uint64
	bytes  []byte
	kind   protowire.Type
}

func protoFields(b []byte) (map[protowire.Number][]protoValue, error) {
	out := map[protowire.Number][]protoValue{}
	fields := 0
	for len(b) > 0 {
		fields++
		if fields > maxNodes {
			return nil, fmt.Errorf("%w: protobuf field count", ErrInvalid)
		}
		tag, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil, fmt.Errorf("%w: protobuf tag", ErrInvalid)
		}
		b = b[n:]
		value := protoValue{kind: typ}
		switch typ {
		case protowire.VarintType:
			value.number, n = protowire.ConsumeVarint(b)
		case protowire.BytesType:
			value.bytes, n = protowire.ConsumeBytes(b)
		case protowire.Fixed32Type:
			var v uint32
			v, n = protowire.ConsumeFixed32(b)
			value.number = uint64(v)
		case protowire.Fixed64Type:
			value.number, n = protowire.ConsumeFixed64(b)
		default:
			return nil, fmt.Errorf("%w: protobuf wire type", ErrInvalid)
		}
		if n < 0 {
			return nil, fmt.Errorf("%w: truncated protobuf field", ErrInvalid)
		}
		b = b[n:]
		out[tag] = append(out[tag], value)
	}
	return out, nil
}
