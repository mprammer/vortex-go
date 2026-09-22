// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 the vortex-go contributors.
package encoding

import (
	"encoding/binary"
	"fmt"
)

// A deliberately small protobuf wire reader. Slices alias the bounded metadata
// input; unknown fields are skipped without allocation proportional to a length.
type protoField struct {
	tag  int
	wire byte
	num  uint64
	data []byte
}
type proto []protoField

func parseProto(b []byte) (proto, error) {
	var out proto
	for len(b) > 0 {
		k, n := binary.Uvarint(b)
		if n <= 0 || k>>3 == 0 {
			return nil, fmt.Errorf("invalid protobuf field")
		}
		b = b[n:]
		f := protoField{tag: int(k >> 3), wire: byte(k & 7)}
		switch f.wire {
		case 0:
			v, n := binary.Uvarint(b)
			if n <= 0 {
				return nil, fmt.Errorf("invalid protobuf varint")
			}
			f.num = v
			b = b[n:]
		case 1:
			if len(b) < 8 {
				return nil, fmt.Errorf("truncated protobuf fixed64")
			}
			f.num = binary.LittleEndian.Uint64(b)
			b = b[8:]
		case 2:
			v, n := binary.Uvarint(b)
			if n <= 0 {
				return nil, fmt.Errorf("invalid protobuf length")
			}
			b = b[n:]
			if v > uint64(len(b)) {
				return nil, fmt.Errorf("truncated protobuf bytes")
			}
			f.data = b[:int(v)]
			b = b[int(v):]
		case 5:
			if len(b) < 4 {
				return nil, fmt.Errorf("truncated protobuf fixed32")
			}
			f.num = uint64(binary.LittleEndian.Uint32(b))
			b = b[4:]
		default:
			return nil, fmt.Errorf("unsupported protobuf wire type %d", f.wire)
		}
		if len(out) >= 1<<18 {
			return nil, fmt.Errorf("too many protobuf fields")
		}
		out = append(out, f)
	}
	return out, nil
}
func (p proto) get(tag int) (protoField, bool) {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i].tag == tag {
			return p[i], true
		}
	}
	return protoField{}, false
}
func (p proto) num(tag int) uint64   { f, _ := p.get(tag); return f.num }
func (p proto) bytes(tag int) []byte { f, _ := p.get(tag); return f.data }
func (p proto) has(tag int) bool     { _, ok := p.get(tag); return ok }
func (p proto) repeated(tag int) ([]uint64, error) {
	var v []uint64
	for _, f := range p {
		if f.tag != tag {
			continue
		}
		if f.wire == 0 {
			v = append(v, f.num)
			continue
		}
		if f.wire != 2 {
			return nil, fmt.Errorf("invalid repeated integer")
		}
		b := f.data
		for len(b) > 0 {
			x, n := binary.Uvarint(b)
			if n <= 0 {
				return nil, fmt.Errorf("invalid packed integer")
			}
			v = append(v, x)
			b = b[n:]
		}
	}
	return v, nil
}
