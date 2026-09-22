// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 the vortex-go contributors.
package encoding

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func (v *values) arrow(ctx context.Context) (arrow.Array, error) {
	dt, e := v.typ.ArrowType()
	if e != nil {
		return nil, e
	}
	nulls := 0
	var validity []byte
	if v.valid != nil {
		validity = make([]byte, (v.n+7)/8)
		for i, b := range v.valid {
			if b {
				validity[i/8] |= 1 << uint(i%8)
			} else {
				nulls++
			}
		}
	}
	if v.typ.Kind == "null" {
		return array.NewNull(v.n), nil
	}
	var raw [][]byte
	var children []arrow.ArrayData
	var arrays []arrow.Array
	defer func() {
		for _, a := range arrays {
			a.Release()
		}
	}()
	switch {
	case v.typ.Kind == "struct":
		raw = [][]byte{validity}
		for _, f := range v.fields {
			a, e := f.arrow(ctx)
			if e != nil {
				return nil, e
			}
			arrays = append(arrays, a)
			children = append(children, a.Data())
		}
	case v.bytes != nil:
		offsets := make([]byte, (v.n+1)*4)
		total := int64(0)
		for i, b := range v.bytes {
			if v.isValid(i) {
				total += int64(len(b))
			}
			if total > maxDecodeBytes {
				return nil, fmt.Errorf("Arrow binary data exceeds allocation limit")
			}
			binary.LittleEndian.PutUint32(offsets[(i+1)*4:], uint32(total))
		}
		data := make([]byte, 0, int(total))
		for i, b := range v.bytes {
			if v.isValid(i) {
				data = append(data, b...)
			}
		}
		raw = [][]byte{validity, offsets, data}
	case v.wide != nil:
		w := 16
		if v.typ.Precision > 38 {
			w = 32
		}
		bound := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(v.typ.Precision)), nil)
		negative := new(big.Int).Neg(bound)
		mod := new(big.Int).Lsh(big.NewInt(1), uint(w*8))
		scratch := make([]byte, w)
		num := new(big.Int)
		for i := 0; i < v.n; i++ {
			if !v.isValid(i) {
				continue
			}
			b := v.wide[i*w : (i+1)*w]
			for j, x := range b {
				scratch[w-1-j] = x
			}
			num.SetBytes(scratch)
			if b[w-1]&128 != 0 {
				num.Sub(num, mod)
			}
			if num.Cmp(bound) >= 0 || num.Cmp(negative) <= 0 {
				return nil, fmt.Errorf("decimal exceeds declared precision %d", v.typ.Precision)
			}
		}
		raw = [][]byte{validity, v.wide}
	case v.typ.Kind == "bool":
		data := make([]byte, (v.n+7)/8)
		for i, b := range v.bits {
			if b != 0 {
				data[i/8] |= 1 << uint(i%8)
			}
		}
		raw = [][]byte{validity, data}
	default:
		typ := v.typ
		if typ.Kind == "extension" {
			typ = *typ.Storage
		}
		w := typ.width()
		if w == 0 {
			return nil, fmt.Errorf("unsupported Arrow storage %q", typ.Kind)
		}
		data := make([]byte, v.n*w)
		for i, x := range v.bits {
			writeWord(data[i*w:], w, x)
		}
		raw = [][]byte{validity, data}
	}
	buffers := make([]*memory.Buffer, len(raw))
	for i, b := range raw {
		if b != nil {
			buffers[i] = memory.NewResizableBuffer(compute.GetAllocator(ctx))
			buffers[i].Resize(len(b))
			copy(buffers[i].Bytes(), b)
		}
	}
	data := array.NewData(dt, v.n, buffers, children, nulls, 0)
	for _, b := range buffers {
		if b != nil {
			b.Release()
		}
	}
	defer data.Release()
	return array.MakeFromData(data), nil
}
