// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 the vortex-go contributors.
package encoding

import (
	"encoding/binary"
	"fmt"
	"unicode/utf8"

	"github.com/klauspost/compress/zstd"
	"github.com/mprammer/vortex-go/internal/pco"
)

func (d *decoder) varbin(n *Node, t Type, length int, m proto) (*values, error) {
	if t.Kind != "utf8" && t.Kind != "binary" {
		return nil, fmt.Errorf("variable binary requires binary/UTF8 type")
	}
	out, e := d.empty(t, length)
	if e != nil {
		return nil, e
	}
	if n.Encoding == "vortex.varbin" {
		data, e := oneBuffer(n)
		if e != nil {
			return nil, e
		}
		ot, e := primitiveType(m.num(1), false)
		if e != nil {
			return nil, e
		}
		if !ot.integer() {
			return nil, fmt.Errorf("offsets require integer type")
		}
		offsets, e := d.child(n, 0, ot, length+1)
		if e != nil {
			return nil, e
		}
		for i := range out.bytes {
			a, b := offsets.bits[i], offsets.bits[i+1]
			if a > b || b > uint64(len(data)) {
				return nil, fmt.Errorf("invalid variable binary offsets")
			}
			out.bytes[i] = data[a:b]
		}
		out.valid, e = d.validity(n, 1, length)
		if e != nil {
			return nil, e
		}
	} else {
		if len(n.Buffers) == 0 {
			return nil, fmt.Errorf("missing views buffer")
		}
		views := n.Buffers[len(n.Buffers)-1]
		if len(views) != length*16 {
			return nil, fmt.Errorf("incomplete views buffer")
		}
		out.valid, e = d.validity(n, 0, length)
		if e != nil {
			return nil, e
		}
		for i := range out.bytes {
			if !out.isValid(i) {
				continue
			}
			v := views[i*16 : (i+1)*16]
			size := uint64(binary.LittleEndian.Uint32(v))
			if size <= 12 {
				out.bytes[i] = v[4 : 4+size]
				continue
			}
			buffer := uint64(binary.LittleEndian.Uint32(v[8:]))
			offset := uint64(binary.LittleEndian.Uint32(v[12:]))
			if buffer >= uint64(len(n.Buffers)-1) {
				return nil, fmt.Errorf("view buffer index out of bounds")
			}
			data := n.Buffers[buffer]
			if offset > uint64(len(data)) || size > uint64(len(data))-offset {
				return nil, fmt.Errorf("view byte range out of bounds")
			}
			value := data[offset : offset+size]
			if string(v[4:8]) != string(value[:4]) {
				return nil, fmt.Errorf("view prefix mismatch")
			}
			out.bytes[i] = value
		}
	}
	if t.Kind == "utf8" {
		for i, b := range out.bytes {
			if out.isValid(i) && !utf8.Valid(b) {
				return nil, fmt.Errorf("invalid UTF8 at row %d", i)
			}
		}
	}
	return out, nil
}

func (d *decoder) fsst(n *Node, t Type, length int, m proto) (*values, error) {
	if t.Kind != "utf8" && t.Kind != "binary" {
		return nil, fmt.Errorf("FSST requires binary/UTF8")
	}
	if len(n.Buffers) != 2 && len(n.Buffers) != 3 {
		return nil, fmt.Errorf("FSST requires 2 or 3 buffers")
	}
	symbols, sizes := n.Buffers[0], n.Buffers[1]
	if len(symbols)%8 != 0 || len(symbols)/8 != len(sizes) || len(sizes) > 255 {
		return nil, fmt.Errorf("invalid FSST symbol table")
	}
	for _, s := range sizes {
		if s > 8 {
			return nil, fmt.Errorf("invalid FSST symbol size")
		}
	}
	var codes, lens *values
	var e error
	lt, e := primitiveType(m.num(1), false)
	if e != nil {
		return nil, e
	}
	if !lt.integer() {
		return nil, fmt.Errorf("FSST lengths require integer type")
	}
	if len(n.Buffers) == 2 {
		codes, e = d.child(n, 0, Type{Kind: "binary", Nullable: t.Nullable}, length)
		if e != nil {
			return nil, e
		}
		lens, e = d.child(n, 1, lt, length)
	} else {
		lens, e = d.child(n, 0, lt, length)
		if e != nil {
			return nil, e
		}
		ot, e := primitiveType(m.num(2), false)
		if e != nil {
			return nil, e
		}
		if !ot.integer() {
			return nil, fmt.Errorf("FSST offsets require integer type")
		}
		offsets, e := d.child(n, 1, ot, length+1)
		if e != nil {
			return nil, e
		}
		codes, e = d.empty(Type{Kind: "binary", Nullable: t.Nullable}, length)
		if e != nil {
			return nil, e
		}
		data := n.Buffers[2]
		for i := range codes.bytes {
			a, b := offsets.bits[i], offsets.bits[i+1]
			if a > b || b > uint64(len(data)) {
				return nil, fmt.Errorf("FSST code offsets out of bounds")
			}
			codes.bytes[i] = data[a:b]
		}
		codes.valid, e = d.validity(n, 2, length)
	}
	if e != nil {
		return nil, e
	}
	out, e := d.empty(t, length)
	if e != nil {
		return nil, e
	}
	out.valid = codes.valid
	for i, c := range codes.bytes {
		if !out.isValid(i) {
			continue
		}
		expected := lens.bits[i]
		if expected > uint64(d.remaining) {
			return nil, fmt.Errorf("FSST value exceeds allocation limit")
		}
		if e = d.reserve(int64(expected)); e != nil {
			return nil, e
		}
		b := make([]byte, 0, int(expected))
		for j := 0; j < len(c); j++ {
			code := int(c[j])
			if code == 255 {
				j++
				if j >= len(c) {
					return nil, fmt.Errorf("truncated FSST escape")
				}
				if uint64(len(b))+1 > expected {
					return nil, fmt.Errorf("FSST exceeds declared length")
				}
				b = append(b, c[j])
			} else {
				if code >= len(sizes) || sizes[code] == 0 {
					return nil, fmt.Errorf("invalid FSST symbol code")
				}
				size := int(sizes[code])
				if uint64(len(b)+size) > expected {
					return nil, fmt.Errorf("FSST exceeds declared length")
				}
				b = append(b, symbols[code*8:code*8+size]...)
			}
		}
		if uint64(len(b)) != expected {
			return nil, fmt.Errorf("FSST decoded length mismatch")
		}
		if t.Kind == "utf8" && !utf8.Valid(b) {
			return nil, fmt.Errorf("FSST invalid UTF8")
		}
		out.bytes[i] = b
	}
	return out, nil
}
func (d *decoder) zstd(n *Node, t Type, length int, m proto) (*values, error) {
	out, e := d.empty(t, length)
	if e != nil {
		return nil, e
	}
	if t.Kind != "utf8" && t.Kind != "binary" && t.width() == 0 {
		return nil, fmt.Errorf("unsupported Zstd type")
	}
	out.valid, e = d.validity(n, 0, length)
	if e != nil {
		return nil, e
	}
	opts := []zstd.DOption{zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(maxDecodeBytes), zstd.WithDecoderMaxWindow(maxDecodeBytes), zstd.WithDecodeAllCapLimit(true)}
	buffers := n.Buffers
	if m.num(1) != 0 {
		if len(buffers) == 0 || uint64(len(buffers[0])) != m.num(1) {
			return nil, fmt.Errorf("invalid Zstd dictionary size")
		}
		opts = append(opts, zstd.WithDecoderDicts(buffers[0]))
		buffers = buffers[1:]
	}
	z, e := zstd.NewReader(nil, opts...)
	if e != nil {
		return nil, e
	}
	defer z.Close()
	var frames []proto
	for _, f := range m {
		if f.tag == 2 {
			p, e := parseProto(f.data)
			if e != nil {
				return nil, e
			}
			frames = append(frames, p)
		}
	}
	if len(frames) != len(buffers) {
		return nil, fmt.Errorf("Zstd frame count mismatch")
	}
	row := 0
	for i, frame := range frames {
		size := frame.num(1)
		if size > uint64(d.remaining) {
			return nil, fmt.Errorf("Zstd output exceeds allocation limit")
		}
		if e = d.reserve(int64(size)); e != nil {
			return nil, e
		}
		var header zstd.Header
		if e = header.Decode(buffers[i]); e != nil {
			return nil, e
		}
		if !header.HasFCS || header.FrameContentSize != size {
			return nil, fmt.Errorf("Zstd frame content size mismatch")
		}
		raw, e := z.DecodeAll(buffers[i], make([]byte, 0, int(size)))
		if e != nil {
			return nil, e
		}
		if uint64(len(raw)) != size {
			return nil, fmt.Errorf("Zstd decoded size mismatch")
		}
		count := uint64(0)
		for pos := 0; pos < len(raw); {
			for row < length && !out.isValid(row) {
				row++
			}
			if row >= length {
				return nil, fmt.Errorf("Zstd contains too many values")
			}
			if out.bits != nil {
				w := t.width()
				if len(raw)-pos < w {
					return nil, fmt.Errorf("truncated Zstd primitive")
				}
				out.bits[row] = readWord(raw[pos:], w)
				pos += w
			} else {
				if len(raw)-pos < 4 {
					return nil, fmt.Errorf("truncated Zstd length prefix")
				}
				size := uint64(binary.LittleEndian.Uint32(raw[pos:]))
				pos += 4
				if size > uint64(len(raw)-pos) {
					return nil, fmt.Errorf("truncated Zstd value")
				}
				out.bytes[row] = raw[pos : pos+int(size)]
				if t.Kind == "utf8" && !utf8.Valid(out.bytes[row]) {
					return nil, fmt.Errorf("Zstd invalid UTF8")
				}
				pos += int(size)
			}
			row++
			count++
		}
		if frame.num(2) != 0 && frame.num(2) != count {
			return nil, fmt.Errorf("Zstd frame value count mismatch")
		}
	}
	for row < length && !out.isValid(row) {
		row++
	}
	if row != length {
		return nil, fmt.Errorf("Zstd contains too few values")
	}
	return out, nil
}
func (d *decoder) pco(n *Node, t Type, length int, m proto) (*values, error) {
	if t.width() == 0 || t.Kind == "bool" {
		return nil, fmt.Errorf("PCO requires primitive type")
	}
	out, e := d.empty(t, length)
	if e != nil {
		return nil, e
	}
	out.valid, e = d.validity(n, 0, length)
	if e != nil {
		return nil, e
	}
	var counts [][]uint32
	var total uint64
	for _, f := range m {
		if f.tag != 2 {
			continue
		}
		chunk, e := parseProto(f.data)
		if e != nil {
			return nil, e
		}
		var pages []uint32
		for _, p := range chunk {
			if p.tag != 1 {
				continue
			}
			page, e := parseProto(p.data)
			if e != nil {
				return nil, e
			}
			cnt := page.num(1)
			if cnt > 1<<24 {
				return nil, fmt.Errorf("PCO page exceeds value limit")
			}
			total += cnt
			if total > uint64(length) {
				return nil, fmt.Errorf("PCO has too many values")
			}
			pages = append(pages, uint32(cnt))
		}
		counts = append(counts, pages)
	}
	if len(n.Buffers) < len(counts) {
		return nil, fmt.Errorf("PCO missing chunk metadata")
	}
	validCount := 0
	for i := 0; i < length; i++ {
		if out.isValid(i) {
			validCount++
		}
	}
	if total != uint64(validCount) {
		return nil, fmt.Errorf("PCO count differs from validity")
	}
	if e = d.reserve(int64(total) * int64(t.width())); e != nil {
		return nil, e
	}
	raw, e := pco.Decode(m.bytes(1), n.Buffers[:len(counts)], n.Buffers[len(counts):], counts, string(t.Kind))
	if e != nil {
		return nil, e
	}
	if len(raw) != validCount*t.width() {
		return nil, fmt.Errorf("PCO output length mismatch")
	}
	pos := 0
	for i := range out.bits {
		if out.isValid(i) {
			out.bits[i] = readWord(raw[pos:], t.width())
			pos += t.width()
		}
	}
	return out, nil
}
func (d *decoder) decimal(n *Node, t Type, length int, m proto) (*values, error) {
	if t.Kind != "decimal" {
		return nil, fmt.Errorf("decimal encoding requires decimal type")
	}
	if _, e := t.ArrowType(); e != nil {
		return nil, e
	}
	p := m.num(1)
	if p > 5 {
		return nil, fmt.Errorf("invalid decimal storage width")
	}
	w := 1 << p
	b, e := oneBuffer(n)
	if e != nil {
		return nil, e
	}
	if len(b) != length*w {
		return nil, fmt.Errorf("incomplete decimal buffer")
	}
	out, e := d.empty(t, length)
	if e != nil {
		return nil, e
	}
	ow := 16
	if t.Precision > 38 {
		ow = 32
	}
	for i := 0; i < length; i++ {
		raw := b[i*w : (i+1)*w]
		if w > ow {
			sign := byte(0)
			if raw[ow-1]&128 != 0 {
				sign = 255
			}
			for _, x := range raw[ow:] {
				if x != sign {
					return nil, fmt.Errorf("decimal exceeds storage width")
				}
			}
			raw = raw[:ow]
		}
		if raw[len(raw)-1]&128 != 0 {
			for j := 0; j < ow; j++ {
				out.wide[i*ow+j] = 255
			}
		}
		copy(out.wide[i*ow:(i+1)*ow], raw)
	}
	out.valid, e = d.validity(n, 0, length)
	return out, e
}

// OnPair keeps a dictionary of short tokens in one blob and encodes each row as
// a run of token codes. Decoding concatenates the tokens a row's codes name:
// unlike FSST there are no escapes, and tokens are stored already expanded, so
// a code never refers to another code.
//
// The dictionary blob is the only buffer. The children are the dictionary
// offsets, the code stream, the per-row code boundaries, the per-row decoded
// lengths, and an optional validity child. A sliced array keeps the whole code
// stream and narrows only the boundaries, so the codes a row names need not
// start at the beginning of the stream.
func (d *decoder) onpair(n *Node, t Type, length int, m proto) (*values, error) {
	if t.Kind != "utf8" && t.Kind != "binary" {
		return nil, fmt.Errorf("OnPair requires binary/UTF8")
	}
	dict, e := oneBuffer(n)
	if e != nil {
		return nil, e
	}
	dictSize, e := boundedInt(m.num(3))
	if e != nil {
		return nil, e
	}
	codesLen, e := boundedInt(m.num(4))
	if e != nil {
		return nil, e
	}
	offsetsType, e := primitiveType(m.num(5), false)
	if e != nil {
		return nil, e
	}
	codesType, e := primitiveType(m.num(6), false)
	if e != nil {
		return nil, e
	}
	boundsType, e := primitiveType(m.num(7), false)
	if e != nil {
		return nil, e
	}
	lengthsType, e := primitiveType(m.num(1), false)
	if e != nil {
		return nil, e
	}
	for _, ct := range []Type{offsetsType, codesType, boundsType, lengthsType} {
		if !ct.integer() || ct.signed() {
			return nil, fmt.Errorf("OnPair children require unsigned integers")
		}
	}
	dictOffsets, e := d.child(n, 0, offsetsType, dictSize+1)
	if e != nil {
		return nil, e
	}
	codes, e := d.child(n, 1, codesType, codesLen)
	if e != nil {
		return nil, e
	}
	bounds, e := d.child(n, 2, boundsType, length+1)
	if e != nil {
		return nil, e
	}
	lens, e := d.child(n, 3, lengthsType, length)
	if e != nil {
		return nil, e
	}
	out, e := d.empty(t, length)
	if e != nil {
		return nil, e
	}
	out.valid, e = d.validity(n, 4, length)
	if e != nil {
		return nil, e
	}
	// A code cannot name a token the code width could not address, so a
	// dictionary larger than that is malformed rather than merely unusual.
	if width := codesType.width() * 8; width < 64 && uint64(dictSize) > uint64(1)<<width {
		return nil, fmt.Errorf("OnPair dictionary larger than the code width can address")
	}
	// Resolve the dictionary once: every row indexes the same token table.
	// The slice headers are charged to the decoder's budget like any other
	// allocation it makes on behalf of the file.
	if e = d.reserve(int64(dictSize) * 24); e != nil {
		return nil, e
	}
	tokens := make([][]byte, dictSize)
	var prev uint64
	for i := range tokens {
		a, b := dictOffsets.bits[i], dictOffsets.bits[i+1]
		if a < prev || a > b || b > uint64(len(dict)) {
			return nil, fmt.Errorf("OnPair dictionary offsets out of bounds")
		}
		if a == b {
			// An empty token would let a row consume codes without ever
			// reaching its decoded length, so the work per row would not be
			// bounded by anything the file declares.
			return nil, fmt.Errorf("OnPair dictionary token %d is empty", i)
		}
		prev = b
		tokens[i] = dict[a:b]
	}
	for i := 0; i < length; i++ {
		if !out.isValid(i) {
			continue
		}
		a, b := bounds.bits[i], bounds.bits[i+1]
		if a > b || b > uint64(codesLen) {
			return nil, fmt.Errorf("OnPair code offsets out of bounds")
		}
		expected := lens.bits[i]
		// Every token is at least one byte, so a row cannot need more codes
		// than the bytes it decodes to. Without this a row may name the whole
		// code stream and the total work escapes what the offsets bound.
		if b-a > expected {
			return nil, fmt.Errorf("OnPair row %d names more codes than its decoded length", i)
		}
		if expected > uint64(d.remaining) {
			return nil, fmt.Errorf("OnPair value exceeds allocation limit")
		}
		if e = d.reserve(int64(expected)); e != nil {
			return nil, e
		}
		v := make([]byte, 0, int(expected))
		for _, c := range codes.bits[a:b] {
			if c >= uint64(dictSize) {
				return nil, fmt.Errorf("invalid OnPair token code")
			}
			token := tokens[c]
			if uint64(len(v)+len(token)) > expected {
				return nil, fmt.Errorf("OnPair exceeds declared length")
			}
			v = append(v, token...)
		}
		if uint64(len(v)) != expected {
			return nil, fmt.Errorf("OnPair decoded length mismatch")
		}
		if t.Kind == "utf8" && !utf8.Valid(v) {
			return nil, fmt.Errorf("OnPair invalid UTF8")
		}
		out.bytes[i] = v
	}
	return out, nil
}
