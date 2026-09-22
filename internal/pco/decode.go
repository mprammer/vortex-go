// SPDX-License-Identifier: Apache-2.0
// Algorithms derived from pco 1.0.3 (Copyright the pcodec contributors).
// See README.md for provenance. This is an independent Go implementation.

// Package pco decodes the wrapped Pco streams embedded in Vortex arrays.
package pco

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"

	"github.com/apache/arrow-go/v18/arrow/float16"
)

// Decode returns packed little-endian values, before Vortex null expansion.
// Each chunk has one metadata buffer and the page counts describe the flat
// pages slice. Counts are bounded before allocating decoded storage.
func Decode(header []byte, chunks, pages [][]byte, pageCounts [][]uint32, kind string) ([]byte, error) {
	widths := map[string]int{"i8": 8, "u8": 8, "i16": 16, "u16": 16, "f16": 16, "i32": 32, "u32": 32, "f32": 32, "i64": 64, "u64": 64, "f64": 64}
	width, ok := widths[kind]
	if !ok {
		return nil, fmt.Errorf("pco: unsupported type %q", kind)
	}
	if len(header) == 0 || header[0] > 4 || header[0] == 0 || (header[0] >= 4 && (len(header) != 2 || header[1] > 1)) || (header[0] < 4 && len(header) != 1) {
		return nil, fmt.Errorf("pco: unsupported or malformed header")
	}
	if len(chunks) != len(pageCounts) {
		return nil, fmt.Errorf("pco: chunk count mismatch")
	}
	total, np := uint64(0), 0
	for _, counts := range pageCounts {
		for _, n := range counts {
			if n > 1<<24 {
				return nil, fmt.Errorf("pco: oversized page")
			}
			total += uint64(n)
			np++
		}
	}
	if np != len(pages) || total > 1<<24 {
		return nil, fmt.Errorf("pco: invalid page counts or oversized array")
	}
	out := make([]byte, 0, int(total)*width/8)
	pi := 0
	for ci, data := range chunks {
		c, err := parseChunk(data, int(header[0]), width, kind)
		if err != nil {
			return nil, fmt.Errorf("pco chunk %d: %w", ci, err)
		}
		for _, n := range pageCounts[ci] {
			values, err := c.page(pages[pi], int(n))
			if err != nil {
				return nil, fmt.Errorf("pco page %d: %w", pi, err)
			}
			pi++
			for _, v := range values {
				switch width {
				case 8:
					out = append(out, byte(v))
				case 16:
					out = binary.LittleEndian.AppendUint16(out, uint16(v))
				case 32:
					out = binary.LittleEndian.AppendUint32(out, uint32(v))
				case 64:
					out = binary.LittleEndian.AppendUint64(out, v)
				}
			}
		}
	}
	return out, nil
}

type reader struct {
	data []byte
	pos  uint64
	err  error
}

func (r *reader) read(n int) uint64 {
	if r.err != nil {
		return 0
	}
	if n < 0 || n > 64 || uint64(n) > uint64(len(r.data))*8-r.pos {
		r.err = io.ErrUnexpectedEOF
		return 0
	}
	var x uint64
	for done := 0; done < n; {
		shift := int(r.pos % 8)
		take := min(n-done, 8-shift)
		x |= uint64((r.data[r.pos/8]>>shift)&byte((1<<take)-1)) << done
		r.pos += uint64(take)
		done += take
	}
	return x
}
func (r *reader) align() {
	if r.read(int((8-r.pos%8)%8)) != 0 && r.err == nil {
		r.err = fmt.Errorf("nonzero padding")
	}
}
func (r *reader) finish() error {
	r.align()
	if r.err != nil {
		return r.err
	}
	if r.pos != uint64(len(r.data))*8 {
		return fmt.Errorf("trailing bytes")
	}
	return nil
}
func mask(w int) uint64 {
	if w == 64 {
		return math.MaxUint64
	}
	return (uint64(1) << w) - 1
}

type bin struct {
	weight, offset int
	lower          uint64
}
type ansNode struct {
	next, read, offset int
	lower              uint64
}
type latent struct {
	width, log int
	bins       []bin
	table      []ansNode
	delta      bool
}
type chunk struct {
	width                                        int
	kind                                         string
	mode, delta, order, window, quant, convQuant int
	secondary                                    bool
	base                                         uint64
	dict                                         []uint64
	bias                                         int64
	weights                                      []int64
	vars                                         []latent
}

func parseChunk(data []byte, version, width int, kind string) (*chunk, error) {
	r := &reader{data: data}
	c := &chunk{width: width, kind: kind}
	c.mode = int(r.read(4))
	switch c.mode {
	case 0:
	case 1, 2:
		c.base = r.read(width)
	case 3:
		c.quant = int(r.read(8))
	case 4:
		n := r.read(25)
		r.align()
		if n > uint64(len(data))*8/uint64(width) {
			return nil, io.ErrUnexpectedEOF
		}
		c.dict = make([]uint64, int(n))
		for i := range c.dict {
			c.dict[i] = r.read(width)
		}
	default:
		return nil, fmt.Errorf("unknown mode %d", c.mode)
	}
	floating := kind[0] == 'f'
	if c.mode == 1 && (floating || c.base == 0) {
		return nil, fmt.Errorf("invalid integer mode")
	}
	if c.mode == 2 {
		b := floatValue(orderedBits(c.base, width), width)
		if !floating || math.IsNaN(b) || math.IsInf(b, 0) || b == 0 {
			return nil, fmt.Errorf("invalid float multiplier")
		}
	}
	precision := map[int]int{16: 10, 32: 23, 64: 52}[width]
	if c.mode == 3 && (!floating || c.quant < 1 || c.quant > precision) {
		return nil, fmt.Errorf("invalid float quantization")
	}
	if version < 3 {
		c.order = int(r.read(3))
		if c.order > 0 {
			c.delta = 1
		}
	} else {
		c.delta = int(r.read(4))
		switch c.delta {
		case 0:
		case 1:
			c.order = int(r.read(3))
			c.secondary = r.read(1) != 0
			if c.order == 0 {
				return nil, fmt.Errorf("zero delta order")
			}
		case 2:
			wl := 1 + int(r.read(5))
			sl := int(r.read(4))
			c.secondary = r.read(1) != 0
			if wl > 24 || sl > wl {
				return nil, fmt.Errorf("invalid lookback window")
			}
			c.window = 1 << wl
			c.order = 1 << sl
		case 3:
			c.convQuant = int(r.read(5))
			c.bias = int64(r.read(64) ^ (uint64(1) << 63))
			c.order = 1 + int(r.read(5))
			c.weights = make([]int64, c.order)
			for i := range c.weights {
				c.weights[i] = int64(int32(uint32(r.read(32)) ^ (uint32(1) << 31)))
			}
		default:
			return nil, fmt.Errorf("unknown delta variant")
		}
	}
	// Quantization belongs to the mode; convolution has a separate bit count.
	// Keep it separate so FloatQuant and Conv1 compose correctly.
	if c.delta == 2 {
		v, e := parseLatent(r, 32, false)
		if e != nil {
			return nil, e
		}
		for _, b := range v.bins {
			if b.lower < 1 || b.lower > uint64(c.window) {
				return nil, fmt.Errorf("invalid lookback bin")
			}
		}
		c.vars = append(c.vars, v)
	}
	pw := width
	if c.mode == 4 {
		pw = 32
	}
	if c.delta == 3 {
		if pw > 32 {
			return nil, fmt.Errorf("convolution unsupported for 64-bit latents")
		}
		convWidth := pw * 2
		// Rust uses a signed accumulator of twice the latent width. Values
		// serialized as i64/i32 are narrowed before validation there.
		if convWidth == 16 {
			c.bias = int64(int16(c.bias))
			for i := range c.weights {
				c.weights[i] = int64(int16(c.weights[i]))
			}
		} else if convWidth == 32 {
			c.bias = int64(int32(c.bias))
		}
		if c.convQuant >= convWidth {
			return nil, fmt.Errorf("invalid convolution quantization")
		}
		bound := math.Abs(float64(c.bias))
		for _, w := range c.weights {
			bound += math.Ldexp(1, pw) * math.Abs(float64(w))
		}
		if bound >= math.Ldexp(1, convWidth-1) {
			return nil, fmt.Errorf("convolution overflow")
		}
	}
	v, e := parseLatent(r, pw, c.delta != 0)
	if e != nil {
		return nil, e
	}
	c.vars = append(c.vars, v)
	if c.mode >= 1 && c.mode <= 3 {
		v, e := parseLatent(r, width, c.secondary)
		if e != nil {
			return nil, e
		}
		c.vars = append(c.vars, v)
	}
	if err := r.finish(); err != nil {
		return nil, err
	}
	return c, nil
}
func parseLatent(r *reader, w int, delta bool) (latent, error) {
	v := latent{width: w, delta: delta, log: int(r.read(4))}
	n := int(r.read(15))
	if r.err != nil {
		return v, r.err
	}
	if v.log > 14 || n > 1<<v.log || (n <= 1 && v.log != 0) {
		return v, fmt.Errorf("invalid ANS size")
	}
	v.bins = make([]bin, n)
	sum := 0
	for i := range v.bins {
		b := bin{weight: 1 + int(r.read(v.log)), lower: r.read(w), offset: int(r.read(bits.Len(uint(w))))}
		if b.offset > w {
			return v, fmt.Errorf("invalid offset width")
		}
		v.bins[i] = b
		sum += b.weight
	}
	if r.err != nil {
		return v, r.err
	}
	size := 1 << v.log
	if n == 0 {
		return v, nil
	}
	if sum != size {
		return v, fmt.Errorf("ANS weights do not sum to table size")
	}
	symbols := make([]int, size)
	stride := (3 * size) / 5
	if stride%2 == 0 {
		stride++
	}
	step := 0
	for s, b := range v.bins {
		for range b.weight {
			symbols[(step*stride)&(size-1)] = s
			step++
		}
	}
	next := make([]int, n)
	for i, b := range v.bins {
		next[i] = b.weight
	}
	v.table = make([]ansNode, size)
	for i, s := range symbols {
		nb := bits.Len(uint(size)) - bits.Len(uint(next[s]))
		b := v.bins[s]
		v.table[i] = ansNode{next: (next[s] << nb) - size, read: nb, offset: b.offset, lower: b.lower}
		next[s]++
	}
	return v, nil
}

type pageVar struct {
	state  []uint64
	ans    [4]int
	values []uint64
}

func (c *chunk) page(data []byte, n int) ([]uint64, error) {
	r := &reader{data: data}
	vs := make([]pageVar, len(c.vars))
	for i, v := range c.vars {
		if v.delta {
			vs[i].state = make([]uint64, c.order)
			for j := range vs[i].state {
				vs[i].state[j] = r.read(v.width)
			}
		}
		for j := range vs[i].ans {
			vs[i].ans[j] = int(r.read(v.log))
		}
		vs[i].values = make([]uint64, 0, n)
	}
	r.align()
	if r.err != nil {
		return nil, r.err
	}
	for start := 0; start < n; start += 256 {
		for i, v := range c.vars {
			limit := min(256, n-start)
			if v.delta || (c.delta == 2 && i == 0) {
				limit = min(256, max(0, n-start-c.order))
			}
			vals, e := readLatents(r, v, &vs[i], limit)
			if e != nil {
				return nil, e
			}
			vs[i].values = append(vs[i].values, vals...)
		}
	}
	if e := r.finish(); e != nil {
		return nil, e
	}
	var lookbacks []uint64
	primary := 0
	if c.delta == 2 {
		lookbacks = vs[0].values
		primary = 1
	}
	for i := primary; i < len(vs); i++ {
		if c.vars[i].delta {
			a, e := c.restore(vs[i].values, vs[i].state, lookbacks, n, c.vars[i].width)
			if e != nil {
				return nil, e
			}
			vs[i].values = a
		}
	}
	p := vs[primary].values
	var s []uint64
	if primary+1 < len(vs) {
		s = vs[primary+1].values
	}
	out := make([]uint64, n)
	for i := range out {
		v := p[i]
		switch c.mode {
		case 1:
			v = v*c.base + s[i]
		case 2:
			product := floatProduct(intFloat(v, c.width), floatValue(orderedBits(c.base, c.width), c.width), c.width)
			v = (floatOrdered(product, c.width) + s[i]) ^ (uint64(1) << (c.width - 1))
		case 3:
			low := s[i]
			if low > mask(c.quant) {
				return nil, fmt.Errorf("invalid float quantization residual")
			}
			if v < (uint64(1)<<(c.width-1))>>c.quant {
				low = mask(c.quant) - low
			}
			v = (v << c.quant) + low
		case 4:
			if v >= uint64(len(c.dict)) {
				return nil, fmt.Errorf("dictionary index out of bounds")
			}
			v = c.dict[v]
		}
		v &= mask(c.width)
		if c.kind[0] == 'f' {
			v = orderedBits(v, c.width)
		} else if c.kind[0] == 'i' {
			v ^= uint64(1) << (c.width - 1)
		}
		out[i] = v
	}
	return out, nil
}
func readLatents(r *reader, v latent, p *pageVar, n int) ([]uint64, error) {
	out := make([]uint64, n)
	widths := make([]int, n)
	if n > 0 && len(v.bins) == 0 {
		return nil, fmt.Errorf("no bins for nonempty data")
	}
	for i := range out {
		state := p.ans[i%4]
		if state < 0 || state >= len(v.table) {
			return nil, fmt.Errorf("ANS state out of bounds")
		}
		a := v.table[state]
		out[i] = a.lower
		widths[i] = a.offset
		p.ans[i%4] = a.next + int(r.read(a.read))
	}
	for i := range out {
		out[i] = (out[i] + r.read(widths[i])) & mask(v.width)
	}
	return out, r.err
}
func (c *chunk) restore(raw, state, look []uint64, n, w int) ([]uint64, error) {
	m := mask(w)
	mid := uint64(1) << (w - 1)
	switch c.delta {
	case 1:
		out := make([]uint64, n)
		copy(out, raw)
		for i := range out {
			out[i] ^= mid
		}
		for k := len(state) - 1; k >= 0; k-- {
			moment := state[k]
			for i, d := range out {
				out[i] = moment
				moment = (moment + d) & m
			}
		}
		return out, nil
	case 2:
		// A short page stores its initial values right-aligned in the state.
		out := make([]uint64, 0, n)
		initial := min(n, len(state))
		out = append(out, state[len(state)-initial:]...)
		for i, d := range raw {
			back := look[i]
			pos := len(out)
			if back == 0 || back > uint64(c.window) {
				return nil, fmt.Errorf("invalid lookback")
			}
			prev := uint64(0)
			if back <= uint64(pos) {
				prev = out[pos-int(back)]
			}
			out = append(out, (prev+(d^mid))&m)
		}
		return out, nil
	case 3:
		out := make([]uint64, 0, n)
		out = append(out, state[:min(n, len(state))]...)
		for _, d := range raw {
			sum := c.bias
			start := len(out) - c.order
			for j, weight := range c.weights {
				sum += int64(out[start+j]) * weight
			}
			if sum < 0 {
				sum = 0
			}
			out = append(out, ((uint64(sum)>>c.convQuant)+(d^mid))&m)
		}
		return out, nil
	}
	return raw, nil
}
func orderedBits(v uint64, w int) uint64 {
	mid := uint64(1) << (w - 1)
	if v&mid != 0 {
		return v ^ mid
	}
	return ^v & mask(w)
}
func floatValue(v uint64, w int) float64 {
	switch w {
	case 16:
		return float64(float16.FromBits(uint16(v)).Float32())
	case 32:
		return float64(math.Float32frombits(uint32(v)))
	default:
		return math.Float64frombits(v)
	}
}
func floatBits(v float64, w int) uint64 {
	switch w {
	case 16:
		return uint64(float16.New(float32(v)).Uint16())
	case 32:
		return uint64(math.Float32bits(float32(v)))
	default:
		return math.Float64bits(v)
	}
}
func floatOrdered(v float64, w int) uint64 {
	b := floatBits(v, w)
	mid := uint64(1) << (w - 1)
	if b&mid != 0 {
		return ^b & mask(w)
	}
	return b ^ mid
}
func floatProduct(a, b float64, w int) float64 {
	switch w {
	case 16:
		return float64(float16.New(float32(a) * float32(b)).Float32())
	case 32:
		return float64(float32(a) * float32(b))
	default:
		return a * b
	}
}
func intFloat(v uint64, w int) float64 {
	mid := uint64(1) << (w - 1)
	negative := v < mid
	var a uint64
	if negative {
		a = mid - 1 - v
	} else {
		a = v - mid
	}
	precision := map[int]int{16: 11, 32: 24, 64: 53}[w]
	gpi := uint64(1) << precision
	var f float64
	if a < gpi {
		f = float64(a)
	} else {
		f = floatValue((floatBits(float64(gpi), w)+a-gpi)&mask(w), w)
	}
	if negative {
		f = -f
	}
	return f
}
