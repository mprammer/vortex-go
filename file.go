// SPDX-License-Identifier: Apache-2.0

// Package vortex reads Vortex files using native Go decoders.
package vortex

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/mprammer/vortex-go/internal/encoding"
	"github.com/mprammer/vortex-go/internal/wire/fb"
)

var (
	ErrInvalid     = errors.New("invalid Vortex file")
	ErrUnsupported = errors.New("unsupported Vortex feature")
)

const (
	maxMetadata = 64 << 20
	maxSegment  = 256 << 20
	maxRead     = 64 << 10
	maxDepth    = 64
	maxNodes    = 1 << 20
)

type segment struct {
	offset int64
	length int
}
type layout struct {
	id       string
	rows     int64
	metadata []byte
	children []*layout
	segments []uint32
}

// File holds immutable file metadata. It does not own or close its ReaderAt.
// Concurrent record readers are supported when the source supports concurrent reads.
// Individual metadata and data segments are limited to 64 MiB and 256 MiB,
// respectively. Every source read is at most 64 KiB.
type File struct {
	source   io.ReaderAt
	size     int64
	schema   *arrow.Schema
	dtype    encoding.Type
	root     *layout
	segments []segment
	arrays   []string
	layouts  []string
}

// Open reads the file header and footer metadata, without decoding data arrays.
func Open(ctx context.Context, source io.ReaderAt, size int64) (file *File, err error) {
	defer recoverParse(&err)
	if source == nil || size < 12 {
		return nil, fmt.Errorf("%w: missing header or footer", ErrInvalid)
	}
	f := &File{source: source, size: size}
	header, err := f.read(ctx, 0, 4, maxMetadata)
	if err != nil {
		return nil, err
	}
	if string(header) != "VTXF" {
		return nil, fmt.Errorf("%w: header magic", ErrInvalid)
	}
	eof, err := f.read(ctx, size-8, 8, maxMetadata)
	if err != nil {
		return nil, err
	}
	if string(eof[4:]) != "VTXF" {
		return nil, fmt.Errorf("%w: footer magic", ErrInvalid)
	}
	if v := binary.LittleEndian.Uint16(eof); v != 1 {
		return nil, fmt.Errorf("%w: file version %d", ErrUnsupported, v)
	}
	psLen := int(binary.LittleEndian.Uint16(eof[2:]))
	if psLen == 0 || psLen > 65527 {
		return nil, fmt.Errorf("%w: postscript length", ErrInvalid)
	}
	psStart := size - 8 - int64(psLen)
	psBytes, err := f.read(ctx, psStart, psLen, maxMetadata)
	if err != nil {
		return nil, err
	}
	if err = checkRoot(psBytes); err != nil {
		return nil, err
	}
	ps := fb.GetRootAsPostscript(psBytes, 0)
	get := func(s *fb.PostscriptSegment) ([]byte, error) {
		if s == nil {
			return nil, fmt.Errorf("%w: required postscript segment missing", ErrInvalid)
		}
		if s.HasTransforms() {
			return nil, fmt.Errorf("%w: compressed/encrypted footer", ErrUnsupported)
		}
		if s.Offset() > uint64(psStart) || uint64(s.Length()) > uint64(psStart)-s.Offset() {
			return nil, fmt.Errorf("%w: footer segment range", ErrInvalid)
		}
		if s.AlignmentExponent() > 30 {
			return nil, fmt.Errorf("%w: footer segment alignment", ErrInvalid)
		}
		return f.read(ctx, int64(s.Offset()), int(s.Length()), maxMetadata)
	}
	dtypeBytes, err := get(ps.Dtype(nil))
	if err != nil {
		return nil, err
	}
	footerBytes, err := get(ps.Footer(nil))
	if err != nil {
		return nil, err
	}
	layoutBytes, err := get(ps.Layout(nil))
	if err != nil {
		return nil, err
	}
	if err = f.parseRegistry(footerBytes); err != nil {
		return nil, err
	}
	if err = checkRoot(dtypeBytes); err != nil {
		return nil, err
	}
	budget := maxNodes
	f.dtype, err = parseType(fb.GetRootAsDType(dtypeBytes, 0), 0, &budget)
	if err != nil {
		return nil, err
	}
	if f.dtype.Kind != "struct" || f.dtype.Nullable {
		return nil, fmt.Errorf("%w: root must be a non-nullable struct", ErrUnsupported)
	}
	fields := make([]arrow.Field, len(f.dtype.Fields))
	seen := map[string]bool{}
	for i, v := range f.dtype.Fields {
		if seen[v.Name] {
			return nil, fmt.Errorf("%w: duplicate column %q", ErrInvalid, v.Name)
		}
		seen[v.Name] = true
		dt, e := v.Type.ArrowType()
		if e != nil {
			return nil, fmt.Errorf("%w: column %q: %v", ErrUnsupported, v.Name, e)
		}
		fields[i] = arrow.Field{Name: v.Name, Type: dt, Nullable: v.Type.Nullable}
	}
	f.schema = arrow.NewSchema(fields, nil)
	if err = checkRoot(layoutBytes); err != nil {
		return nil, err
	}
	budget = maxNodes
	f.root, err = f.parseLayout(fb.GetRootAsLayout(layoutBytes, 0), 0, &budget)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (f *File) Schema() *arrow.Schema { return f.schema }
func (f *File) NumRows() int64        { return f.root.rows }

// Close leaves the caller-owned source open. RecordReader.Release frees scan buffers.
func (f *File) Close() error { return nil }

func (f *File) read(ctx context.Context, offset int64, length, limit int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if offset < 0 || length < 0 || offset > f.size || int64(length) > f.size-offset {
		return nil, fmt.Errorf("%w: read range %d + %d outside %d", ErrInvalid, offset, length, f.size)
	}
	if length > limit {
		return nil, fmt.Errorf("%w: segment length %d exceeds limit %d", ErrUnsupported, length, limit)
	}
	out := make([]byte, length)
	for done := 0; done < length; {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end := min(done+maxRead, length)
		n, err := f.source.ReadAt(out[done:end], offset+int64(done))
		if n < 0 || n > end-done {
			return nil, fmt.Errorf("%w: invalid ReaderAt byte count", ErrInvalid)
		}
		if n != end-done {
			if err == nil {
				err = io.ErrUnexpectedEOF
			}
			return nil, fmt.Errorf("read at %d: %w", offset+int64(done), err)
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("read at %d: %w", offset+int64(done), err)
		}
		done = end
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func recoverParse(err *error) {
	if v := recover(); v != nil {
		*err = fmt.Errorf("%w: malformed metadata: %v", ErrInvalid, v)
	}
}
func checkRoot(b []byte) error {
	if len(b) < 8 || uint64(binary.LittleEndian.Uint32(b)) > uint64(len(b)-4) {
		return fmt.Errorf("%w: truncated FlatBuffer", ErrInvalid)
	}
	return nil
}
func checkCount(n int, b []byte, stride int) error {
	if n < 0 || n > maxNodes || n > len(b)/stride {
		return fmt.Errorf("%w: metadata vector length %d", ErrInvalid, n)
	}
	return nil
}
func nodeBudget(depth int, budget *int) error {
	*budget--
	if depth > maxDepth || *budget < 0 {
		return fmt.Errorf("%w: metadata nesting or node count", ErrInvalid)
	}
	return nil
}
func (f *File) parseRegistry(b []byte) error {
	if err := checkRoot(b); err != nil {
		return err
	}
	r := fb.GetRootAsFooter(b, 0)
	if err := checkCount(r.ArraySpecsLength(), b, 4); err != nil {
		return err
	}
	textBytes := 0
	for i := 0; i < r.ArraySpecsLength(); i++ {
		var v fb.ArraySpec
		r.ArraySpecs(&v, i)
		textBytes += len(v.Id())
		if textBytes > maxMetadata {
			return fmt.Errorf("%w: registry text budget", ErrInvalid)
		}
		f.arrays = append(f.arrays, string(v.Id()))
	}
	if err := checkCount(r.LayoutSpecsLength(), b, 4); err != nil {
		return err
	}
	for i := 0; i < r.LayoutSpecsLength(); i++ {
		var v fb.LayoutSpec
		r.LayoutSpecs(&v, i)
		textBytes += len(v.Id())
		if textBytes > maxMetadata {
			return fmt.Errorf("%w: registry text budget", ErrInvalid)
		}
		f.layouts = append(f.layouts, string(v.Id()))
	}
	if err := checkCount(r.SegmentSpecsLength(), b, 16); err != nil {
		return err
	}
	for i := 0; i < r.SegmentSpecsLength(); i++ {
		var v fb.SegmentSpec
		r.SegmentSpecs(&v, i)
		if v.Offset() > uint64(f.size) || uint64(v.Length()) > uint64(f.size)-v.Offset() {
			return fmt.Errorf("%w: segment %d range", ErrInvalid, i)
		}
		if v.AlignmentExponent() > 30 {
			return fmt.Errorf("%w: segment %d alignment", ErrInvalid, i)
		}
		if v.HasTransforms() {
			return fmt.Errorf("%w: segment compression/encryption", ErrUnsupported)
		}
		f.segments = append(f.segments, segment{int64(v.Offset()), int(v.Length())})
	}
	return nil
}
func (f *File) parseLayout(v *fb.Layout, depth int, budget *int) (*layout, error) {
	if v == nil {
		return nil, fmt.Errorf("%w: missing layout", ErrInvalid)
	}
	if err := nodeBudget(depth, budget); err != nil {
		return nil, err
	}
	if int(v.Encoding()) >= len(f.layouts) || v.RowCount() > math.MaxInt64 {
		return nil, fmt.Errorf("%w: layout encoding or rows", ErrInvalid)
	}
	b := v.Table().Bytes
	if err := checkCount(v.ChildrenLength(), b, 4); err != nil {
		return nil, err
	}
	if err := checkCount(v.SegmentsLength(), b, 4); err != nil {
		return nil, err
	}
	out := &layout{id: f.layouts[v.Encoding()], rows: int64(v.RowCount()), metadata: v.MetadataBytes()}
	for i := 0; i < v.ChildrenLength(); i++ {
		var c fb.Layout
		v.Children(&c, i)
		parsed, err := f.parseLayout(&c, depth+1, budget)
		if err != nil {
			return nil, err
		}
		out.children = append(out.children, parsed)
	}
	for i := 0; i < v.SegmentsLength(); i++ {
		idx := v.Segments(i)
		if uint64(idx) >= uint64(len(f.segments)) {
			return nil, fmt.Errorf("%w: unknown segment %d", ErrInvalid, idx)
		}
		out.segments = append(out.segments, idx)
	}
	return out, nil
}

// Metadata tables may share long strings. Charge copied text to the same
// bounded parser budget rather than multiplying shared input into allocations.
func copyText(b []byte, budget *int) (string, error) {
	cost := (len(b) + 63) / 64
	if cost > *budget {
		return "", fmt.Errorf("%w: metadata text budget", ErrInvalid)
	}
	*budget -= cost
	return string(b), nil
}
