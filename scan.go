// SPDX-License-Identifier: Apache-2.0

package vortex

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/mprammer/vortex-go/internal/encoding"
)

// CompareOp identifies an integer comparison used for conservative pruning.
type CompareOp uint8

const (
	OpEQ CompareOp = iota
	OpNEQ
	OpGT
	OpGTE
	OpLT
	OpLTE
)

// Predicate excludes only row ranges whose statistics prove the comparison false.
// Consumers must still apply the exact predicate to the returned rows.
type Predicate struct {
	Column string
	Op     CompareOp
	Value  int64
}

// ScanOptions configures a scan. Predicates are ANDed. A nil Columns selects all
// columns; an empty non-nil slice selects none. BatchSize defaults to 65536.
type ScanOptions struct {
	Columns    []string
	BatchSize  int
	Predicates []Predicate
}

type piece struct {
	start, end int64
	node       *layout
	dtype      encoding.Type
	field      int // -1 for a scalar, otherwise a field of a flat struct array
	dictionary *layout
	dictType   encoding.Type
}
type columnPlan struct {
	pieces []*piece
	zones  []*zoneRef
}
type columnCursor struct {
	plan       *columnPlan
	index      int
	cached     *piece
	values     arrow.Array
	dictionary *layout
	dictValues arrow.Array
}

func (c *columnCursor) release() {
	if c.values != nil {
		c.values.Release()
		c.values = nil
	}
	if c.dictValues != nil {
		c.dictValues.Release()
		c.dictValues = nil
	}
	c.cached = nil
	c.dictionary = nil
}
func (c *columnCursor) at(pos int64) (*piece, error) {
	for c.index < len(c.plan.pieces) && c.plan.pieces[c.index].end <= pos {
		c.index++
	}
	if c.index >= len(c.plan.pieces) || c.plan.pieces[c.index].start > pos {
		return nil, fmt.Errorf("%w: column layout has a row gap at %d", ErrInvalid, pos)
	}
	return c.plan.pieces[c.index], nil
}

// RecordReader streams projected batches in physical row order. Next and accessors
// must not run concurrently. Retain and Release follow Arrow ownership semantics.
// The current batch remains valid until Next or Release unless the caller retains it.
type RecordReader struct {
	refs       atomic.Int64
	file       *File
	ctx        context.Context
	schema     *arrow.Schema
	batchSize  int64
	rows       int64
	pos        int64
	offset     int64
	current    arrow.RecordBatch
	err        error
	columns    []*columnCursor
	predicates []scanPredicate
}
type scanPredicate struct {
	predicate Predicate
	plan      *columnPlan
}

var _ array.RecordReader = (*RecordReader)(nil)

func (f *File) NewRecordReader(ctx context.Context, opts ScanOptions) (*RecordReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opts.BatchSize < 0 {
		return nil, fmt.Errorf("negative batch size")
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 65536
	}
	names := opts.Columns
	if names == nil {
		names = make([]string, f.schema.NumFields())
		for i, v := range f.schema.Fields() {
			names[i] = v.Name
		}
	}
	plans := map[string]*columnPlan{}
	getPlan := func(name string) (*columnPlan, int, error) {
		ids := f.schema.FieldIndices(name)
		if len(ids) != 1 {
			return nil, 0, fmt.Errorf("unknown or ambiguous Vortex column %q", name)
		}
		if plan, ok := plans[name]; ok {
			return plan, ids[0], nil
		}
		plan := &columnPlan{}
		if err := buildFieldPlan(f.root, f.dtype, ids[0], 0, plan); err != nil {
			return nil, 0, fmt.Errorf("column %q: %w", name, err)
		}
		plans[name] = plan
		return plan, ids[0], nil
	}
	rr := &RecordReader{file: f, ctx: ctx, batchSize: int64(opts.BatchSize), rows: f.NumRows()}
	rr.refs.Store(1)
	fields := make([]arrow.Field, len(names))
	for i, name := range names {
		plan, idx, err := getPlan(name)
		if err != nil {
			return nil, err
		}
		fields[i] = f.schema.Field(idx)
		rr.columns = append(rr.columns, &columnCursor{plan: plan})
	}
	rr.schema = arrow.NewSchema(fields, nil)
	for _, pred := range opts.Predicates {
		if pred.Op > OpLTE {
			return nil, fmt.Errorf("invalid comparison %d", pred.Op)
		}
		plan, idx, err := getPlan(pred.Column)
		if err != nil {
			return nil, err
		}
		typ := f.dtype.Fields[idx].Type
		if typ.Kind != "i8" && typ.Kind != "i16" && typ.Kind != "i32" && typ.Kind != "i64" {
			continue
		}
		rr.predicates = append(rr.predicates, scanPredicate{pred, plan})
	}
	return rr, nil
}
func (r *RecordReader) Schema() *arrow.Schema          { return r.schema }
func (r *RecordReader) RecordBatch() arrow.RecordBatch { return r.current }

// Record is the deprecated Arrow compatibility accessor; use RecordBatch.
func (r *RecordReader) Record() arrow.RecordBatch { return r.current }
func (r *RecordReader) Err() error                { return r.err }
func (r *RecordReader) RowOffset() int64          { return r.offset }
func (r *RecordReader) Retain()                   { r.refs.Add(1) }
func (r *RecordReader) Release() {
	if r.refs.Add(-1) != 0 {
		return
	}
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	for _, c := range r.columns {
		c.release()
	}
	for _, p := range r.predicates {
		for _, z := range p.plan.zones {
			z.release()
		}
	}
	r.columns = nil
	r.predicates = nil
}
func (r *RecordReader) Next() (ok bool) {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if r.err != nil || r.refs.Load() <= 0 {
		return false
	}
	defer func() {
		if v := recover(); v != nil {
			r.err = fmt.Errorf("%w: scan: %v", ErrInvalid, v)
			ok = false
		}
		if !ok {
			for _, c := range r.columns {
				c.release()
			}
			for _, p := range r.predicates {
				for _, z := range p.plan.zones {
					z.release()
				}
			}
		}
	}()
	if r.err = r.ctx.Err(); r.err != nil {
		return false
	}
	for r.pos < r.rows {
		if r.err = r.ctx.Err(); r.err != nil {
			return false
		}
		end := r.pos + min(r.batchSize, r.rows-r.pos)
		skipUntil := r.pos
		for _, p := range r.predicates {
			for _, z := range p.plan.zones {
				if r.pos < z.start {
					end = min(end, z.start)
					continue
				}
				if r.pos >= z.end {
					continue
				}
				zoneEnd, skip, err := z.evaluate(r.ctx, r.file, r.pos, p.predicate)
				if err != nil {
					r.err = err
					return false
				}
				end = min(end, zoneEnd)
				if skip {
					skipUntil = max(skipUntil, zoneEnd)
				}
			}
		}
		if skipUntil > r.pos {
			r.pos = skipUntil
			continue
		}
		pieces := make([]*piece, len(r.columns))
		for i, c := range r.columns {
			p, err := c.at(r.pos)
			if err != nil {
				r.err = err
				return false
			}
			pieces[i] = p
			end = min(end, p.end)
		}
		if end <= r.pos {
			r.err = fmt.Errorf("%w: scan failed to advance", ErrInvalid)
			return false
		}
		cols := make([]arrow.Array, 0, len(r.columns))
		defer func() {
			for _, v := range cols {
				v.Release()
			}
		}()
		for i, c := range r.columns {
			a, err := c.read(r.ctx, r.file, pieces[i], r.pos, end)
			if err != nil {
				r.err = err
				return false
			}
			cols = append(cols, a)
		}
		if r.err = r.ctx.Err(); r.err != nil {
			return false
		}
		r.current = array.NewRecordBatch(r.schema, cols, end-r.pos)
		r.offset = r.pos
		r.pos = end
		return true
	}
	return false
}
func (c *columnCursor) read(ctx context.Context, f *File, p *piece, start, end int64) (arrow.Array, error) {
	if c.cached != p {
		if c.values != nil {
			c.values.Release()
			c.values = nil
		}
		a, err := f.decodeFlat(ctx, p.node, p.dtype)
		if err != nil {
			return nil, err
		}
		if p.field >= 0 {
			st, ok := a.(*array.Struct)
			if !ok || p.field >= st.NumField() {
				a.Release()
				return nil, fmt.Errorf("%w: expected struct array", ErrInvalid)
			}
			v := st.Field(p.field)
			v.Retain()
			a.Release()
			a = v
		}
		if p.dictionary == nil && c.dictValues != nil {
			c.dictValues.Release()
			c.dictValues = nil
			c.dictionary = nil
		}
		if p.dictionary != nil {
			if c.dictionary != p.dictionary {
				if c.dictValues != nil {
					c.dictValues.Release()
					c.dictValues = nil
				}
				values, e := f.decodeLayout(ctx, p.dictionary, p.dictType)
				if e != nil {
					a.Release()
					return nil, e
				}
				c.dictValues = values
				c.dictionary = p.dictionary
			}
			result, e := takeDictionary(ctx, c.dictValues, a, p.dictType)
			a.Release()
			if e != nil {
				return nil, e
			}
			a = result
		}
		c.values = a
		c.cached = p
	}
	return array.NewSlice(c.values, start-p.start, end-p.start), nil
}

func buildFieldPlan(l *layout, t encoding.Type, field int, start int64, out *columnPlan) error {
	switch l.id {
	case "vortex.struct":
		if t.Kind != "struct" || t.Nullable || len(l.children) != len(t.Fields) {
			return fmt.Errorf("%w: struct layout shape", ErrInvalid)
		}
		for _, c := range l.children {
			if c.rows != l.rows {
				return fmt.Errorf("%w: struct child rows", ErrInvalid)
			}
		}
		return buildScalarPlan(l.children[field], t.Fields[field].Type, start, out)
	case "vortex.chunked":
		offset := start
		for _, c := range l.children {
			if c.rows > l.rows-(offset-start) {
				return fmt.Errorf("%w: chunk rows exceed parent", ErrInvalid)
			}
			if err := buildFieldPlan(c, t, field, offset, out); err != nil {
				return err
			}
			offset += c.rows
		}
		if offset-start != l.rows {
			return fmt.Errorf("%w: chunk row sum", ErrInvalid)
		}
		return nil
	case "vortex.flat":
		out.pieces = append(out.pieces, &piece{start: start, end: start + l.rows, node: l, dtype: t, field: field})
		return nil
	default:
		return fmt.Errorf("%w: root layout %q", ErrUnsupported, l.id)
	}
}
func buildScalarPlan(l *layout, t encoding.Type, start int64, out *columnPlan) error {
	switch l.id {
	case "vortex.flat":
		out.pieces = append(out.pieces, &piece{start: start, end: start + l.rows, node: l, dtype: t, field: -1})
		return nil
	case "vortex.chunked":
		offset := start
		for _, c := range l.children {
			if c.rows > l.rows-(offset-start) {
				return fmt.Errorf("%w: chunk rows exceed parent", ErrInvalid)
			}
			if err := buildScalarPlan(c, t, offset, out); err != nil {
				return err
			}
			offset += c.rows
		}
		if offset-start != l.rows {
			return fmt.Errorf("%w: chunk row sum", ErrInvalid)
		}
		return nil
	case "vortex.zoned", "vortex.stats":
		if len(l.children) != 2 || l.children[0].rows != l.rows {
			return fmt.Errorf("%w: zoned layout shape", ErrInvalid)
		}
		out.zones = append(out.zones, newZoneRef(l, t, start))
		return buildScalarPlan(l.children[0], t, start, out)
	case "vortex.dict":
		if len(l.children) != 2 || l.children[1].rows != l.rows {
			return fmt.Errorf("%w: dictionary layout shape", ErrInvalid)
		}
		typ, err := dictionaryCodeType(l, t)
		if err != nil {
			return err
		}
		// Code statistics describe dictionary indices, not the logical values
		// compared by predicates. Keep only the code pieces in the value plan.
		codes := &columnPlan{}
		if err = buildScalarPlan(l.children[1], typ, start, codes); err != nil {
			return err
		}
		for _, p := range codes.pieces {
			if p.dictionary != nil {
				return fmt.Errorf("%w: nested dictionary layout", ErrUnsupported)
			}
			p.dictionary = l.children[0]
			p.dictType = t
		}
		out.pieces = append(out.pieces, codes.pieces...)
		return nil
	default:
		return fmt.Errorf("%w: layout %q", ErrUnsupported, l.id)
	}
}
func dictionaryCodeType(l *layout, t encoding.Type) (encoding.Type, error) {
	m, err := protoFields(l.metadata)
	if err != nil {
		return encoding.Type{}, err
	}
	id := uint64(0)
	if values := m[1]; len(values) > 0 {
		id = values[len(values)-1].number
	}
	nullable := t.Nullable
	if values := m[2]; len(values) > 0 {
		nullable = values[len(values)-1].number != 0
	}
	kinds := []encoding.Kind{"u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64"}
	if id >= uint64(len(kinds)) {
		return encoding.Type{}, fmt.Errorf("%w: dictionary code type", ErrInvalid)
	}
	return encoding.Type{Kind: kinds[id], Nullable: nullable}, nil
}

// decodeLayout is used only for dictionary values and optional zone-map tables.
// Data columns stream through columnCursor instead of concatenating whole files.
func (f *File) decodeLayout(ctx context.Context, l *layout, t encoding.Type) (out arrow.Array, err error) {
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if l.rows > 32<<20 {
		return nil, fmt.Errorf("%w: auxiliary array row limit", ErrUnsupported)
	}
	switch l.id {
	case "vortex.flat":
		return f.decodeFlat(ctx, l, t)
	case "vortex.zoned", "vortex.stats":
		if len(l.children) != 2 || l.children[0].rows != l.rows {
			return nil, fmt.Errorf("%w: zoned layout shape", ErrInvalid)
		}
		return f.decodeLayout(ctx, l.children[0], t)
	case "vortex.struct":
		if t.Kind != "struct" || t.Nullable || len(l.children) != len(t.Fields) {
			return nil, fmt.Errorf("%w: auxiliary struct shape", ErrInvalid)
		}
		cols := make([]arrow.Array, 0, len(t.Fields))
		defer func() {
			for _, v := range cols {
				v.Release()
			}
		}()
		fields := make([]arrow.Field, len(t.Fields))
		for i, field := range t.Fields {
			if l.children[i].rows != l.rows {
				return nil, fmt.Errorf("%w: auxiliary struct rows", ErrInvalid)
			}
			a, e := f.decodeLayout(ctx, l.children[i], field.Type)
			if e != nil {
				return nil, e
			}
			cols = append(cols, a)
			fields[i] = arrow.Field{Name: field.Name, Type: a.DataType(), Nullable: field.Type.Nullable}
		}
		return array.NewStructArrayWithFields(cols, fields)
	case "vortex.chunked":
		chunks := make([]arrow.Array, 0, len(l.children))
		defer func() {
			for _, v := range chunks {
				v.Release()
			}
		}()
		sum := int64(0)
		for _, child := range l.children {
			if child.rows > l.rows-sum {
				return nil, fmt.Errorf("%w: auxiliary chunk rows", ErrInvalid)
			}
			sum += child.rows
			a, e := f.decodeLayout(ctx, child, t)
			if e != nil {
				return nil, e
			}
			chunks = append(chunks, a)
		}
		if sum != l.rows {
			return nil, fmt.Errorf("%w: auxiliary chunk row sum", ErrInvalid)
		}
		if len(chunks) == 0 {
			dt, e := t.ArrowType()
			if e != nil {
				return nil, e
			}
			b := array.NewBuilder(compute.GetAllocator(ctx), dt)
			defer b.Release()
			return b.NewArray(), nil
		}
		return array.Concatenate(chunks, compute.GetAllocator(ctx))
	case "vortex.dict":
		if len(l.children) != 2 || l.children[1].rows != l.rows {
			return nil, fmt.Errorf("%w: auxiliary dictionary shape", ErrInvalid)
		}
		codesType, e := dictionaryCodeType(l, t)
		if e != nil {
			return nil, e
		}
		values, e := f.decodeLayout(ctx, l.children[0], t)
		if e != nil {
			return nil, e
		}
		defer values.Release()
		codes, e := f.decodeLayout(ctx, l.children[1], codesType)
		if e != nil {
			return nil, e
		}
		defer codes.Release()
		return takeDictionary(ctx, values, codes, t)
	default:
		return nil, fmt.Errorf("%w: auxiliary layout %q", ErrUnsupported, l.id)
	}
}

// Nullable codes can introduce nulls even when every dictionary value is valid.
func takeDictionary(ctx context.Context, values, codes arrow.Array, t encoding.Type) (arrow.Array, error) {
	a, err := compute.TakeArray(ctx, values, codes)
	if err != nil {
		return nil, fmt.Errorf("dictionary indices: %w", err)
	}
	if !t.Nullable && t.Kind != "null" && a.NullN() != 0 {
		a.Release()
		return nil, fmt.Errorf("%w: null in nonnullable dictionary %s", ErrInvalid, t.Kind)
	}
	return a, nil
}
