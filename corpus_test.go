// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Source-value formulas were developed for Iceberg's Rust reference corpus.
// They are independent of the compressed file and of either Go reader.
package vortex_test

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	vortex "github.com/mprammer/vortex-go"
)

func corpusFile(t *testing.T, name string) *vortex.File {
	t.Helper()
	f, e := os.Open(filepath.Join("testdata/rust", name))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { f.Close() })
	s, e := f.Stat()
	if e != nil {
		t.Fatal(e)
	}
	vf, e := vortex.Open(t.Context(), f, s.Size())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { vf.Close() })
	return vf
}
func TestDefaultWriterOriginalValues(t *testing.T) {
	fields := []string{"row_id", "flag", "v32", "v64", "f32", "f64", "text", "blob"}
	for fixture, name := range []string{"repeated", "progression", "broad", "chunked", "precision"} {
		t.Run(name, func(t *testing.T) {
			f := corpusFile(t, "default_"+name+".vortex")
			if f.NumRows() != 8193 {
				t.Fatal(f.NumRows())
			}
			projections := [][]string{nil}
			for _, field := range fields {
				projections = append(projections, []string{field})
			}
			for _, projection := range projections {
				t.Run(fmt.Sprint(projection), func(t *testing.T) {
					r, e := f.NewRecordReader(t.Context(), vortex.ScanOptions{Columns: projection, BatchSize: 257})
					if e != nil {
						t.Fatal(e)
					}
					defer r.Release()
					rows := 0
					for r.Next() {
						batch := r.RecordBatch()
						if r.RowOffset() != int64(rows) {
							t.Fatalf("offset %d rows %d", r.RowOffset(), rows)
						}
						for col, field := range batch.Schema().Fields() {
							index := -1
							for i, name := range fields {
								if name == field.Name {
									index = i
								}
							}
							if index < 0 {
								t.Fatal(field.Name)
							}
							a := batch.Column(col)
							for j := 0; j < a.Len(); j++ {
								want, valid := defaultWriterOriginal(fixture, index, rows+j)
								if valid != a.IsValid(j) {
									t.Fatalf("%s row %d validity", field.Name, rows+j)
								}
								if !valid {
									continue
								}
								var got any
								switch a := a.(type) {
								case *array.Int64:
									got = a.Value(j)
								case *array.Int32:
									got = a.Value(j)
								case *array.Boolean:
									got = a.Value(j)
								case *array.Float32:
									got = math.Float32bits(a.Value(j))
								case *array.Float64:
									got = math.Float64bits(a.Value(j))
								case *array.String:
									got = a.Value(j)
								case *array.Binary:
									got = a.Value(j)
								default:
									t.Fatalf("unexpected type %T", a)
								}
								equal := reflect.DeepEqual(got, want)
								if a, ok := got.([]byte); ok {
									equal = bytes.Equal(a, want.([]byte))
								}
								if !equal {
									t.Fatalf("%s row %d: got %v want %v", field.Name, rows+j, got, want)
								}
							}
						}
						rows += int(batch.NumRows())
					}
					if r.Err() != nil {
						t.Fatal(r.Err())
					}
					if rows != 8193 {
						t.Fatal(rows)
					}
				})
			}
		})
	}
}
func TestDateDecimalOriginalValues(t *testing.T) {
	f := corpusFile(t, "date_decimal.vortex")
	r, e := f.NewRecordReader(t.Context(), vortex.ScanOptions{BatchSize: 3})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Release()
	days := []int32{-100000, -1, 0, 1, 20000, 0, math.MaxInt32, math.MinInt32}
	amounts := []int64{-999999999999999, -1, 0, 1, 999999999999999, 0, 123456789012345, -123456789012345}
	row := 0
	for r.Next() {
		b := r.RecordBatch()
		d := b.Column(1).(*array.Date32)
		a := b.Column(2).(*array.Decimal128)
		for j := 0; j < d.Len(); j++ {
			if row >= 8 {
				t.Fatal("too many rows")
			}
			if d.IsNull(j) != (row == 5) || a.IsNull(j) != (row == 5) {
				t.Fatal("null mismatch", row)
			}
			if row != 5 && (int32(d.Value(j)) != days[row] || a.Value(j) != decimal128.FromI64(amounts[row])) {
				t.Fatal("value mismatch", row)
			}
			row++
		}
	}
	if r.Err() != nil {
		t.Fatal(r.Err())
	}
	if row != 8 {
		t.Fatal(row)
	}
}
func defaultWriterHash(row int) uint64 {
	x := uint64(row) + 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb

	return x ^ (x >> 31)
}

// Source formulas are independent of both readers and of the compressed file.
// Float expectations are IEEE bits, so NaN payloads and signed zeros are checked.
func defaultWriterOriginal(fixture, col, row int) (any, bool) {
	valid := col == 0 || ((fixture != 3 || row < 3072 || row >= 4096) && (row+col*3)%29 != 0)
	if col == 0 {
		return int64(row), true
	}
	mode := fixture
	if fixture == 3 {
		mode = (row / 2048) % 3
	}
	h := defaultWriterHash(row)
	switch col {
	case 1:
		switch mode {
		case 0:
			return true, valid
		case 1, 4:
			return (row/127)%2 == 0, valid
		default:
			return h&1 != 0, valid
		}
	case 2:
		switch mode {
		case 0:
			return int32(-77), valid
		case 1:
			return int32(row - 4096), valid
		case 4:
			switch row % 1009 {
			case 0:
				return int32(math.MinInt32), valid
			case 1:
				return int32(math.MaxInt32), valid
			default:
				return int32(1000 + row%17), valid
			}
		default:
			switch row % 257 {
			case 0:
				return int32(math.MinInt32), valid
			case 1:
				return int32(math.MaxInt32), valid
			default:
				return int32(h), valid
			}
		}
	case 3:
		switch mode {
		case 0:
			return int64(1<<53 + 1), valid
		case 1:
			return int64(math.MinInt64) + int64(row)*17, valid
		case 4:
			switch row % 1009 {
			case 0:
				return int64(math.MinInt64), valid
			case 1:
				return int64(math.MaxInt64), valid
			default:
				return int64(1<<45) + int64(row%31), valid
			}
		default:
			switch row % 257 {
			case 0:
				return int64(math.MinInt64), valid
			case 1:
				return int64(math.MaxInt64), valid
			default:
				return int64(h), valid
			}
		}
	case 4:
		switch mode {
		case 0:
			return math.Float32bits(1.25), valid
		case 1:
			return math.Float32bits(float32(row%2001-1000) / 10), valid
		default:
			special := [...]uint32{0x80000000, 0, 0x7fc01234, 0xffc05678, 0x7f800000, 0xff800000, 1, 0x80000001, 0x7f7fffff, 0x00800000}
			period := 257
			if mode == 4 {
				period = 1021
			}
			if row%period < len(special) {
				return special[row%period], valid
			}
			if mode == 4 {
				return uint32(0x3f800000) | uint32(h)&0x007fffff, valid
			}

			return uint32(h) & 0xff7fffff, valid
		}
	case 5:
		switch mode {
		case 0:
			return math.Float64bits(-3.5), valid
		case 1:
			return math.Float64bits(float64(row%2001-1000) / 10), valid
		default:
			special := [...]uint64{0x8000000000000000, 0, 0x7ff8000000001234, 0xfff8000000005678, 0x7ff0000000000000, 0xfff0000000000000, 1, 0x8000000000000001, 0x7fefffffffffffff, 0x0010000000000000}
			period := 257
			if mode == 4 {
				period = 1021
			}
			if row%period < len(special) {
				return special[row%period], valid
			}
			if mode == 4 {
				return uint64(0x3ff0000000000000) | h&0x000fffffffffffff, valid
			}

			return h & 0xffefffffffffffff, valid
		}
	case 6:
		switch mode {
		case 0:
			return "constant string longer than twelve bytes", valid
		case 1, 4:
			return [...]string{"", "a", "iceberg", "vortex dictionary entry", "東京", "éclair", "🙂", "a\x00b"}[row%8], valid
		default:
			switch row % 4 {
			case 0:
				return "", valid
			case 1:
				return fmt.Sprintf("s%d", row), valid
			case 2:
				return fmt.Sprintf("row=%05d;hash=%016x;東京;", row, h), valid
			default:
				return fmt.Sprintf("%s:%d:%016x", strings.Repeat("shared long prefix/", 12), row, h), valid
			}
		}
	case 7:
		switch mode {
		case 0:
			return []byte{0, 255, 128, 1, 0, 7}, valid
		case 1, 4:
			return []byte{0, byte(row % 7), 255}, valid
		default:
			b := make([]byte, [...]int{0, 3, 19, 131}[row%4])
			for i := range b {
				b[i] = byte(h>>((i%8)*8)) + byte(i)
			}

			return b, valid
		}
	default:
		panic("unexpected source column")
	}
}

// These source values are authored independently of the compressed files and
// checked with full, single-column, and reordered projections at batch boundaries.
func TestOwnedScalarAndNullableSourceValues(t *testing.T) {
	cases := []struct {
		name   string
		rows   int
		fields []arrow.Field
	}{
		{"owned_scalars", 6149, []arrow.Field{
			{Name: "sequence", Type: arrow.PrimitiveTypes.Int64},
			{Name: "small", Type: arrow.PrimitiveTypes.Int32},
			{Name: "wide", Type: arrow.PrimitiveTypes.Int64},
			{Name: "measure", Type: arrow.PrimitiveTypes.Float64},
			{Name: "enabled", Type: arrow.FixedWidthTypes.Boolean},
			{Name: "label", Type: arrow.BinaryTypes.String},
			{Name: "note", Type: arrow.BinaryTypes.String},
		}},
		{"owned_nullable", 2057, []arrow.Field{
			{Name: "maybe_int", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "maybe_str", Type: arrow.BinaryTypes.String, Nullable: true},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := corpusFile(t, tc.name+".vortex")
			if f.NumRows() != int64(tc.rows) || !f.Schema().Equal(arrow.NewSchema(tc.fields, nil)) {
				t.Fatalf("unexpected fixture shape: %d rows, %s", f.NumRows(), f.Schema())
			}
			projections := [][]string{nil, {tc.fields[len(tc.fields)-1].Name, tc.fields[0].Name}}
			for _, field := range tc.fields {
				projections = append(projections, []string{field.Name})
			}
			for _, batchSize := range []int{127, 1024} {
				for _, projection := range projections {
					t.Run(fmt.Sprintf("batch=%d/columns=%v", batchSize, projection), func(t *testing.T) {
						rr, err := f.NewRecordReader(t.Context(), vortex.ScanOptions{Columns: projection, BatchSize: batchSize})
						if err != nil {
							t.Fatal(err)
						}
						defer rr.Release()
						expectedFields := tc.fields
						if projection != nil {
							expectedFields = nil
							for _, name := range projection {
								for _, field := range tc.fields {
									if field.Name == name {
										expectedFields = append(expectedFields, field)
									}
								}
							}
						}
						if !rr.Schema().Equal(arrow.NewSchema(expectedFields, nil)) {
							t.Fatalf("unexpected projection schema: %s", rr.Schema())
						}
						row := 0
						for rr.Next() {
							b := rr.RecordBatch()
							if rr.RowOffset() != int64(row) || b.NumRows() > int64(batchSize) {
								t.Fatalf("batch at %d has offset %d and %d rows", row, rr.RowOffset(), b.NumRows())
							}
							for col, field := range b.Schema().Fields() {
								a := b.Column(col)
								for j := range a.Len() {
									want, valid := ownedSourceValue(field.Name, row+j)
									if a.IsValid(j) != valid {
										t.Fatalf("%s row %d validity", field.Name, row+j)
									}
									if !valid {
										continue
									}
									var got any
									switch a := a.(type) {
									case *array.Int64:
										got = a.Value(j)
									case *array.Int32:
										got = a.Value(j)
									case *array.Float64:
										got = math.Float64bits(a.Value(j))
									case *array.Boolean:
										got = a.Value(j)
									case *array.String:
										got = a.Value(j)
									default:
										t.Fatalf("unexpected type %T", a)
									}
									if !reflect.DeepEqual(got, want) {
										t.Fatalf("%s row %d: got %v want %v", field.Name, row+j, got, want)
									}
								}
							}
							row += int(b.NumRows())
						}
						if rr.Err() != nil {
							t.Fatal(rr.Err())
						}
						if row != tc.rows {
							t.Fatalf("read %d rows, want %d", row, tc.rows)
						}
					})
				}
			}
		})
	}
}

func ownedSourceValue(field string, row int) (any, bool) {
	switch field {
	case "sequence":
		return int64(row)*29 - 80000, true
	case "small":
		return int32((row*37)%10007 - 5003), true
	case "wide":
		return int64(9007199254740993) + int64(row)*101, true
	case "measure":
		return math.Float64bits(float64(row%1801-900) / 8), true
	case "enabled":
		return row%7 < 3, true
	case "label":
		return [...]string{"", "cedar", "fjord", "東京駅", "naïve", "🪁", "a\x00z", "independent strings exceed twelve bytes"}[row%8], true
	case "note":
		return fmt.Sprintf("reading/%05d/sector-%d", row, row%23), true
	case "maybe_int":
		return int32(row*19 - 7000), row%13 != 4 && (row < 1000 || row >= 1017)
	case "maybe_str":
		return fmt.Sprintf("item-%04d-δ", row), row%17 != 9
	default:
		panic("unexpected owned source column: " + field)
	}
}
