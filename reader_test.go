// SPDX-License-Identifier: Apache-2.0

package vortex

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func openFixture(t *testing.T, name string) (*File, *os.File) {
	t.Helper()
	src, err := os.Open(filepath.Join("testdata/rust", name+".vortex"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { src.Close() })
	stat, err := src.Stat()
	if err != nil {
		t.Fatal(err)
	}
	f, err := Open(context.Background(), src, stat.Size())
	if err != nil {
		t.Fatal(err)
	}
	return f, src
}
func TestRustFixtures(t *testing.T) {
	paths, err := filepath.Glob("testdata/rust/*.vortex")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("Rust fixture corpus is empty")
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			name := filepath.Base(path)
			name = name[:len(name)-len(".vortex")]
			f, _ := openFixture(t, name)
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			ctx := compute.WithAllocator(context.Background(), mem)
			rr, err := f.NewRecordReader(ctx, ScanOptions{BatchSize: 211})
			if err != nil {
				t.Fatal(err)
			}
			defer rr.Release()
			rows := int64(0)
			for rr.Next() {
				if rr.RowOffset() != rows {
					t.Fatalf("physical row offset %d expected %d", rr.RowOffset(), rows)
				}
				rows += rr.RecordBatch().NumRows()
			}
			if rr.Err() != nil {
				t.Fatal(rr.Err())
			}
			if rows != f.NumRows() {
				t.Fatalf("rows %d expected %d", rows, f.NumRows())
			}
		})
	}
}
func TestPairedParquetValues(t *testing.T) {
	paths, err := filepath.Glob("testdata/rust/*.parquet")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("paired Parquet corpus is empty")
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			name := filepath.Base(path)
			name = name[:len(name)-len(".parquet")]
			f, _ := openFixture(t, name)
			src, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer src.Close()
			want, err := pqarrow.ReadTable(context.Background(), src, parquet.NewReaderProperties(memory.DefaultAllocator), pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
			if err != nil {
				t.Fatal(err)
			}
			defer want.Release()
			if f.Schema().NumFields() != want.Schema().NumFields() {
				t.Fatalf("column count mismatch: Vortex %d; Parquet %d", f.Schema().NumFields(), want.Schema().NumFields())
			}
			// Parquet readers add format-specific field-ID metadata. Compare the
			// logical schema: field order, name, type, and nullability.
			for i, got := range f.Schema().Fields() {
				expected := want.Schema().Field(i)
				if got.Name != expected.Name || got.Nullable != expected.Nullable || !arrow.TypeEqual(got.Type, expected.Type) {
					t.Fatalf("column %d schema mismatch: Vortex %s; Parquet %s", i, got, expected)
				}
			}
			if f.NumRows() != want.NumRows() {
				t.Fatalf("row count mismatch: Vortex %d; Parquet %d", f.NumRows(), want.NumRows())
			}
			rr, err := f.NewRecordReader(context.Background(), ScanOptions{BatchSize: 127})
			if err != nil {
				t.Fatal(err)
			}
			defer rr.Release()
			chunks := make([][]arrow.Array, len(f.Schema().Fields()))
			defer func() {
				for _, col := range chunks {
					for _, a := range col {
						a.Release()
					}
				}
			}()
			var rows int64
			for rr.Next() {
				rows += rr.RecordBatch().NumRows()
				for i, a := range rr.RecordBatch().Columns() {
					a.Retain()
					chunks[i] = append(chunks[i], a)
				}
			}
			if rr.Err() != nil {
				t.Fatal(rr.Err())
			}
			if rows != want.NumRows() {
				t.Fatalf("read %d rows; expected %d", rows, want.NumRows())
			}
			for i, col := range chunks {
				got, err := array.Concatenate(col, memory.DefaultAllocator)
				if err != nil {
					t.Fatal(err)
				}
				defer got.Release()
				expected, err := array.Concatenate(want.Column(i).Data().Chunks(), memory.DefaultAllocator)
				if err != nil {
					t.Fatal(err)
				}
				defer expected.Release()
				if !array.Equal(got, expected) {
					t.Fatalf("column %s differs: got %s expected %s", f.Schema().Field(i).Name, got.DataType(), expected.DataType())
				}
			}
		})
	}
}

type trackedSource struct {
	src                   io.ReaderAt
	reads, bytes, largest int
	fail                  bool
}

func (s *trackedSource) ReadAt(p []byte, off int64) (int, error) {
	s.reads++
	s.bytes += len(p)
	s.largest = max(s.largest, len(p))
	if s.fail {
		return 0, io.ErrClosedPipe
	}
	return s.src.ReadAt(p, off)
}
func TestReaderLifecycleAndProjection(t *testing.T) {
	_, src := openFixture(t, "default_chunked")
	stat, _ := src.Stat()
	tracked := &trackedSource{src: src}
	f, err := Open(context.Background(), tracked, stat.Size())
	if err != nil {
		t.Fatal(err)
	}
	before := tracked.reads
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx, cancel := context.WithCancel(compute.WithAllocator(context.Background(), mem))
	defer cancel()
	name := f.Schema().Field(0).Name
	rr, err := f.NewRecordReader(ctx, ScanOptions{Columns: []string{name}, BatchSize: 19})
	if err != nil {
		t.Fatal(err)
	}
	defer rr.Release()
	if tracked.reads != before {
		t.Fatal("reader construction read data")
	}
	if !rr.Next() {
		t.Fatal(rr.Err())
	}
	if rr.RecordBatch().NumCols() != 1 {
		t.Fatal("projection")
	}
	retained := rr.RecordBatch()
	retained.Retain()
	defer retained.Release()
	cancel()
	if rr.Next() || !errors.Is(rr.Err(), context.Canceled) {
		t.Fatalf("cancellation: %v", rr.Err())
	}
	if retained.NumRows() != 19 {
		t.Fatal("retained batch invalidated")
	}
	if tracked.largest > maxRead {
		t.Fatalf("unbounded read %d", tracked.largest)
	}
	if _, err = f.NewRecordReader(context.Background(), ScanOptions{Columns: []string{"missing"}}); err == nil {
		t.Fatal("unknown projection accepted")
	}
	zero, err := f.NewRecordReader(context.Background(), ScanOptions{Columns: []string{}, BatchSize: 113})
	if err != nil {
		t.Fatal(err)
	}
	defer zero.Release()
	before = tracked.reads
	rows := int64(0)
	for zero.Next() {
		rows += zero.RecordBatch().NumRows()
		if zero.RecordBatch().NumCols() != 0 {
			t.Fatal("zero column projection")
		}
	}
	if zero.Err() != nil || rows != f.NumRows() || tracked.reads != before {
		t.Fatalf("zero columns: %d %v reads=%d before=%d", rows, zero.Err(), tracked.reads, before)
	}
}
