// SPDX-License-Identifier: Apache-2.0

package vortex

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"testing"

	"github.com/mprammer/vortex-go/internal/wire/fb"
)

func TestMalformedFooterReturnsErrors(t *testing.T) {
	raw, err := os.ReadFile("testdata/rust/owned_scalars.vortex")
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{0, 1, 4, 7, 8, 11, len(raw) - 1} {
		if _, err := Open(context.Background(), bytes.NewReader(raw[:n]), int64(n)); err == nil {
			t.Fatalf("accepted truncated length %d", n)
		}
	}
	for _, change := range []func([]byte){
		func(b []byte) { b[0] = 'x' },
		func(b []byte) { b[len(b)-1] = 'x' },
		func(b []byte) { binary.LittleEndian.PutUint16(b[len(b)-8:], 999) },
		func(b []byte) { binary.LittleEndian.PutUint16(b[len(b)-6:], 65535) },
		func(b []byte) { ps := postscriptBytes(b); binary.LittleEndian.PutUint32(ps, math.MaxUint32) },
		func(b []byte) {
			ps := fb.GetRootAsPostscript(postscriptBytes(b), 0)
			ps.Footer(nil).MutateOffset(math.MaxUint64)
		},
		func(b []byte) {
			ps := fb.GetRootAsPostscript(postscriptBytes(b), 0)
			ps.Dtype(nil).MutateLength(math.MaxUint32)
		},
	} {
		data := append([]byte(nil), raw...)
		change(data)
		if _, err := Open(context.Background(), bytes.NewReader(data), int64(len(data))); err == nil {
			t.Fatal("accepted malformed footer")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Open(ctx, bytes.NewReader(raw), int64(len(raw))); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}
func postscriptBytes(b []byte) []byte {
	end := len(b) - 8
	n := int(binary.LittleEndian.Uint16(b[end+2:]))
	return b[end-n : end]
}
func TestReaderAtErrorPropagation(t *testing.T) {
	_, src := openFixture(t, "default_chunked")
	info, _ := src.Stat()
	tracked := &trackedSource{src: src}
	f, err := Open(context.Background(), tracked, info.Size())
	if err != nil {
		t.Fatal(err)
	}
	rr, err := f.NewRecordReader(context.Background(), ScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer rr.Release()
	tracked.fail = true
	if rr.Next() || !errors.Is(rr.Err(), io.ErrClosedPipe) {
		t.Fatalf("read error: %v", rr.Err())
	}
}
func TestCancellationAfterLastBatch(t *testing.T) {
	f, _ := openFixture(t, "owned_scalars")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rr, err := f.NewRecordReader(ctx, ScanOptions{BatchSize: int(f.NumRows())})
	if err != nil {
		t.Fatal(err)
	}
	defer rr.Release()
	var rows int64
	for rows < f.NumRows() {
		if !rr.Next() {
			t.Fatal(rr.Err())
		}
		rows += rr.RecordBatch().NumRows()
	}
	cancel()
	if rr.Next() || !errors.Is(rr.Err(), context.Canceled) {
		t.Fatalf("final Next lost cancellation: %v", rr.Err())
	}
}
func FuzzOpen(f *testing.F) {
	f.Add([]byte("VTXF\x01\x00\x00\x00VTXF"))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		file, err := Open(context.Background(), bytes.NewReader(data), int64(len(data)))
		if err == nil {
			file.Close()
		}
	})
}
