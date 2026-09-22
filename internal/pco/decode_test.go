// SPDX-License-Identifier: Apache-2.0
package pco

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
)

func TestRustModes(t *testing.T) {
	var corpus struct {
		Version string `json:"pco_version"`
		Cases   []struct {
			Name, Type, Header, Meta, Expected string
			Mode, Delta                        int
			Pages                              []struct {
				Data string
				N    uint32
			}
		}
	}
	path := os.Getenv("PCO_MODES_FIXTURE")
	if path == "" {
		path = "testdata/modes.json"
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &corpus); err != nil {
		t.Fatal(err)
	}
	if corpus.Version != "1.0.3" || len(corpus.Cases) != 59 {
		t.Fatalf("unexpected reference corpus: Pco %q, %d cases", corpus.Version, len(corpus.Cases))
	}
	seen := make(map[string]bool)
	for _, c := range corpus.Cases {
		if seen[c.Name] {
			t.Fatalf("duplicate reference case %q", c.Name)
		}
		seen[c.Name] = true
		t.Run(c.Name, func(t *testing.T) {
			decode := func(s string) []byte {
				b, e := hex.DecodeString(s)
				if e != nil {
					t.Fatal(e)
				}
				return b
			}
			var pages [][]byte
			var counts []uint32
			for _, p := range c.Pages {
				pages = append(pages, decode(p.Data))
				counts = append(counts, p.N)
			}
			header, metadata := decode(c.Header), decode(c.Meta)
			if !bytes.Equal(header, []byte{4, 1}) {
				t.Fatalf("unexpected Pco header %x", header)
			}
			width := map[string]int{"u16": 16, "i16": 16, "u32": 32, "i32": 32, "f32": 32, "u64": 64, "i64": 64, "f64": 64}[c.Type]
			if width == 0 {
				t.Fatalf("unexpected reference type %q", c.Type)
			}
			chunk, err := parseChunk(metadata, int(header[0]), width, c.Type)
			if err != nil {
				t.Fatal(err)
			}
			if chunk.mode != c.Mode || chunk.delta != c.Delta {
				t.Fatalf("physical modes: got (%d,%d), reference (%d,%d)", chunk.mode, chunk.delta, c.Mode, c.Delta)
			}
			for end := 0; end < len(header); end++ {
				if _, err := Decode(header[:end], [][]byte{metadata}, pages, [][]uint32{counts}, c.Type); err == nil {
					t.Fatalf("truncated header to %d accepted", end)
				}
			}
			for _, end := range []int{0, len(metadata) / 2, len(metadata) - 1} {
				if _, err := Decode(header, [][]byte{metadata[:end]}, pages, [][]uint32{counts}, c.Type); err == nil {
					t.Fatalf("truncated chunk metadata to %d accepted", end)
				}
			}
			got, e := Decode(decode(c.Header), [][]byte{decode(c.Meta)}, pages, [][]uint32{counts}, c.Type)
			if e != nil {
				t.Fatal(e)
			}
			want := decode(c.Expected)
			if !bytes.Equal(got, want) {
				for i := range min(len(got), len(want)) {
					if got[i] != want[i] {
						t.Fatalf("byte %d got %02x want %02x", i, got[i], want[i])
					}
				}
				t.Fatalf("length got %d want %d", len(got), len(want))
			}
			// A second identical chunk must restart mode/delta state and append
			// the complete independent oracle, including every page boundary.
			twice, err := Decode(header, [][]byte{metadata, metadata}, append(append([][]byte(nil), pages...), pages...), [][]uint32{counts, counts}, c.Type)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(twice, bytes.Repeat(want, 2)) {
				t.Fatal("second chunk did not reset and append exact values")
			}
			for i, p := range pages {
				for _, n := range []int{0, len(p) / 2, len(p) - 1} {
					if n < 0 {
						continue
					}
					short := append([][]byte(nil), pages...)
					short[i] = p[:n]
					if _, e := Decode(decode(c.Header), [][]byte{decode(c.Meta)}, short, [][]uint32{counts}, c.Type); e == nil {
						t.Fatalf("truncated page %d to %d accepted", i, n)
					}
				}
			}
		})
	}
}

func FuzzDecode(f *testing.F) {
	var corpus struct {
		Cases []struct {
			Type, Header, Meta string
			Pages              []struct {
				Data string
				N    uint32
			}
		}
	}
	data, e := os.ReadFile("testdata/modes.json")
	if e != nil {
		f.Fatal(e)
	}
	if e = json.Unmarshal(data, &corpus); e != nil {
		f.Fatal(e)
	}
	for _, c := range corpus.Cases {
		if len(c.Pages) == 0 {
			continue
		}
		h, _ := hex.DecodeString(c.Header)
		m, _ := hex.DecodeString(c.Meta)
		p, _ := hex.DecodeString(c.Pages[0].Data)
		f.Add(h, m, p, c.Pages[0].N, c.Type)
	}
	f.Fuzz(func(t *testing.T, h, m, p []byte, n uint32, kind string) {
		if len(h) > 16 || len(m) > 65536 || len(p) > 65536 {
			return
		}
		n %= 513
		_, _ = Decode(h, [][]byte{m}, [][]byte{p}, [][]uint32{{n}}, kind)
	})
}
