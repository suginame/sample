package compress

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompress(t *testing.T) {
	type payload struct {
		ID   int      `json:"id"`
		Name string   `json:"name"`
		Tags []string `json:"tags"`
	}

	// ローカルヘルパー：gzip解凍してJSON復元（CRCまで読み切る）
	decodeGzipJSON := func(t *testing.T, gz []byte, out any) {
		t.Helper()
		gr, err := gzip.NewReader(bytes.NewReader(gz))
		assert.Exactly(t, nil, err)
		defer gr.Close()
		assert.Exactly(t, nil, json.NewDecoder(gr).Decode(out))
		// CRC検証
		_, err = io.Copy(io.Discard, gr) //nolint:gosec // G110: test-only CRC verification with small, controlled input
		assert.Exactly(t, nil, err)
	}

	t.Run("nil data case", func(t *testing.T) {
		_, err := Compress(nil)
		assert.Exactly(t, true, err != nil)
		assert.Exactly(t, true, strings.Contains(err.Error(), "data cannot be nil"))
	})

	t.Run("unsupported type case", func(t *testing.T) {
		_, err := Compress(make(chan int))
		assert.Exactly(t, true, err != nil)
		assert.Exactly(t, true, strings.Contains(err.Error(), "failed to encode JSON (type:chan int)"))
	})

	t.Run("simple struct case", func(t *testing.T) {
		in := payload{ID: 1, Name: "alpha", Tags: []string{"x", "y"}}

		got, err := Compress(in)
		assert.Exactly(t, nil, err)
		assert.Exactly(t, true, len(got) > 0)
		assert.Exactly(t, true, bytes.HasPrefix(got, []byte{0x1f, 0x8b})) // gzip magic

		var out payload
		decodeGzipJSON(t, got, &out)
		assert.Exactly(t, in, out)
	})
}

func TestDecompress(t *testing.T) {
	type user struct {
		ID    int      `json:"id"`
		Email string   `json:"email"`
		Roles []string `json:"roles"`
	}

	// ローカルヘルパー
	gzipCompressJSON := func(t *testing.T, v any) []byte {
		t.Helper()
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		assert.Exactly(t, nil, json.NewEncoder(gw).Encode(v))
		assert.Exactly(t, nil, gw.Close())
		return buf.Bytes()
	}
	gzipCompressRaw := func(t *testing.T, b []byte) []byte {
		t.Helper()
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		_, err := gw.Write(b)
		assert.Exactly(t, nil, err)
		assert.Exactly(t, nil, gw.Close())
		return buf.Bytes()
	}
	truncateEnd := func(b []byte, n int) []byte {
		if n <= 0 || n >= len(b) {
			return nil
		}
		return b[:len(b)-n]
	}

	okData := user{ID: 42, Email: "u@example.com", Roles: []string{"admin", "dev"}}
	okCompressed := gzipCompressJSON(t, okData)

	t.Run("empty input case", func(t *testing.T) {
		var out user
		err := Decompress([]byte{}, &out, 0)
		assert.Exactly(t, true, err != nil)
		assert.Exactly(t, true, strings.Contains(err.Error(), "compressedData cannot be empty"))
	})

	t.Run("nil output pointer case", func(t *testing.T) {
		var out *user = nil
		err := Decompress(okCompressed, out, 0)
		assert.Exactly(t, true, err != nil)
		assert.Exactly(t, true, strings.Contains(err.Error(), "output pointer cannot be nil"))
	})

	t.Run("invalid gzip header case", func(t *testing.T) {
		var out user
		err := Decompress([]byte("not gzipped"), &out, 0)
		assert.Exactly(t, true, err != nil)
		assert.Exactly(t, true, strings.Contains(err.Error(), "failed to create gzip reader"))
	})

	t.Run("decode size exceeds limit case", func(t *testing.T) {
		// 100文字 → limit 10で越える
		comp := gzipCompressJSON(t, strings.Repeat("a", 100))
		var s string
		err := Decompress(comp, &s, 10)
		assert.Exactly(t, true, err != nil)
		assert.Exactly(t, true, strings.Contains(err.Error(), "decompressed size exceeds limit: 10 bytes"))
	})

	t.Run("invalid json case", func(t *testing.T) {
		comp := gzipCompressRaw(t, []byte("{")) // 不正JSON
		m := map[string]any{}
		err := Decompress(comp, &m, 1024)
		assert.Exactly(t, true, err != nil)
		assert.Exactly(t, true, strings.Contains(err.Error(), "failed to decode JSON"))
	})

	t.Run("corrupted gzip stream case", func(t *testing.T) {
		gz := gzipCompressJSON(t, map[string]any{"ok": true})
		corrupted := truncateEnd(gz, 4) // フッター壊す
		m := map[string]any{}
		err := Decompress(corrupted, &m, 1024)
		assert.Exactly(t, true, err != nil)
		assert.Exactly(t, true, strings.Contains(err.Error(), "failed to verify complete gzip stream"))
	})

	t.Run("success default limit case", func(t *testing.T) {
		var got user
		err := Decompress(okCompressed, &got, 0) // 0 → デフォルト4MB
		assert.Exactly(t, nil, err)
		assert.Exactly(t, okData, got)
	})

	t.Run("success small but enough limit case", func(t *testing.T) {
		var got user
		err := Decompress(okCompressed, &got, 256)
		assert.Exactly(t, nil, err)
		assert.Exactly(t, okData, got)
	})
}
