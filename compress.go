package compress

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"

	"go-server/pkg/errors"
)

const (
	// 展開後の最大サイズ (4MB)
	defaultMaxDecompressedSize = 4 * 1024 * 1024
)

// Compress JSONデータをGZIP圧縮する
func Compress(data any) ([]byte, error) {
	if data == nil {
		return nil, errors.New(errors.InvalidParams, "data cannot be nil")
	}

	// bytes.Bufferで最終出力用のメモリを確保し、gzipWriterでストリーム圧縮を行う
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	enc := json.NewEncoder(gw)

	// Encoderで直接書き込みすることで中間バッファが不要となり、JSONエンコードとGZIP圧縮を一度の処理で実行できる
	if err := enc.Encode(data); err != nil {
		_ = gw.Close() // エラー時もリソースを確実に解放
		return nil, errors.Wrapf(err, errors.InternalServerError, "failed to encode JSON (type:%T)", data)
	}

	// GZIP圧縮の完了処理
	// Close時にフッターの書き込みとバッファのフラッシュが行われる
	// エラーは破損の可能性があるため、InternalServerErrorとして扱う
	if err := gw.Close(); err != nil {
		return nil, errors.Wrapf(err, errors.InternalServerError, "failed to finalize compression")
	}

	return buf.Bytes(), nil
}

// Decompress GZIP圧縮されたJSONデータを展開する
// maxUncompressed が 0 の場合はデフォルトの制限(4MB)を使用
func Decompress[T any](compressedData []byte, out *T, maxUncompressed int64) error {
	if len(compressedData) == 0 {
		return errors.New(errors.InvalidParams, "compressedData cannot be empty")
	}

	if out == nil {
		return errors.New(errors.InvalidParams, "output pointer cannot be nil")
	}

	// GZIP展開サイズの上限値未指定の場合は、デフォルト値を適用
	if maxUncompressed <= 0 {
		maxUncompressed = defaultMaxDecompressedSize
	}

	// bytes.NewReaderで入力バッファを直接読み込み、メモリ使用を効率化
	gr, err := gzip.NewReader(bytes.NewReader(compressedData))
	if err != nil {
		return errors.Wrapf(err, errors.InternalServerError, "failed to create gzip reader")
	}
	defer gr.Close() // リソースリーク防止

	// DoS対策: io.LimitedReaderで展開サイズを制限
	// JSONデコード中にサイズ制限を超えた場合、即座にエラーを返す
	lr := &io.LimitedReader{R: gr, N: maxUncompressed}
	if err := json.NewDecoder(lr).Decode(out); err != nil {
		if lr.N <= 0 {
			return errors.Errorf(errors.InvalidParams, "decompressed size exceeds limit: %d bytes", maxUncompressed)
		}
		return errors.Wrapf(err, errors.InternalServerError, "failed to decode JSON")
	}

	// データ整合性の検証
	// ・JSONデコード後の未読データも必ずサイズ制限内で読み切る
	// ・GZIPのCRCチェックはストリームを完全に読了する必要がある
	// ・同じLimitedReaderを使用し、残りデータも制限対象とする
	if _, err := io.Copy(io.Discard, lr); err != nil {
		if lr.N <= 0 {
			return errors.Errorf(errors.InvalidParams, "decompressed size exceeds limit: %d bytes", maxUncompressed)
		}
		return errors.Wrapf(err, errors.InternalServerError, "failed to verify complete gzip stream")
	}

	return nil
}
