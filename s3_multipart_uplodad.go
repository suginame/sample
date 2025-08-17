package model

import (
	"context"
	"go-server/domain/model"
	"go-server/pkg/errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// CompletedPart S3のマルチパートアップロードを完了する際の1パート分の情報を表す
type CompletedPart struct {
	// パート番号（1 から始まる連番）
	PartNumber int32 `json:"partNumber"`
	// アップロード後に S3 が返す ETag（完了処理時に必須）
	ETag string `json:"eTag"`
}

// CreateMultipartUpload 開始する
func (sp *s3Proxy) CreateMultipartUpload(ctx context.Context, bucket, key string) (string, error) {
	out, err := sp.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return "", errors.Wrapf(err, errors.Storage, "failed to create multipart upload(%s/%s)", bucket, key)
	}

	if out.UploadId == nil {
		return "", errors.Wrapf(nil, errors.Storage, "empty upload id(%s/%s)", bucket, key)
	}

	return *out.UploadId, nil
}

// PresignMultipartUploadPart 指定したパート用の署名付きURLを発行する
func (sp *s3Proxy) PresignMultipartUploadPart(
	ctx context.Context, bucket, key, uploadID string, partNumber int32, expires time.Duration) (string, error) {
	presigner := s3.NewPresignClient(sp.client)

	input := &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(partNumber),
	}

	res, err := presigner.PresignUploadPart(ctx, input, s3.WithPresignExpires(expires))
	if err != nil {
		return "", errors.Wrapf(err, errors.Storage, "failed to presign upload part(%s/%s) part:%d", bucket, key, partNumber)
	}

	return res.URL, nil
}

// CompleteMultipartUpload アップロードを完了する
func (sp *s3Proxy) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []model.CompletedPart) error {
	cps := make([]types.CompletedPart, 0, len(parts))
	for _, p := range parts {
		cps = append(cps, types.CompletedPart{
			ETag:       aws.String(p.ETag),
			PartNumber: aws.Int32(p.PartNumber),
		})
	}

	_, err := sp.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: cps,
		},
	})

	if err != nil {
		return errors.Wrapf(err, errors.Storage, "failed to complete multipart upload(%s/%s)", bucket, key)
	}

	return nil
}

// AbortMultipartUpload アップロードを取り消す
func (sp *s3Proxy) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	_, err := sp.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})

	if err != nil {
		return errors.Wrapf(err, errors.Storage, "failed to abort multipart upload(%s/%s)", bucket, key)
	}

	return nil
}

func (s *S3TestSuite) Test_s3Proxy_MultipartUpload() {
	ctx := context.Background()

	s.Run("success case", func() {
		// initiate
		uploadID, err := s.s3.CreateMultipartUpload(ctx, s.testBucket, "multipart/test.bin")
		s.NoError(err)
		s.NotEmpty(uploadID)

		// presign part 1
		url, err := s.s3.PresignMultipartUploadPart(ctx, s.testBucket, "multipart/test.bin", uploadID, 1, time.Minute*15)
		s.NoError(err)
		s.NotEmpty(url)

		// abort
		s.NoError(s.s3.AbortMultipartUpload(ctx, s.testBucket, "multipart/test.bin", uploadID))
	})

	s.Run("error case", func() {
		// invalid initiate
		id, err := s.s3.CreateMultipartUpload(ctx, "", "")
		s.Empty(id)
		s.ErrorContains(err, "failed to create multipart upload")

		// invalid presign
		url, err := s.s3.PresignMultipartUploadPart(ctx, "", "", "", 0, time.Nanosecond)
		s.ErrorContains(err, "failed to presign upload part")
		s.Empty(url)

		// invalid complete
		err = s.s3.CompleteMultipartUpload(ctx, "", "", "", nil)
		s.ErrorContains(err, "failed to complete multipart upload")

		// invalid abort
		err = s.s3.AbortMultipartUpload(ctx, "", "", "")
		s.ErrorContains(err, "failed to abort multipart upload")
	})
}
