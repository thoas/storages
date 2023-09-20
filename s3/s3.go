package s3

import (
	"bytes"
	"context"
	"io"
	"mime"
	"net/http"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"
	"github.com/ulule/gostorages"
)

// Storage is a s3 storage.
type Storage struct {
	bucket   string
	s3       *s3.S3
	uploader *s3manager.Uploader
}

// NewStorage returns a new Storage.
func NewStorage(cfg Config) (*Storage, error) {
	awscfg := &aws.Config{
		Credentials: credentials.NewStaticCredentialsFromCreds(credentials.Value{
			AccessKeyID:     cfg.AccessKeyID,
			SecretAccessKey: cfg.SecretAccessKey,
		}),
		Region: aws.String(cfg.Region),
	}
	if cfg.Endpoint != "" {
		awscfg.Endpoint = &(cfg.Endpoint)
	}
	s, err := session.NewSession(awscfg)
	if err != nil {
		return nil, err
	}

	return &Storage{
		bucket:   cfg.Bucket,
		s3:       s3.New(s),
		uploader: s3manager.NewUploader(s),
	}, nil
}

// Config is the configuration for Storage.
type Config struct {
	AccessKeyID     string
	Bucket          string
	Endpoint        string
	Region          string
	SecretAccessKey string
}

// Save saves content to path.
func (s *Storage) Save(ctx context.Context, content io.Reader, path string) error {
	input := &s3manager.UploadInput{
		ACL:    aws.String(s3.ObjectCannedACLPublicRead),
		Body:   content,
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	}

	contenttype := mime.TypeByExtension(filepath.Ext(path)) // first, detect content type from extension
	if contenttype == "" {
		// second, detect content type from first 512 bytes of content
		data := make([]byte, 512)
		if _, err := content.Read(data); err != nil {
			return err
		}
		contenttype = http.DetectContentType(data)
		input.Body = io.MultiReader(bytes.NewReader(data), content)
	}
	if contenttype != "" {
		input.ContentType = aws.String(contenttype)
	}

	_, err := s.uploader.UploadWithContext(ctx, input)
	return err
}

// Stat returns path metadata.
func (s *Storage) Stat(ctx context.Context, path string) (*gostorages.Stat, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	}
	out, err := s.s3.HeadObjectWithContext(ctx, input)

	if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
		return nil, gostorages.ErrNotExist
	} else if err != nil {
		return nil, err
	}

	return &gostorages.Stat{
		ModifiedTime: *out.LastModified,
		Size:         *out.ContentLength,
	}, nil
}

// Open opens path for reading.
func (s *Storage) Open(ctx context.Context, path string) (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	}
	out, err := s.s3.GetObjectWithContext(ctx, input)
	if aerr, ok := err.(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchKey {
		return nil, gostorages.ErrNotExist
	} else if err != nil {
		return nil, err
	}
	return out.Body, nil
}

// Delete deletes path.
func (s *Storage) Delete(ctx context.Context, path string) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	}
	_, err := s.s3.DeleteObjectWithContext(ctx, input)
	return err
}

// OpenWithStat opens path for reading with file stats.
func (s *Storage) OpenWithStat(ctx context.Context, path string) (io.ReadCloser, *gostorages.Stat, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	}
	out, err := s.s3.GetObjectWithContext(ctx, input)
	if aerr, ok := err.(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchKey {
		return nil, nil, errors.Wrapf(gostorages.ErrNotExist,
			"%s does not exist in bucket %s, code: %s", path, s.bucket, s3.ErrCodeNoSuchKey)
	} else if err != nil {
		return nil, nil, err
	}
	return out.Body, &gostorages.Stat{
		ModifiedTime: *out.LastModified,
		Size:         *out.ContentLength,
	}, nil
}
