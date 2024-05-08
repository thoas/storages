package s3

import (
	"bytes"
	"context"
	"io"
	"mime"
	"net/http"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/pkg/errors"
	"github.com/ulule/gostorages"
)

type CustomAPIHTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

func withUploaderConcurrency(concurrency int64) func(uploader *manager.Uploader) {
	return func(uploader *manager.Uploader) {
		uploader.Concurrency = int(concurrency)
	}
}

// Storage is a s3 storage.
type Storage struct {
	bucket   string
	s3       *s3.Client
	uploader *manager.Uploader
	acl      types.ObjectCannedACL
}

// NewStorage returns a new Storage.
func NewStorage(cfg Config) (*Storage, error) {
	awscfg := aws.Config{
		Credentials: credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		Region:      *aws.String(cfg.Region),
	}
	var uploaderopts []func(uploader *manager.Uploader)
	client := s3.NewFromConfig(awscfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		if cfg.CustomHTTPClient != nil {
			o.HTTPClient = cfg.CustomHTTPClient
		}
	})

	if cfg.ACL == "" {
		cfg.ACL = types.ObjectCannedACLPublicRead
	}

	if cfg.UploadConcurrency != nil {
		uploaderopts = append(uploaderopts, withUploaderConcurrency(*cfg.UploadConcurrency))
	}
	return &Storage{
		bucket:   cfg.Bucket,
		s3:       client,
		uploader: manager.NewUploader(client, uploaderopts...),
		acl:      cfg.ACL,
	}, nil
}

// Config is the configuration for Storage.
type Config struct {
	AccessKeyID     string
	Bucket          string
	Endpoint        string
	Region          string
	SecretAccessKey string
	ACL             types.ObjectCannedACL

	UploadConcurrency *int64

	CustomHTTPClient CustomAPIHTTPClient
}

// Save saves content to path.
func (s *Storage) Save(ctx context.Context, content io.Reader, path string) error {
	input := &s3.PutObjectInput{
		Body:   content,
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
		ACL:    s.acl,
	}

	contenttype := mime.TypeByExtension(filepath.Ext(path)) // first, detect content type from extension
	if contenttype == "" {
		// second, detect content type from first 512 bytes of content
		data := make([]byte, 512)
		n, err := content.Read(data)
		if err != nil {
			return err
		}
		contenttype = http.DetectContentType(data)
		input.Body = io.MultiReader(bytes.NewReader(data[:n]), content)
	}
	if contenttype != "" {
		input.ContentType = aws.String(contenttype)
	}
	_, err := s.uploader.Upload(ctx, input)
	return errors.WithStack(err)
}

// Stat returns path metadata.
func (s *Storage) Stat(ctx context.Context, path string) (*gostorages.Stat, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	}
	out, err := s.s3.HeadObject(ctx, input)
	var notfounderr *types.NotFound
	if errors.As(err, &notfounderr) {
		return nil, gostorages.ErrNotExist
	} else if err != nil {
		return nil, errors.WithStack(err)
	}

	return &gostorages.Stat{
		ModifiedTime: *out.LastModified,
		Size:         out.ContentLength,
	}, nil
}

// Open opens path for reading.
func (s *Storage) Open(ctx context.Context, path string) (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	}
	out, err := s.s3.GetObject(ctx, input)
	var notsuckkeyerr *types.NoSuchKey
	if errors.As(err, &notsuckkeyerr) {
		return nil, gostorages.ErrNotExist
	} else if err != nil {
		return nil, errors.WithStack(err)
	}
	return out.Body, nil
}

// Delete deletes path.
func (s *Storage) Delete(ctx context.Context, path string) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	}
	_, err := s.s3.DeleteObject(ctx, input)
	return errors.WithStack(err)
}

// OpenWithStat opens path for reading with file stats.
func (s *Storage) OpenWithStat(ctx context.Context, path string) (io.ReadCloser, *gostorages.Stat, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	}

	out, err := s.s3.GetObject(ctx, input)
	var notsuckkeyerr *types.NoSuchKey
	if errors.As(err, &notsuckkeyerr) {
		return nil, nil, errors.Wrapf(gostorages.ErrNotExist,
			"%s does not exist in bucket %s, code: %s", path, s.bucket, notsuckkeyerr.Error())
	} else if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	return out.Body, &gostorages.Stat{
		ModifiedTime: *out.LastModified,
		Size:         out.ContentLength,
	}, nil
}
