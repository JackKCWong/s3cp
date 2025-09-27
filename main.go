package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/semaphore"
)

func main() {
	concurrency := flag.Int("c", runtime.NumCPU()*4, "concurrency")
	flag.Parse()

	if len(flag.Args()) != 2 {
		slog.Info("Usage: s3cp <src> <dst>")
		os.Exit(1)
	}

	src := flag.Arg(0)
	dst := flag.Arg(1)

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		slog.Info("failed to load AWS config", "err", err)
		os.Exit(1)
	}
	client := s3.NewFromConfig(cfg)

	if isS3Url(src) && !isS3Url(dst) {
		// Download
		bucket, prefix, err := parseS3Url(src)
		if err != nil {
			slog.Info("invalid S3 url", "err", err)
			os.Exit(1)
		}
		err = downloadS3PrefixTransferManager(client, bucket, prefix, dst, *concurrency)
		if err != nil {
			slog.Info("download error", "err", err)
			os.Exit(1)
		}
	} else if !isS3Url(src) && isS3Url(dst) {
		// Upload
		bucket, prefix, err := parseS3Url(dst)
		if err != nil {
			slog.Info("invalid S3 url", "err", err)
			os.Exit(1)
		}
		err = uploadPathToS3TransferManager(client, src, bucket, prefix, *concurrency)
		if err != nil {
			slog.Info("upload error", "err", err)
			os.Exit(1)
		}
	} else {
		slog.Info("Either source or destination must be an S3 url (s3://bucket/prefix)")
		os.Exit(1)
	}
}

func isS3Url(s string) bool {
	return strings.HasPrefix(s, "s3://")
}

func parseS3Url(s string) (bucket, prefix string, err error) {
	if !isS3Url(s) {
		return "", "", errors.New("not an s3 url")
	}
	trimmed := strings.TrimPrefix(s, "s3://")
	parts := strings.SplitN(trimmed, "/", 2)
	bucket = parts[0]
	if len(parts) > 1 {
		prefix = parts[1]
	}
	return
}

func downloadS3PrefixTransferManager(client *s3.Client, bucket, prefix, localPath string, concurrency int) error {
	ctx := context.TODO()
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	transferManager := manager.NewDownloader(client)
	type download struct {
		LocalPath string
		Bucket    string
		Key       string
	}

	wg := sync.WaitGroup{}
	sem := semaphore.NewWeighted(int64(concurrency))

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}
		for _, obj := range page.Contents {
			relPath := strings.TrimPrefix(*obj.Key, prefix)
			relPath = strings.TrimPrefix(relPath, "/")
			localFile := filepath.Join(localPath, relPath)
			if err := os.MkdirAll(filepath.Dir(localFile), 0755); err != nil {
				return err
			}

			err = sem.Acquire(ctx, 1)
			if err != nil {
				return err
			}
			wg.Add(1)
			go func(bucket, key, destFile string) error {
				defer wg.Done()
				defer sem.Release(1)
				f, err := os.Create(destFile)
				if err != nil {
					return err
				}
				defer f.Close()

				_, err = transferManager.Download(ctx, f, &s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				if err != nil {
					slog.Error("failed to download", "err", err)
					return err
				}
				slog.Info("file downloaded", "key", *obj.Key, "file", localFile)
				return nil
			}(localFile, bucket, *obj.Key)
		}
	}

	return nil
}

func uploadPathToS3TransferManager(client *s3.Client, localPath, bucket, prefix string, concurrency int) error {
	transferManager := manager.NewUploader(client)
	ctx := context.TODO()

	return filepath.Walk(localPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(localPath, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(filepath.Join(prefix, rel))

		sem := semaphore.NewWeighted(int64(concurrency))
		wg := sync.WaitGroup{}

		err = sem.Acquire(context.Background(), 1)
		if err != nil {
			return err
		}

		wg.Add(1)
		go func() error {
			defer wg.Done()
			defer sem.Release(1)
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()

			_, err = transferManager.Upload(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   f,
			})
			if err != nil {
				return err
			}

			slog.Info("file uploaded", "file", path, "s3", fmt.Sprintf("%s/%s", bucket, key))
			return nil
		}()

		return nil
	})
}
