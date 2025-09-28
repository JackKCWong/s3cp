package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/lmittmann/tint"
	"golang.org/x/sync/semaphore"
)

func main() {
	slog.SetDefault(slog.New(tint.NewHandler(os.Stdout, &tint.Options{
		Level:      slog.LevelDebug,
		TimeFormat: time.Kitchen,
	})))
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
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.Region = "default"
	})

	if isS3Url(src) && !isS3Url(dst) {
		// Download
		bucket, prefix, err := parseS3Url(src)
		if err != nil {
			slog.Info("invalid S3 url", "err", err)
			os.Exit(1)
		}
		err = downloadS3Prefix(client, bucket, prefix, dst, *concurrency)
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
		err = uploadPathToS3(client, src, bucket, prefix, *concurrency)
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

func downloadS3Prefix(client *s3.Client, bucket, prefix, localPath string, concurrency int) error {
	slog.Info("downloading", "bucket", bucket, "prefix", prefix, "localPath", localPath, "concurrency", concurrency)
	ctx := context.TODO()
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
		Delimiter: aws.String("/"),
	})

	downloader := manager.NewDownloader(client)

	wg := sync.WaitGroup{}
	defer wg.Wait()

	sem := semaphore.NewWeighted(int64(concurrency))

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			slog.Error("failed to get next page", "err", err)
			return err
		}
		for _, obj := range page.Contents {
			slog.Info("found object", "key", *obj.Key, "size", *obj.Size)
			if *obj.Size == 0 {
				// skip directories
				continue
			}

			relPath := strings.TrimPrefix(*obj.Key, prefix)
			relPath = strings.TrimPrefix(relPath, "/")
			localFile := filepath.Join(localPath, relPath)
			// slog.Info("preparing to download", "key", *obj.Key, "prefix", prefix, "relPath", relPath, "localFile", localFile)
			if err := os.MkdirAll(filepath.Dir(localFile), 0750); err != nil {
				slog.Error("failed to create directories", "path", filepath.Dir(localFile), "err", err)
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
				slog.Info("file downloading", "key", key, "file", destFile)
				f, err := os.Create(destFile)
				if err != nil {
					slog.Error("failed to create file", "file", destFile, "err", err)
					return err
				}
				defer f.Close()

				_, err = downloader.Download(ctx, f, &s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				if err != nil {
					slog.Error("failed to download", "err", err)
					return err
				}
				slog.Info("file downloaded", "key", *obj.Key, "file", destFile)
				return nil
			}(bucket, *obj.Key, localFile)
		}
	}

	return nil
}

func uploadPathToS3(client *s3.Client, localPath, bucket, prefix string, concurrency int) error {
	slog.Info("uploading", "localPath", localPath, "bucket", bucket, "prefix", prefix, "concurrency", concurrency)
	uploader := manager.NewUploader(client)
	ctx := context.TODO()

	sem := semaphore.NewWeighted(int64(concurrency))
	wg := sync.WaitGroup{}
	defer wg.Wait()

	return filepath.WalkDir(localPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			slog.Error("failed to access path", "path", path, "err", err)
			return err
		}
		if d.IsDir() {
			slog.Info("skipping directory", "path", path)
			return nil
		}
		rel, err := filepath.Rel(localPath, path)
		if err != nil {
			slog.Error("failed to get relative path", "path", path, "err", err)
			return err
		}
		key := filepath.ToSlash(filepath.Join(prefix, rel))
		err = sem.Acquire(context.Background(), 1)
		if err != nil {
			slog.Error("failed to acquire semaphore", "err", err)
			return err
		}

		slog.Info("file uploading", "file", path, "s3", fmt.Sprintf("%s/%s", bucket, key))
		wg.Add(1)
		go func() error {
			defer wg.Done()
			defer sem.Release(1)
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()

			_, err = uploader.Upload(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   f,
			})
			if err != nil {
				slog.Error("failed to upload", "file", path, "s3", fmt.Sprintf("%s/%s", bucket, key), "err", err)
				return err
			}

			slog.Info("file uploaded", "file", path, "s3", fmt.Sprintf("%s/%s", bucket, key))
			return nil
		}()

		return nil
	})
}
