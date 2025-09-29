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
	// -s now takes an integer number of minutes for the presigned URL expiry.
	// A value > 0 enables presigning; 0 (default) means disabled.
	signMins := flag.Int("s", 0, "generate presigned url(s) for S3 object or prefix; minutes until expiry")
	flag.Parse()

	// If -s is provided (minutes > 0) we expect a single S3 url argument and will print
	// presigned GET URLs to stdout (one per line for prefixes).
	if *signMins > 0 {
		if len(flag.Args()) != 1 {
			slog.Info("Usage: s3cp -s <minutes> s3://bucket/key_or_prefix/")
			os.Exit(1)
		}
		s3url := flag.Arg(0)
		cfg, err := config.LoadDefaultConfig(context.TODO())
		if err != nil {
			slog.Info("failed to load AWS config", "err", err)
			os.Exit(1)
		}
		client := s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.UsePathStyle = true
			o.Region = "default"
		})
		if !isS3Url(s3url) {
			slog.Info("invalid S3 url")
			os.Exit(1)
		}
		bucket, prefix, err := parseS3Url(s3url)
		if err != nil {
			slog.Info("invalid S3 url", "err", err)
			os.Exit(1)
		}
		if err := generatePresignedForS3(client, bucket, prefix, *signMins); err != nil {
			slog.Info("failed to generate presigned url(s)", "err", err)
			os.Exit(1)
		}
	} else {
		args := flag.Args()
		// Create AWS client early so listing mode can use it.
		cfg, err := config.LoadDefaultConfig(context.TODO())
		if err != nil {
			slog.Info("failed to load AWS config", "err", err)
			os.Exit(1)
		}
		client := s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.UsePathStyle = true
			o.Region = "default"
		})

		// Support a single-argument s3 listing mode: `s3cp s3://bucket/prefix/`
		if len(args) == 1 && isS3Url(args[0]) {
			s3url := args[0]
			bucket, prefix, err := parseS3Url(s3url)
			if err != nil {
				slog.Info("invalid S3 url", "err", err)
				os.Exit(1)
			}
			if err := listS3Prefix(client, bucket, prefix); err != nil {
				slog.Info("list error", "err", err)
				os.Exit(1)
			}
			return
		}

		if len(args) != 2 {
			slog.Info("Usage: s3cp <src> <dst>")
			os.Exit(1)
		}

		src := args[0]
		dst := args[1]

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

}

// generatePresignedForS3 will generate presigned GET URLs for either a single
// object (when prefix does not end with '/') or for all objects under a
// prefix when prefix ends with '/'. It writes one URL per line to stdout.
func generatePresignedForS3(client *s3.Client, bucket, prefix string, mins int) error {
	ctx := context.TODO()
	presigner := s3.NewPresignClient(client)

	// Helper to produce and print a presigned URL for a given key.
	gen := func(key string) error {
		req, err := presigner.PresignGetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		}, func(o *s3.PresignOptions) {
			o.Expires = time.Duration(mins) * time.Minute
		})
		if err != nil {
			return err
		}
		fmt.Println(req.URL)
		return nil
	}

	// If prefix ends with a slash, treat as directory and list objects.
	if strings.HasSuffix(prefix, "/") || prefix == "" {
		paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
			Prefix: aws.String(prefix),
		})
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				return err
			}
			for _, obj := range page.Contents {
				if obj.Key == nil {
					continue
				}
				if obj.Size != nil && *obj.Size == 0 {
					// skip directory placeholders
					continue
				}
				if err := gen(*obj.Key); err != nil {
					return err
				}
			}
		}
		return nil
	}

	// Otherwise try to head the object. If it exists, return a single presigned URL.
	_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(prefix),
	})
	if err == nil {
		return gen(prefix)
	}

	// If HeadObject failed, fall back to listing objects with the prefix (in
	// case the user omitted a trailing slash but intended a prefix).
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	found := false
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			if obj.Size != nil && *obj.Size == 0 {
				continue
			}
			if err := gen(*obj.Key); err != nil {
				return err
			}
			found = true
		}
	}
	if !found {
		return fmt.Errorf("no objects found for s3://%s/%s", bucket, prefix)
	}
	return nil
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

// listS3Prefix prints one s3://bucket/key line per object found under the
// given prefix. If prefix is an exact object key (no trailing slash) it will
// attempt a HeadObject and print that single key. If prefix ends with '/' or
// the HeadObject fails, it will list objects with the prefix and print them.
func listS3Prefix(client *s3.Client, bucket, prefix string) error {
	ctx := context.TODO()

	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	found := false
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			if obj.Size != nil && *obj.Size == 0 {
				// skip directory placeholders
				continue
			}
			size := int64(0)
			if obj.Size != nil {
				size = *obj.Size
			}
			var lastModTime string
			if obj.LastModified != nil {
				lastModTime = obj.LastModified.UTC().Format(time.RFC3339)
			} else {
				lastModTime = "-"
			}
			fmt.Printf("%s\t%s\ts3://%s/%s\n", lastModTime, humanSize(size), bucket, *obj.Key)
			found = true
		}
	}
	if !found {
		return fmt.Errorf("no objects found for s3://%s/%s", bucket, prefix)
	}
	return nil
}

// humanSize converts bytes into a human-friendly string using 1024 base.
func humanSize(bytes int64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%dB", bytes)
	}
	units := []string{"B", "K", "M", "G", "T", "P"}
	f := float64(bytes)
	idx := 0
	for f >= 1024 && idx < len(units)-1 {
		f /= 1024
		idx++
	}
	return fmt.Sprintf("%.0f%s", f, units[idx])
}

func downloadS3Prefix(client *s3.Client, bucket, prefix, localPath string, concurrency int) error {
	slog.Info("downloading", "bucket", bucket, "prefix", prefix, "localPath", localPath, "concurrency", concurrency)
	ctx := context.TODO()
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
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

		rel, err := filepath.Rel(localPath, path)
		if err != nil {
			slog.Error("failed to get relative path", "path", path, "err", err)
			return err
		}
		if rel == "." {
			rel = ""
		}

		// If this is a directory, create a 0-byte "placeholder" object with a
		// trailing slash to mimic a filesystem tree in S3. Skip creating a
		// placeholder for the root when both prefix and rel are empty.
		if d.IsDir() {
			key := filepath.ToSlash(filepath.Join(prefix, rel))
			if key == "" {
				// nothing to upload for the workspace root
				slog.Info("skipping root directory placeholder", "path", path)
				return nil
			}
			if !strings.HasSuffix(key, "/") {
				key = key + "/"
			}

			err = sem.Acquire(context.Background(), 1)
			if err != nil {
				slog.Error("failed to acquire semaphore", "err", err)
				return err
			}

			wg.Add(1)
			go func(key string) {
				defer wg.Done()
				defer sem.Release(1)

				slog.Info("creating directory placeholder", "s3", fmt.Sprintf("%s/%s", bucket, key))
				_, err := uploader.Upload(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
					Body:   strings.NewReader(""),
				})
				if err != nil {
					slog.Error("failed to create directory placeholder", "s3", fmt.Sprintf("%s/%s", bucket, key), "err", err)
				} else {
					slog.Info("created directory placeholder", "s3", fmt.Sprintf("%s/%s", bucket, key))
				}
			}(key)

			return nil
		}

		// It's a file: upload as before.
		key := filepath.ToSlash(filepath.Join(prefix, rel))
		err = sem.Acquire(context.Background(), 1)
		if err != nil {
			slog.Error("failed to acquire semaphore", "err", err)
			return err
		}

		slog.Info("file uploading", "file", path, "s3", fmt.Sprintf("%s/%s", bucket, key))
		wg.Add(1)
		go func(path, key string) {
			defer wg.Done()
			defer sem.Release(1)
			f, err := os.Open(path)
			if err != nil {
				slog.Error("failed to open file", "file", path, "err", err)
				return
			}
			defer f.Close()

			_, err = uploader.Upload(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   f,
			})
			if err != nil {
				slog.Error("failed to upload", "file", path, "s3", fmt.Sprintf("%s/%s", bucket, key), "err", err)
				return
			}

			slog.Info("file uploaded", "file", path, "s3", fmt.Sprintf("%s/%s", bucket, key))
		}(path, key)

		return nil
	})
}
