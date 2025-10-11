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
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

		bucket, prefix, pattern, err := parseS3UrlWithPattern(s3url)
		if err != nil {
			slog.Info("invalid S3 url", "err", err)
			os.Exit(1)
		}

		// If there's a glob pattern in the URL, use the pattern-aware presigned URL generation
		if pattern != "" {
			if err := generatePresignedForS3WithPattern(client, bucket, prefix, pattern, *signMins); err != nil {
				slog.Info("failed to generate presigned url(s)", "err", err)
				os.Exit(1)
			}
		} else {
			if err := generatePresignedForS3WithPattern(client, bucket, prefix, "", *signMins); err != nil {
				slog.Info("failed to generate presigned url(s)", "err", err)
				os.Exit(1)
			}
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
			bucket, prefix, pattern, err := parseS3UrlWithPattern(s3url)
			if err != nil {
				slog.Info("invalid S3 url", "err", err)
				os.Exit(1)
			}

			// If there's a glob pattern in the URL, use the pattern-aware listing function
			if pattern != "" {
				if err := listS3PrefixWithPattern(client, bucket, prefix, pattern); err != nil {
					slog.Info("list error", "err", err)
					os.Exit(1)
				}
			} else {
				if err := listS3PrefixWithPattern(client, bucket, prefix, ""); err != nil {
					slog.Info("list error", "err", err)
					os.Exit(1)
				}
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
			bucket, prefix, pattern, err := parseS3UrlWithPattern(src)
			if err != nil {
				slog.Info("invalid S3 url", "err", err)
				os.Exit(1)
			}

			// Check if we're downloading a single S3 object to a local directory
			// If the destination is a directory and the S3 path refers to a single file (doesn't end with '/'),
			// and there's no glob pattern, we should append the filename to the destination
			var adjustedDst string
			if pattern == "" && !strings.HasSuffix(prefix, "/") {
				// This looks like a single S3 object (not a prefix/directory)
				fi, err := os.Stat(dst)
				if err == nil && fi.IsDir() {
					// Destination is a directory, so append the S3 object filename
					adjustedDst = filepath.Join(dst, filepath.Base(prefix))
				} else {
					// Destination is not a directory, use as is
					adjustedDst = dst
				}
			} else {
				// This is a prefix/directory operation or has a pattern, use destination as is
				adjustedDst = dst
			}

			// Use the unified download function with pattern (empty pattern means no filtering)
			err = downloadS3Prefix(client, bucket, prefix, pattern, adjustedDst, *concurrency)
		

			if err != nil {
				slog.Info("download error", "err", err)
				os.Exit(1)
			}
		} else if !isS3Url(src) && isS3Url(dst) {
			// Upload
			bucket, prefix, pattern, err := parseS3UrlWithPattern(dst)
			if err != nil {
				slog.Info("invalid S3 url", "err", err)
				os.Exit(1)
			}
			// Uploading doesn't make sense with glob patterns, so error if a pattern is found
			if pattern != "" {
				slog.Info("glob patterns not supported for upload destinations")
				os.Exit(1)
			}

			// Check if we're uploading a local file to an S3 directory/prefix
			// If the source is a file and the S3 destination looks like a directory (ends with '/'),
			// we should append the filename to the S3 prefix
			var adjustedPrefix string
			fi, err := os.Stat(src)
			if err == nil && !fi.IsDir() && strings.HasSuffix(prefix, "/") {
				// Source is a file and destination is a directory/prefix, append filename
				adjustedPrefix = filepath.Join(prefix, filepath.Base(src))
				// Convert back to S3 format (forward slashes)
				adjustedPrefix = filepath.ToSlash(adjustedPrefix)
			} else {
				// Use prefix as is
				adjustedPrefix = prefix
			}

			err = uploadPathToS3(client, src, bucket, adjustedPrefix, *concurrency)
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

// generatePresignedForS3WithPattern will generate presigned GET URLs for objects that match a glob pattern
func generatePresignedForS3WithPattern(client *s3.Client, bucket, prefix, pattern string, mins int) error {
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

	// If prefix ends with a slash or if there's a pattern, treat as directory and list objects.
	if strings.HasSuffix(prefix, "/") || prefix == "" || pattern != "" {
		return listS3ObjectsWithPattern(client, bucket, prefix, pattern, func(key string, obj *types.Object) error {
			return gen(key)
		})
	}

	// Otherwise try to head the object. If it exists, return a single presigned URL.
	_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(prefix),
	})
	if err == nil {
		// If there's a pattern, check if the single object matches
		if pattern != "" {
			relPath := strings.TrimPrefix(prefix, prefix)
			relPath = strings.TrimPrefix(relPath, "/")
			if relPath == "" {
				relPath = prefix
			}
			matches, err := filepath.Match(pattern, filepath.Base(relPath))
			if err != nil {
				slog.Error("failed to match pattern", "pattern", pattern, "file", relPath, "err", err)
				return err
			}

			if !matches {
				return nil // No matches found, return without error
			}
		}
		return gen(prefix)
	}

	// If HeadObject failed, fall back to listing objects with the prefix (in
	// case the user omitted a trailing slash but intended a prefix).
	found := false
	err = listS3ObjectsWithPattern(client, bucket, prefix, pattern, func(key string, obj *types.Object) error {
		if err := gen(key); err != nil {
			return err
		}
		found = true
		return nil
	})
	if err != nil {
		return err
	}

	if !found && pattern != "" {
		return fmt.Errorf("no objects found matching pattern %s for s3://%s/%s", pattern, bucket, prefix)
	} else if !found {
		return fmt.Errorf("no objects found for s3://%s/%s", bucket, prefix)
	}
	return nil
}

func isS3Url(s string) bool {
	return strings.HasPrefix(s, "s3://")
}

// parseS3UrlWithPattern extracts both the bucket/prefix and any glob pattern from the URL
func parseS3UrlWithPattern(s string) (bucket, prefix, pattern string, err error) {
	if !isS3Url(s) {
		return "", "", "", errors.New("not an s3 url")
	}
	trimmed := strings.TrimPrefix(s, "s3://")
	parts := strings.SplitN(trimmed, "/", 2)
	bucket = parts[0]

	if len(parts) > 1 {
		originalPath := parts[1]

		// Check if the path contains glob patterns
		if strings.ContainsAny(originalPath, "*?[]{}") {
			// Extract the directory part and pattern part
			dir := filepath.Dir(originalPath)
			pattern = filepath.Base(originalPath)

			// If the directory part is "." then there's no parent directory
			if dir == "." {
				prefix = ""
			} else {
				prefix = dir
			}

			// Ensure prefix ends with "/" if it's not empty and doesn't already
			if prefix != "" && !strings.HasSuffix(prefix, "/") {
				prefix += "/"
			}
		} else {
			// No glob characters, treat as regular path
			prefix = originalPath
		}
	}
	return
}

// listS3PrefixWithPattern lists S3 objects that match a glob pattern
func listS3PrefixWithPattern(client *s3.Client, bucket, prefix, pattern string) error {

	found := false
	err := listS3ObjectsWithPattern(client, bucket, prefix, pattern, func(key string, obj *types.Object) error {
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
		fmt.Printf("%s\t%s\ts3://%s/%s\n", lastModTime, humanSize(size), bucket, key)
		found = true
		return nil
	})

	if err != nil {
		return err
	}

	if !found {
		if pattern != "" {
			return fmt.Errorf("no objects found matching pattern %s for s3://%s/%s", pattern, bucket, prefix)
		} else {
			return fmt.Errorf("no objects found for s3://%s/%s", bucket, prefix)
		}
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

// listS3ObjectsWithPattern iterates through S3 objects and calls the provided callback function for each object
// that matches the optional glob pattern. It handles the common logic of listing, filtering, and skipping
// directory placeholders.
func listS3ObjectsWithPattern(client *s3.Client, bucket, prefix, pattern string, callback func(key string, obj *types.Object) error) error {
	ctx := context.TODO()

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

			key := *obj.Key
			// Extract the relative path after the prefix
			relPath := strings.TrimPrefix(key, prefix)
			relPath = strings.TrimPrefix(relPath, "/")

			// If pattern is provided, check if the relative path matches
			if pattern != "" {
				matches, err := filepath.Match(pattern, filepath.Base(relPath))
				if err != nil {
					slog.Error("failed to match pattern", "pattern", pattern, "file", relPath, "err", err)
					continue
				}

				if !matches {
					continue
				}
			}

			// Call the callback function for this matching object
			if err := callback(key, &obj); err != nil {
				return err
			}
		}
	}
	return nil
}

// downloadTask represents a single file download task
type downloadTask struct {
	bucket   string
	key      string
	destFile string
}

func downloadS3Prefix(client *s3.Client, bucket, prefix, pattern, localPath string, concurrency int) error {
	if pattern != "" {
		slog.Info("downloading with pattern", "bucket", bucket, "prefix", prefix, "pattern", pattern, "localPath", localPath, "concurrency", concurrency)
	} else {
		slog.Info("downloading", "bucket", bucket, "prefix", prefix, "localPath", localPath, "concurrency", concurrency)
	}

	downloader := manager.NewDownloader(client)
	
	// Create the worker pool
	tasks := make(chan downloadTask, concurrency*2) // buffered channel to allow some queueing
	
	var wg sync.WaitGroup
	// Start workers
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range tasks {
				slog.Info("file downloading", "key", task.key, "file", task.destFile)
				f, err := os.Create(task.destFile)
				if err != nil {
					slog.Error("failed to create file", "file", task.destFile, "err", err)
					continue
				}
				
				_, err = downloader.Download(context.TODO(), f, &s3.GetObjectInput{
					Bucket: aws.String(task.bucket),
					Key:    aws.String(task.key),
				})
				if err != nil {
					slog.Error("failed to download", "err", err)
					f.Close()
					continue
				}
				
				slog.Info("file downloaded", "key", task.key, "file", task.destFile)
				f.Close()
			}
		}()
	}

	// Send download tasks to the worker pool
	err := listS3ObjectsWithPattern(client, bucket, prefix, pattern, func(key string, obj *types.Object) error {
		// Extract the relative path after the prefix
		relPath := strings.TrimPrefix(key, prefix)
		relPath = strings.TrimPrefix(relPath, "/")

		localFile := filepath.Join(localPath, relPath)
		// slog.Info("preparing to download", "key", key, "prefix", prefix, "relPath", relPath, "localFile", localFile)
		if err := os.MkdirAll(filepath.Dir(localFile), 0750); err != nil {
			slog.Error("failed to create directories", "path", filepath.Dir(localFile), "err", err)
			return err
		}

		// Send task to worker pool
		tasks <- downloadTask{
			bucket:   bucket,
			key:      key,
			destFile: localFile,
		}

		return nil
	})

	// Close the tasks channel to signal workers to finish
	close(tasks)
	
	// Wait for all workers to finish
	wg.Wait()

	return err
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
