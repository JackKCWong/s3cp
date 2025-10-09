package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseS3UrlWithPattern(t *testing.T) {
	testCases := []struct {
		input    string
		bucket   string
		prefix   string
		pattern  string
		hasError bool
	}{
		{
			input:    "s3://my-bucket/path/to/*.txt",
			bucket:   "my-bucket",
			prefix:   "path/to/",
			pattern:  "*.txt",
			hasError: false,
		},
		{
			input:    "s3://my-bucket/*.csv.gz",
			bucket:   "my-bucket",
			prefix:   "",
			pattern:  "*.csv.gz",
			hasError: false,
		},
		{
			input:    "s3://my-bucket/path/to/files/",
			bucket:   "my-bucket",
			prefix:   "path/to/files/",
			pattern:  "",
			hasError: false,
		},
		{
			input:    "s3://my-bucket/path/to/files/file.txt",
			bucket:   "my-bucket",
			prefix:   "path/to/files/file.txt",
			pattern:  "",
			hasError: false,
		},
		{
			input:    "s3://my-bucket/dir1/**.csv.gz",
			bucket:   "my-bucket",
			prefix:   "dir1/",
			pattern:  "**.csv.gz",
			hasError: false,
		},
	}

	for _, tc := range testCases {
		bucket, prefix, pattern, err := parseS3UrlWithPattern(tc.input)
		if tc.hasError && err == nil {
			t.Errorf("Expected error for input %s, but got none", tc.input)
		}
		if !tc.hasError && err != nil {
			t.Errorf("Unexpected error for input %s: %v", tc.input, err)
		}
		if bucket != tc.bucket {
			t.Errorf("For input %s, expected bucket %s, got %s", tc.input, tc.bucket, bucket)
		}
		if prefix != tc.prefix {
			t.Errorf("For input %s, expected prefix %s, got %s", tc.input, tc.prefix, prefix)
		}
		if pattern != tc.pattern {
			t.Errorf("For input %s, expected pattern %s, got %s", tc.input, tc.pattern, pattern)
		}

		fmt.Printf("Input: %s\n -> Bucket: %s, Prefix: %s, Pattern: %s\n\n", tc.input, bucket, prefix, pattern)
	}
}

// TestPathAdjustment tests the logic for automatically adding filename to destination when src is file and dest is dir
func TestPathAdjustment(t *testing.T) {
	// Create a temporary file and directory for testing
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "testfile.txt")

	// Create the test file
	err := os.WriteFile(tempFile, []byte("test content"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	// Create a destination directory
	destDir := filepath.Join(tempDir, "destination")
	err = os.MkdirAll(destDir, 0755)
	if err != nil {
		t.Fatal(err)
	}

	// Test 1: Local file to S3 directory - should append filename to prefix
	fi, err := os.Stat(tempFile)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate the logic from the main function for upload path
	src := tempFile
	dst := "s3://my-bucket/prefix/"

	// Extract prefix from S3 URL
	trimmed := strings.TrimPrefix(dst, "s3://")
	parts := strings.SplitN(trimmed, "/", 2)
	prefix := parts[1]

	// Check if source is a file and destination is a directory-like prefix
	adjustedPrefix := prefix
	if !fi.IsDir() && strings.HasSuffix(prefix, "/") {
		adjustedPrefix = filepath.Join(prefix, filepath.Base(src))
		// Convert back to S3 format (forward slashes)
		adjustedPrefix = filepath.ToSlash(adjustedPrefix)
	}

	expected := "prefix/testfile.txt"
	if adjustedPrefix != expected {
		t.Errorf("Expected adjusted prefix to be %s, got %s", expected, adjustedPrefix)
	} else {
		t.Logf("Upload path: correctly adjusted prefix from '%s' to '%s'", prefix, adjustedPrefix)
	}

	// Test 2: S3 single object to local directory - should append filename to local path
	s3Prefix := "path/to/file.txt"
	localDstDir := destDir

	// Simulate the logic from the main function for download path
	adjustedDst := localDstDir
	if !strings.HasSuffix(s3Prefix, "/") { // single object, not a prefix
		fi, err := os.Stat(localDstDir)
		if err == nil && fi.IsDir() {
			adjustedDst = filepath.Join(localDstDir, filepath.Base(s3Prefix))
		}
	}

	expectedDst := filepath.Join(destDir, "file.txt")
	if adjustedDst != expectedDst {
		t.Errorf("Expected adjusted destination to be %s, got %s", expectedDst, adjustedDst)
	} else {
		t.Logf("Download path: correctly adjusted destination from '%s' to '%s'", localDstDir, adjustedDst)
	}
}
