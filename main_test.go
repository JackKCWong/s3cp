package main

import (
	"fmt"
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