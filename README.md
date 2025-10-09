# s3cp

A fast, concurrent CLI utility for copying files and directories between local paths and S3 buckets, inspired by `aws s3 cp` but keep it simple and standalone.

## Installation

```
go install github.com/JackKCWong/s3cp@latest
```

## Usage

```
s3cp [flags] <src> <dst>
```

- Either `<src>` or `<dst>` must be an S3 URL of the form `s3://bucket/prefix`.
- The other must be a local file or directory path.
- When the source is a file and the destination is a directory, the filename is automatically appended to the destination path.

### Examples

**Download from S3 to local:**
```
s3cp s3://my-bucket/data/ ./localdir
```

**Upload from local to S3:**
```
s3cp ./localdir s3://my-bucket/data/
```

**Download with glob patterns:**
```
# Download all CSV files from a directory
s3cp s3://my-bucket/data/*.csv ./localdir

# Download all gzipped CSV files from a directory
s3cp s3://my-bucket/data/*.csv.gz ./localdir

# Download all files with a specific pattern from subdirectories
s3cp s3://my-bucket/data/**/*.csv.gz ./localdir
```

**Automatic filename handling:**
```
# When downloading a specific S3 object to a local directory, the filename is automatically added:
s3cp s3://my-bucket/file.txt ./mydir/    # Downloads to ./mydir/file.txt

# When uploading a local file to an S3 directory (prefix ending with /), the filename is automatically added:
s3cp ./myfile.txt s3://my-bucket/data/  # Uploads to s3://my-bucket/data/myfile.txt
```

### Flags

- `-c <int>`: Set concurrency (default: 4 × number of CPU cores)

## Features
- Upload local files/directories to S3
- Download S3 prefixes to local directories
- Download with glob pattern matching (e.g., `s3cp s3://bucket/dir/*.txt localdir`)
- Highly concurrent (default: 4 × CPU cores, configurable)
- Simple, modern logging with timestamps and levels
- Uses AWS SDK v2 for Go

## Requirements
- Go 1.20+
- AWS credentials configured (via environment, config file, or IAM role)

## Logging
- Uses [tint](https://github.com/lmittmann/tint) for pretty, leveled logs.
- Logs include timestamps, log level, and operation details.

## How it works
- For uploads, recursively walks the local directory and uploads files to S3 with the same relative path.
- For downloads, lists all objects under the given S3 prefix and downloads them to the local directory, preserving structure.
- Uses a weighted semaphore to limit concurrency.

## License

MIT
