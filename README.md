# s3mover

## Description

s3mover is a simple agent for moving local files to Amazon S3.

It moves files from a local directory to an S3 bucket and is designed to run as a daemon on a server. It monitors the local directory for new files and transfers them to the S3 bucket.

### Limitations

- s3mover does not support watching subdirectories, only the specified directory.
- It reads the file as soon as it is created, so the file must be completely written at that time.
- To avoid issues, write the file with a temporary name (starting with a dot) and rename it to the final name after the writing is complete.
- s3mover ignores files whose names begin with a dot (.).

## Installation

### Binary

[Releases](https://github.com/fujiwara/s3mover/releases)

### Docker

[ghcr.io/fujiwara/s3mover](https://github.com/fujiwara/s3mover/pkgs/container/s3mover)

```console
$ docker pull ghcr.io/fujiwara/s3mover:v0.0.3
```

## Usage

```console
Usage of s3mover:
  -bucket string
        S3 bucket name
  -debug
        debug mode
  -gzip
        gzip compress
  -gzip-level int
        gzip compress level (1-9) (default 6)
  -parallels int
        max parallels (default 1)
  -port int
        stats server port (default 9898)
  -prefix string
        S3 key prefix
  -src string
        source directory
  -time-format string
        time format (default "2006/01/02/15/04")
```

All flags accept environment variables with the prefix `S3MOVER_`. For example, the `-bucket` flag can be set with the `S3MOVER_BUCKET` environment variable.

The boolean flags can be set with `true`, `1`, `t`, `T`, `TRUE`, `True`, `false`, `0`, `f`, `F`, `FALSE`, or `False`. For example, the `-gzip` flag can be set with the `S3MOVER_GZIP=true` environment variable.

## Example

```console
$ s3mover -src /path/to/local -bucket mybucket -prefix myprefix/ -gzip
```

1. s3mover watches the `/path/to/local` directory.
2. When a new file is created in the directory, s3mover reads the file and uploads it to the mybucket bucket.
   - The S3 key is `myprefix/YYYY/MM/DD/HH/filename.gz`.
   - If the `-gzip` option is specified, the file is compressed with gzip before uploading.
3. s3mover removes the file from the local directory after the upload is completed.
4. s3mover repeats the above steps.

If any errors occur during the process, s3mover retries the process after 1 second.

## Configurations

### AWS Region

`AWS_REGION` is required to be set in the environment variables. The region is used to determine the endpoint of the S3 bucket.

### AWS Credentials

s3mover uses the AWS SDK Go v2, so you can use the same credentials as the SDK. Typically, you can use the following methods to set credentials:

- Environment variables
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `AWS_PROFILE`
- Shared credentials file
  - `~/.aws/credentials`
- IAM Instance Profile / ECS Task Role / Lambda Execution Role

### IAM Policy

s3mover requires the following permissions to work:
- `s3:PutObject`

### `-src`

The directory to watch for new files. This is required.

### `-bucket`

The name of the S3 bucket to upload files to. This is required.

### `-prefix`

The prefix of the S3 key. The S3 key is constructed as follows (this is required):

```
{prefix}/{time-format}/{filename}
```


`{time-format}` is formatted with the time the file was created.

### `-time-format`

The time format used in the S3 key. The default is `2006/01/02/15/04`, which is formatted as Go's [`time.Format`](https://pkg.go.dev/time#pkg-constants).

s3mover uses a local time to determine the time the file was created. If you want to use UTC, set the `TZ` environment variable to `UTC`.

### `-gzip`

If specified, the file is compressed with gzip before uploading.

Currently, s3mover compresses files in memory, so consider the memory usage when handling large files.

### `-gzip-level`

The gzip compression level. The default is 6. The level must be between 1 and 9.

### `-parallels`

The maximum number of parallel uploads. The default is 1.

### `-port`

The port number of the stats server. The stats server returns the number of objects uploaded, errored, and queued as JSON.

```console
$ curl -s localhost:9898/stats/metrics | jq .
{
  "objects": {
    "uploaded": 0,
    "errored": 0,
    "queued": 0
  }
}
```

- `objects.uploaded`: The number of objects uploaded to S3.
- `objects.errored`: The number of objects that failed to upload.
- `objects.queued`: The number of objects queued for upload.
  - This value indicates the number of files that are not uploaded in the local directory.
  - If the number increases, it may be a sign that the agent is not working properly.
  - If the number is always large, you may need to increase the number of parallels.

`-port=0` disables the stats server.

## LICENSE

MIT License

## Author

Fujiwara Shunichiro
