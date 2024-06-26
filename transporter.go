package s3mover

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	slogcontext "github.com/PumpkinSeed/slog-context"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/semaphore"
)

const (
	// DefaultMaxParallels is the default value of the maximum number of concurrent executions of the transfer process to S3.
	DefaultMaxParallels = 1

	// RetryWait is the interval for retrying the transfer process to S3.
	RetryWait = time.Second

	// TestObjectKey is the key of the test object.
	TestObjectKey = ".s3mover-test-object"

	// DefaultTimeFormat is the default time format for the key of the object in S3.
	DefaultTimeFormat = "2006/01/02/15"
)

var (
	TZ *time.Location
)

func init() {
	TZ = time.Local
}

// pool of bytes.Buffer
// reuse buffer for gzip compression
var pool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func getBufferFromPool() (*bytes.Buffer, func()) {
	buf := pool.Get().(*bytes.Buffer)
	return buf, func() {
		buf.Reset()
		pool.Put(buf)
	}
}

// S3Client is an interface for the S3 client.
type S3Client interface {
	PutObject(ctx context.Context, input *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// Transporter represents a file transfer process to S3.
type Transporter struct {
	s3        S3Client
	config    *Config
	sem       *semaphore.Weighted
	startFile string
	stopFile  string
	metrics   *Metrics
}

// New creates a new Transporter.
func New(ctx context.Context, config *Config) (*Transporter, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	tr := &Transporter{
		s3:        s3.NewFromConfig(cfg),
		config:    config,
		sem:       semaphore.NewWeighted(config.MaxParallels),
		stopFile:  filepath.Join(config.SrcDir, ".stop"),
		startFile: filepath.Join(config.SrcDir, ".start"),
		metrics:   &Metrics{},
	}
	return tr, nil
}

// Run starts the Transporter.
func (tr *Transporter) Run(ctx context.Context) error {
	if err := tr.init(ctx); err != nil {
		return err
	}
	ctx = slogcontext.WithValue(ctx, "component", "transporter")
	slog.InfoContext(ctx, "starting up")
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := tr.run(ctx); err != nil && err != context.Canceled {
			slog.ErrorContext(ctx, err.Error())
		}
	}()
	go func() {
		defer wg.Done()
		if err := tr.runStatsServer(ctx); err != nil && err != context.Canceled {
			slog.ErrorContext(ctx, err.Error())
		}
	}()
	wg.Wait()
	slog.InfoContext(ctx, "shutdown")
	return nil
}

// init initializes the Transporter. checks the source directory and S3 bucket.
func (tr *Transporter) init(ctx context.Context) error {
	if s, err := os.Stat(tr.config.SrcDir); err != nil {
		return fmt.Errorf("failed to stat %s: %w", tr.config.SrcDir, err)
	} else if !s.IsDir() {
		return fmt.Errorf("%s is not a directory", tr.config.SrcDir)
	}
	if f, err := os.Create(tr.startFile); err != nil {
		return fmt.Errorf("failed to create %s: %w", tr.startFile, err)
	} else {
		f.Close()
		if err := os.Remove(tr.startFile); err != nil {
			return fmt.Errorf("failed to remove %s: %w", tr.startFile, err)
		}
	}

	// check if the bucket exists and the user has permission to write
	if _, err := tr.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        &tr.config.Bucket,
		Key:           aws.String(genKey(tr.config.KeyPrefix, TestObjectKey, time.Now(), false, tr.config.TimeFormat)),
		Body:          bytes.NewReader([]byte("test")),
		ContentLength: aws.Int64(4),
	}); err != nil {
		return fmt.Errorf("failed to put object to %s: %w", tr.config.Bucket, err)
	}
	return nil
}

// sleep sleeps for d duration. It differs from time.Sleep in that it interrupts sleep when ctx is canceled.
func (tr *Transporter) sleep(ctx context.Context, d time.Duration) {
	tm := time.After(d)
	select {
	case <-ctx.Done():
		return
	case <-tm:
	}
}

func (tr *Transporter) run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		processed, total, err := tr.runOnce(ctx)
		if err != nil {
			slog.WarnContext(ctx, fmt.Sprintf("retry after %s", RetryWait), "error", err.Error())
			tr.sleep(ctx, RetryWait)
			continue
		}
		if total == 0 {
			slog.DebugContext(ctx, "no files to upload")
			tr.sleep(ctx, RetryWait)
			continue
		}
		if processed > 0 && processed == total {
			slog.InfoContext(ctx, "succeeded to transport all files",
				slog.Int64("processed", processed),
				slog.Int64("total", total),
			)
		} else {
			slog.WarnContext(ctx, "some files are remaining",
				slog.Int64("processed", processed),
				slog.Int64("total", total),
			)
			tr.sleep(ctx, RetryWait)
		}
	}
}

func (tr *Transporter) runOnce(ctx context.Context) (int64, int64, error) {
	paths, err := listFiles(tr.config.SrcDir)
	if err != nil {
		return 0, 0, err
	}
	if len(paths) == 0 {
		// no need to process
		return 0, 0, nil
	}

	total := int64(len(paths))
	var processed int64
	var wg sync.WaitGroup
	for _, path := range paths {
		path := path
		wg.Add(1)
		tr.sem.Acquire(ctx, 1)
		go func() {
			defer tr.sem.Release(1)
			defer wg.Done()
			if err := tr.process(ctx, path); err != nil {
				tr.metrics.PutObject(false)
				slog.WarnContext(ctx, err.Error())
			} else {
				tr.metrics.PutObject(true)
				atomic.AddInt64(&processed, 1)
			}
		}()
	}
	wg.Wait()
	return processed, total, nil
}

func (tr *Transporter) process(ctx context.Context, path string) error {
	slog.DebugContext(ctx, "processing", "path", path)
	if err := tr.upload(ctx, path); err != nil {
		return fmt.Errorf("failed to upload %s: %w", path, err)
	}
	slog.DebugContext(ctx, "uploaded successfully", "path", path)
	slog.DebugContext(ctx, "removing...", "path", path)
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("failed to remove file %s: %w", path, err)
	}
	slog.DebugContext(ctx, "removed successfully", "path", path)
	return nil
}

func (tr *Transporter) upload(ctx context.Context, path string) error {
	body, length, ts, err := loadFile(path, tr.config.Gzip, tr.config.GzipLevel)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer body.Close()
	key := genKey(tr.config.KeyPrefix, filepath.Base(path), ts, tr.config.Gzip, tr.config.TimeFormat)

	slog.DebugContext(ctx, "uploading",
		"s3url", fmt.Sprintf("s3://%s/%s", tr.config.Bucket, key),
		slog.Int64("size", length),
	)
	if _, err := tr.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        &tr.config.Bucket,
		Key:           &key,
		Body:          body,
		ContentLength: aws.Int64(length),
	}); err != nil {
		return fmt.Errorf("failed to put object: %w", err)
	}
	slog.InfoContext(ctx, "upload completed",
		"s3url", fmt.Sprintf("s3://%s/%s", tr.config.Bucket, key),
		slog.Int64("size", length),
	)
	return nil
}

func genKey(prefix, name string, ts time.Time, gz bool, format string) string {
	if format == "" {
		format = DefaultTimeFormat
	}
	key := filepath.Join(prefix, ts.In(TZ).Format(format), name)
	if gz {
		return key + ".gz"
	}
	return key
}

func loadFile(path string, gz bool, gzipLevel int) (io.ReadCloser, int64, time.Time, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, time.Time{}, err
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, 0, time.Time{}, err
	}

	var length int64
	var body io.ReadCloser
	if gz {
		buf, returnToPool := getBufferFromPool()
		defer returnToPool() // bufferをpoolに戻す
		gw, err := gzip.NewWriterLevel(buf, gzipLevel)
		if err != nil {
			return nil, 0, time.Time{}, err
		}
		if _, err := io.Copy(gw, f); err != nil {
			return nil, 0, time.Time{}, err
		}
		gw.Close()
		length = int64(buf.Len())
		body = io.NopCloser(bytes.NewReader(buf.Bytes()))
	} else {
		body = f
		length = stat.Size()
	}
	return body, length, stat.ModTime(), nil
}

func listFiles(dir string) ([]string, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, file := range files {
		// ignore directories and hidden files
		if file.IsDir() || strings.HasPrefix(file.Name(), ".") {
			continue
		}
		paths = append(paths, filepath.Join(dir, file.Name()))
	}
	return paths, nil
}
