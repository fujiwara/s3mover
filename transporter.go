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
	// DefaultMaxParallels は、S3への転送処理の最大同時実行数デフォルト値です
	DefaultMaxParallels = 1

	// RetryWait は、S3への転送処理のリトライ間隔です
	RetryWait = time.Second

	// DiskUsageThreshold は、ディスク使用量がこの閾値を超えた場合に.stopファイルを作成します
	DiskUsageThreshold = 0.8
)

var (
	TZ *time.Location
)

func init() {
	if jst, err := time.LoadLocation("Asia/Tokyo"); err != nil {
		panic(err)
	} else {
		TZ = jst
	}
	slog.SetDefault(slog.New(slogcontext.NewHandler(slog.NewJSONHandler(os.Stdout, nil))))
}

// bytes.Bufferのpool
// 何度もbytes.Bufferを作成するのは非効率なのでsync.Poolで使い回す
var pool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// getBufferは、bytes.Bufferをpoolから取得します
// 2番目の返値は、bytes.Bufferをpoolに戻すための関数です
func getBufferFromPool() (*bytes.Buffer, func()) {
	buf := pool.Get().(*bytes.Buffer)
	return buf, func() {
		buf.Reset()
		pool.Put(buf)
	}
}

type S3Client interface {
	PutObject(ctx context.Context, input *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

type Transporter struct {
	s3        S3Client
	config    *Config
	sem       *semaphore.Weighted
	startFile string
	stopFile  string
	metrics   *Metrics
	now       func() time.Time
}

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
		now:       time.Now,
	}
	if err := tr.init(); err != nil {
		return nil, err
	}
	return tr, nil
}

func (tr *Transporter) Run(ctx context.Context) error {
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

// SrcDirが存在して、そこにファイルの書き込みと削除ができるかを確認する処理
func (tr *Transporter) init() error {
	// os.Stat はファイルやディレクトリが存在するかを確認するための手段
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
	return nil
}

// sleepは、指定した時間だけsleepします
// time.Sleepと違うのはctxがキャンセルされたときにsleepを中断することです
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
		if processed == total {
			slog.InfoContext(ctx, "succeeded to transport all files",
				slog.Int64("processed", processed),
				slog.Int64("total", total),
			)
		} else {
			slog.WarnContext(ctx, "succeeded to transport some files, but some files are remaining",
				slog.Int64("processed", processed),
				slog.Int64("total", total),
			)
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
		tr.sem.Acquire(ctx, 1) // セマフォを1つ取得してprocessをはじめる並列度をコントロール
		go func() {
			defer tr.sem.Release(1) // セマフォを解放
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
	wg.Wait() // 全てのprocessが終わるまで待ち合わせる
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
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// gzip圧縮するためのbufferをpoolから取得
	buf, returnToPool := getBufferFromPool()
	defer returnToPool()                   // bufferをpoolに戻す
	gw, err := gzip.NewWriterLevel(buf, 1) // gzip圧縮レベル1=低圧縮だけど高速
	if err != nil {
		return err
	}
	// gzip圧縮してbufに書き込む。オンメモリになるけどせいぜい1MB程度なので問題ない
	if _, err := io.Copy(gw, f); err != nil {
		return err
	}
	gw.Close()

	key := genKey(tr.config.KeyPrefix, filepath.Base(path), tr.now())
	slog.DebugContext(ctx, "uploading",
		"s3url", fmt.Sprintf("s3://%s/%s", tr.config.Bucket, key),
		slog.Int("size", buf.Len()),
	)
	if _, err := tr.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        &tr.config.Bucket,
		Key:           &key,
		Body:          bytes.NewReader(buf.Bytes()),
		ContentLength: aws.Int64(int64(buf.Len())),
	}); err != nil {
		return fmt.Errorf("failed to put object: %w", err)
	}
	slog.DebugContext(ctx, "uploaded successfully",
		"s3url", fmt.Sprintf("s3://%s/%s", tr.config.Bucket, key),
		slog.Int("size", buf.Len()),
	)
	return nil
}

func genKey(prefix, name string, ts time.Time) string {
	return filepath.Join(prefix, ts.Format("2006/01/02/15/04"), name) + ".gz"
}

func listFiles(dir string) ([]string, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, file := range files {
		// ディレクトリと dot file は無視。サブディレクトリは辿らない
		if file.IsDir() || strings.HasPrefix(file.Name(), ".") {
			continue
		}
		paths = append(paths, filepath.Join(dir, file.Name()))
	}
	return paths, nil
}
