package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	slogcontext "github.com/PumpkinSeed/slog-context"
	"github.com/fujiwara/s3mover"
	"github.com/mattn/go-isatty"
)

func main() {
	if err := _main(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
	slog.Info("s3mover stopped")
}

func _main() error {
	var debug bool
	config := &s3mover.Config{}
	flag.StringVar(&config.SrcDir, "src", "", "source directory")
	flag.StringVar(&config.Bucket, "bucket", "", "S3 bucket name")
	flag.StringVar(&config.KeyPrefix, "prefix", "", "S3 key prefix")
	flag.Int64Var(&config.MaxParallels, "parallels", s3mover.DefaultMaxParallels, "max parallels")
	flag.BoolVar(&config.Gzip, "gzip", false, "gzip compress")
	flag.IntVar(&config.GzipLevel, "gzip-level", 6, "gzip compress level (1-9)")
	flag.BoolVar(&debug, "debug", false, "debug mode")
	flag.IntVar(&config.StatsServerPort, "port", 9898, "stats server port")
	flag.VisitAll(overrideWithEnv) // 環境変数でflagの初期値をセットする処理
	flag.Parse()

	var h slog.Handler
	logLevel := slog.LevelInfo
	if debug {
		logLevel = slog.LevelDebug
	}
	if isatty.IsTerminal(os.Stdout.Fd()) {
		h = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	} else {
		h = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	}
	slog.SetDefault(slog.New(slogcontext.NewHandler(h)))

	slog.Info("starting up s3mover")
	if err := config.Validate(); err != nil {
		return err
	}
	slog.Info("configurations loaded", "config", config)

	// シグナルを受けたときにcancelされるctx
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)
	defer stop()
	tr, err := s3mover.New(ctx, config)
	if err != nil {
		return err
	}
	return tr.Run(ctx) // ctxがcancelされるまで帰ってこない
}

// overrideWithEnv flagの値を環境変数から取得して上書きする
func overrideWithEnv(f *flag.Flag) {
	name := strings.ToUpper(f.Name)
	name = strings.Replace(name, "-", "_", -1)
	if s := os.Getenv("S3MOVER_" + name); s != "" {
		f.Value.Set(s)
	}
}
