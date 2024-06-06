package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/fatih/color"
	"github.com/fujiwara/logutils"
	"github.com/mattn/go-isatty"

	"github.com/fujiwara/s3mover"
)

func main() {
	if err := _main(); err != nil {
		log.Println("[error]", err)
		os.Exit(1)
	}
}

func _main() error {
	var logLevel string
	config := &s3mover.Config{}
	flag.StringVar(&config.SrcDir, "src", "", "source directory")
	flag.StringVar(&config.Bucket, "bucket", "", "S3 bucket name")
	flag.StringVar(&config.KeyPrefix, "prefix", "", "S3 key prefix")
	flag.Int64Var(&config.MaxParallels, "parallels", s3mover.DefaultMaxParallels, "max parallels")
	flag.StringVar(&logLevel, "log-level", "info", "log level")
	flag.IntVar(&config.StatsServerPort, "port", 9898, "stats server port")
	flag.VisitAll(overrideWithEnv) // 環境変数でflagの初期値をセットする処理
	flag.Parse()

	if !isatty.IsTerminal(os.Stdout.Fd()) {
		// 端末で実行されていない場合はログの色づけを無効にする
		color.NoColor = true
	}
	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"debug", "info", "warn", "error"},
		MinLevel: logutils.LogLevel(logLevel),
		ModifierFuncs: []logutils.ModifierFunc{
			nil, // default
			logutils.Color(color.FgBlue),
			logutils.Color(color.FgYellow),
			logutils.Color(color.FgRed, color.BgBlack),
		},
		Writer: os.Stderr,
	}
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetOutput(filter)

	log.Println("[info] starting up")
	if err := config.Validate(); err != nil {
		return err
	}
	log.Printf("[info] config: %#v", config)

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
	if s := os.Getenv("TRANSPORTER_" + name); s != "" {
		f.Value.Set(s)
	}
}
