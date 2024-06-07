package s3mover

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"

	slogcontext "github.com/PumpkinSeed/slog-context"
)

type Metrics struct {
	Objects struct {
		Uploaded int64 `json:"uploaded"`
		Errored  int64 `json:"errored"`
		Queued   int64 `json:"queued"`
	} `json:"objects"`
}

func (m *Metrics) PutObject(success bool) {
	if success {
		atomic.AddInt64(&m.Objects.Uploaded, 1)
	} else {
		atomic.AddInt64(&m.Objects.Errored, 1)
	}
}

func (m *Metrics) SetQueued(n int64) {
	atomic.StoreInt64(&m.Objects.Queued, n)
}

func (tr *Transporter) Metrics() *Metrics {
	return tr.metrics
}

func NewMetrics() *Metrics {
	return &Metrics{}
}

// HTTP server to serve metrics
func (tr *Transporter) runStatsServer(ctx context.Context) error {
	ctx = slogcontext.WithValue(ctx, "component", "stats-server")
	if tr.config.StatsServerPort == 0 {
		slog.InfoContext(ctx, "stats server is disabled")
		return nil
	}

	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-type", "application/json")
		enc := json.NewEncoder(w)
		if err := enc.Encode(tr.Metrics()); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/stats/metrics", handler)
	addr := fmt.Sprintf(":%d", tr.config.StatsServerPort)
	srv := &http.Server{
		Handler: mux,
		Addr:    addr,
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "starting up stats server", "listen", l.Addr().String())

	go func() {
		if err := srv.Serve(l); err != nil {
			select {
			case <-ctx.Done():
				// normal shutdown
			default:
				slog.ErrorContext(ctx, "failed to serve stats server", "error", err.Error())
			}
		}
	}()

	<-ctx.Done()
	slog.InfoContext(ctx, "shutting down stats server")
	return srv.Shutdown(ctx)
}
