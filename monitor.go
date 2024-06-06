package s3mover

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync/atomic"
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

// mackerel-plugin-json でのメトリック取得用HTTP server
func (tr *Transporter) runStatsServer(ctx context.Context) error {
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
	log.Printf("[info] starting up stats server on %s", l.Addr())

	go func() {
		if err := srv.Serve(l); err != nil {
			select {
			case <-ctx.Done():
				// 既にcontextが終了しているなら正常終了なのでなにもしない
			default:
				log.Println("[error] failed to serve stats server", err)
			}
		}
	}()

	<-ctx.Done()
	log.Printf("[info] shutting down stats server")
	return srv.Shutdown(ctx)
}
