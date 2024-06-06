package s3mover_test

import (
	"sync"
	"testing"

	"github.com/fujiwara/s3mover"
)

func TestMetrics(t *testing.T) {
	m := s3mover.NewMetrics()
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.PutObject(true)
			m.PutObject(false)
		}()
	}
	wg.Wait()

	if m.Objects.Uploaded != 100 {
		t.Errorf("success: %d", m.Objects.Uploaded)
	}
	if m.Objects.Errored != 100 {
		t.Errorf("success: %d", m.Objects.Errored)
	}
	t.Log(m)
}
