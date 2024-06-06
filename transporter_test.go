package s3mover_test

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fujiwara/s3mover"
)

var testFileNames = []string{"foo", "foo.txt", ".foo.bar", "bar", "bar.txt"}
var now = time.Date(2022, time.January, 2, 3, 4, 5, 6, s3mover.TZ)

var testKeys = []struct {
	prefix string
	name   string
	key    string
}{
	{"", "foo", "2022/01/02/03/04/foo.gz"},
	{"xxx", "foo", "xxx/2022/01/02/03/04/foo.gz"},
}

func TestGenKey(t *testing.T) {
	for _, p := range testKeys {
		key := s3mover.GenKey(p.prefix, p.name, now)
		if key != p.key {
			t.Errorf("expected %s, got %s", p.key, key)
		}
	}
}

func TestListFiles(t *testing.T) {
	files, err := s3mover.ListFiles("./testdata")
	if err != nil {
		t.Error(err)
	}
	if len(files) != 2 {
		t.Errorf("expected 2 files, got %d", len(files))
	}
	for _, f := range files {
		if strings.HasPrefix(f, "testdata/.") {
			t.Errorf("dot files must not be included. %s", f)
		}
	}
	t.Log(files)
}

func TestRun(t *testing.T) {
	client := s3mover.NewMockS3Client()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr, err := s3mover.New(ctx, &s3mover.Config{
		SrcDir:       "./testdata/testrun",
		Bucket:       "testbucket",
		KeyPrefix:    "test/run",
		MaxParallels: 2,
	})
	if err != nil {
		t.Error(err)
	}
	tr.SetMockS3(client)
	tr.SetMockTime(now)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i, name := range testFileNames {
			if i%3 == 0 {
				time.Sleep(time.Millisecond * 500)
			}
			f, _ := os.Create("./testdata/testrun/" + name)
			f.WriteString(strings.Repeat(name, 1024))
			f.Close()
		}
		time.Sleep(time.Second * 5) // monitorが一回動くのを待つ
		cancel()
	}()
	go func() {
		defer wg.Done()
		if err := tr.Run(ctx); err != nil {
			t.Error(err)
		}
	}()
	wg.Wait()

	if len(client.Objects) != 4 {
		t.Error("expected 4 uploaded objects, got", len(client.Objects))
	}

	for _, name := range testFileNames {
		if strings.HasPrefix(name, ".") {
			if _, err := os.Stat("./testdata/testrun/" + name); err != nil {
				t.Error("dot files must not be removed. " + name)
			}
			if _, found := client.Objects["test/run/2022/01/02/03/04/"+name]; found {
				t.Error("dot files must not be uploaded. " + name)
			}
			continue
		}
		if _, err := os.Stat("./testdata/testrun/" + name); err == nil {
			t.Error("files must be removed. " + name)
		}
		if _, found := client.Objects["test/run/2022/01/02/03/04/"+name+".gz"]; !found {
			t.Error("files must be uploaded. " + name)
		}
	}

	m := tr.Metrics()
	if m.Objects.Uploaded != 4 {
		t.Error("expected 4 uploaded objects, got", m.Objects.Uploaded)
	}
	if m.Objects.Errored != 0 {
		t.Error("expected 0 errored objects, got", m.Objects.Errored)
	}
	t.Logf("%#v", m)
}
