package s3mover_test

import (
	"compress/gzip"
	"context"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fujiwara/s3mover"
	"github.com/samber/lo"
)

var testFileNames = []string{"foo", "foo.txt", ".foo.bar", "bar", "bar.txt"}
var now = time.Date(2022, time.January, 2, 3, 4, 5, 6, s3mover.TZ)

var testKeys = []struct {
	prefix string
	name   string
	key    string
	gz     bool
}{
	{"", "foo", "2022/01/02/03/04/foo", false},
	{"xxx", "foo", "xxx/2022/01/02/03/04/foo", false},
	{"yyy/zzz", "bar.txt", "yyy/zzz/2022/01/02/03/04/bar.txt", false},
	{"", "foo", "2022/01/02/03/04/foo.gz", true},
	{"xxx", "foo", "xxx/2022/01/02/03/04/foo.gz", true},
	{"yyy/zzz", "bar.txt", "yyy/zzz/2022/01/02/03/04/bar.txt.gz", true},
}

func TestGenKey(t *testing.T) {
	for _, p := range testKeys {
		key := s3mover.GenKey(p.prefix, p.name, now, p.gz)
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
	if len(files) != 3 {
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
	testRun(t, false)
	testRun(t, true)
}

func testRun(t *testing.T, gzip bool) {
	client := s3mover.NewMockS3Client()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	config := &s3mover.Config{
		SrcDir:       "./testdata/testrun",
		Bucket:       "testbucket",
		KeyPrefix:    "test/run",
		MaxParallels: 2,
		Gzip:         gzip,
	}
	if err := config.Validate(); err != nil {
		t.Error(err)
	}

	tr, err := s3mover.New(ctx, config)
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
		time.Sleep(time.Second * 2)
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
		t.Errorf("expected 4 uploaded objects, got %v", lo.Keys(client.Objects))
	}

	var suffix string
	if gzip {
		suffix += ".gz"
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
		if _, found := client.Objects["test/run/2022/01/02/03/04/"+name+suffix]; !found {
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

func TestLoadFileRaw(t *testing.T) {
	body, size, err := s3mover.LoadFile("./testdata/raw.txt", false, 0)
	if err != nil {
		t.Error(err)
	}
	defer body.Close()
	if size != 401 {
		t.Errorf("expected size 401, got %d", size)
	}
	content, err := io.ReadAll(body)
	if err != nil {
		t.Error(err)
	}
	if len(content) != 401 {
		t.Errorf("expected content length 401, got %d", len(content))
	}
}

func TestLoadFileGz(t *testing.T) {
	body, size, err := s3mover.LoadFile("./testdata/raw.txt", true, 6)
	if err != nil {
		t.Error(err)
	}
	defer body.Close()
	if size >= 401 {
		t.Errorf("expected size reduced, got %d", size)
	}
	r, err := gzip.NewReader(body)
	if err != nil {
		t.Error(err)
	}
	content, err := io.ReadAll(r)
	if err != nil {
		t.Error(err)
	}
	if len(content) != 401 {
		t.Errorf("expected content length 401, got %d", len(content))
	}
}
