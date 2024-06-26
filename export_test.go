package s3mover

import (
	"context"
	"io"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	ListFiles = listFiles
	GenKey    = genKey
	LoadFile  = loadFile
)

func (tr *Transporter) SetMockS3(client *MockS3Client) {
	tr.s3 = client
}

func NewMockS3Client() *MockS3Client {
	return &MockS3Client{
		mu:      sync.Mutex{},
		Objects: make(map[string]*MockS3Object),
	}
}

type MockS3Client struct {
	mu      sync.Mutex
	Objects map[string]*MockS3Object
}

type MockS3Object struct {
	Bucket  string
	Key     string
	Size    int64
	Content []byte
}

func (c *MockS3Client) PutObject(ctx context.Context, input *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if strings.Contains(*input.Key, TestObjectKey) {
		// ignore test object
		return &s3.PutObjectOutput{}, nil
	}

	b, _ := io.ReadAll(input.Body)
	obj := MockS3Object{
		Bucket:  *input.Bucket,
		Key:     *input.Key,
		Size:    *input.ContentLength,
		Content: b,
	}
	c.Objects[obj.Key] = &obj
	return &s3.PutObjectOutput{}, nil
}
