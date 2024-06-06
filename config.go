package s3mover

import (
	"errors"
)

type Config struct {
	SrcDir          string
	Bucket          string
	KeyPrefix       string
	MaxParallels    int64
	StatsServerPort int
	Gzip            bool
	GzipLevel       int
}

const DefaultGzipLevel = 6

func (c *Config) Validate() error {
	if c.Bucket == "" {
		return errors.New("bucket is required")
	}
	if c.KeyPrefix == "" {
		return errors.New("prefix is required")
	}
	if c.SrcDir == "" {
		return errors.New("src is required")
	}
	if c.Gzip {
		if c.GzipLevel == 0 {
			c.GzipLevel = DefaultGzipLevel
		}
		if c.GzipLevel < 1 || c.GzipLevel > 9 {
			return errors.New("gzip level must be between 1 and 9")
		}
	}
	return nil
}
