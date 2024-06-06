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
}

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
	return nil
}
