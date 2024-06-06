.PHONY: clean test

s3mover: go.* *.go cmd/s3mover/*.go
	go build -o $@ cmd/s3mover/main.go

clean:
	rm -rf s3mover dist/

test:
	go test -v ./...

install:
	go install github.com/fujiwara/s3mover/cmd/s3mover

dist:
	goreleaser build --snapshot --rm-dist
