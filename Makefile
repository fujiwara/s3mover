LATEST_TAG := $(shell git describe --abbrev=0 --tags)
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

docker-build-and-push: Dockerfile
	go mod vendor
	docker buildx build \
		--build-arg version=${LATEST_TAG} \
		--platform=linux/amd64,linux/arm64 \
		-t ghcr.io/fujiwara/s3mover:${LATEST_TAG} \
		-f Dockerfile \
		--push \
		.
