FROM --platform=${BUILDPLATFORM} golang:1.22.4-bookwarm AS build-env

ARG TARGETOS
ARG TARGETARCH

ENV GOOS=${TARGETOS}
ENV GOARCH=${TARGETARCH}
ENV CGO_ENABLED=0
ENV GO111MODULE=on
RUN mkdir -p /go/src/github.com/fujiwara/s3mover
COPY . /go/src/github.com/fujiwara/s3mover
WORKDIR /go/src/github.com/fujiwara/s3mover
RUN make

FROM alpine:3.19

RUN apk --no-cache add ca-certificates
COPY --from=build-env /go/src/github.com/fujiwara/s3mover /usr/bin
VOLUME [ "/tmp/s3mover" ]
WORKDIR /
