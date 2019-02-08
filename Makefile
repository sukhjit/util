RELEASE?=0.0.1
COMMIT?=$(shell git rev-parse --short HEAD)

test:
	go test -race ./...
