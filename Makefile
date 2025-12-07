SHELL := /bin/sh
GOCACHE ?= $(CURDIR)/.gocache

.PHONY: test race

# Full test suite with race detector.
test: race

# Run tests with race detection and without caching.
race:
	GOCACHE=$(GOCACHE) go test -race ./...
