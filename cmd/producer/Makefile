REV?=HEAD
NAME:=$(shell go list)
VERSION:=$(shell git rev-list --count $(REV))-$(shell git rev-parse --short $(REV))-$(shell git rev-parse --abbrev-ref HEAD)
TIME:=$(shell date -u "+%Y-%m-%dT%H:%M:%SZ")
BIN:=producer

.PHONY: build
build:
	mkdir -p bin
	go build -o bin/$(BIN) -ldflags="-X 'main.projectName=$(NAME)' -X 'main.buildVersion=$(VERSION)' -X 'main.buildTime=$(TIME)'" .
