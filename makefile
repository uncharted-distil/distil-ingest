version=0.1.0

.PHONY: all

all:
	@echo "make <cmd>"
	@echo ""
	@echo "commands:"
	@echo "  build         - build the source code"
	@echo "  lint          - lint the source code"
	@echo "  fmt           - format the source code"
	@echo "  test          - test the source code"
	@echo "  install       - install dependencies"

lint:
	@go vet ./...
	@go list ./... | grep -v /vendor/ | xargs -L1 golint

fmt:
	@go fmt ./...

build: lint
	@go build ./...

test: build
	@go test ./... 

install:
	@go get -u github.com/golang/lint/golint
	@go get -u github.com/golang/dep/cmd/dep
	@dep ensure
