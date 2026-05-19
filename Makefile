BINARY_NAME = walveil
MODULE = github.com/sudesh856/walveil
BUILD_DIR = bin
CMD_DIR = ./cmd/walveil


.PHONY: build test test-pg lint vet docker release gen-certs clean

build:
	mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 go build -ldflags="-s -w" -o $(BUILD_DIR)/$(BINARY_NAME) $(CMD_DIR)

test:
	go test ./...

test-pg:
	bash scripts/test-pg.sh

lint:
	golangci-lint run ./...

vet:
	go vet ./...

docker:
	docker build -t $(BINARY_NAME):latest .

release:
	mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
		-ldflags="-s -w -X main.version=$(shell git describe --tags --always)" \
		-o $(BUILD_DIR)/$(BINARY_NAME) -linux-amd64 $(CMD_DIR)


gen-certs:
	bash scripts/gen-certs.sh

clean:
	rm -rf $(BUILD_DIR)
