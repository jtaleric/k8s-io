# K8s-IO Benchmark Tool Makefile

# Variables
BINARY_NAME=k8s-io
GO_VERSION=1.21
PACKAGE=github.com/jtaleric/k8s-io

# Default target
.PHONY: all
all: build

# Build the application
.PHONY: build
build:
	@echo "Building $(BINARY_NAME)..."
	go build -o $(BINARY_NAME) .

# Build for multiple platforms
.PHONY: build-all
build-all:
	@echo "Building for multiple platforms..."
	GOOS=linux GOARCH=amd64 go build -o $(BINARY_NAME)-linux-amd64 .
	GOOS=linux GOARCH=arm64 go build -o $(BINARY_NAME)-linux-arm64 .
	GOOS=darwin GOARCH=amd64 go build -o $(BINARY_NAME)-darwin-amd64 .
	GOOS=darwin GOARCH=arm64 go build -o $(BINARY_NAME)-darwin-arm64 .

# Clean build artifacts
.PHONY: clean
clean:
	@echo "Cleaning build artifacts..."
	rm -f $(BINARY_NAME)*

# Install dependencies
.PHONY: deps
deps:
	@echo "Installing dependencies..."
	go mod tidy
	go mod download

