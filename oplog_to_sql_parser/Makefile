BINARY_NAME := oplog-parser
BUILD_DIR := bin

.PHONY: build test lint vet fmt clean run help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

build:
	go build -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/parser

