APP_NAME := tabellarius
CLI_NAME := tabellarius-cli

BIN_DIR := bin
CMD_DIR := cmd

GO := go
GOFLAGS := -v

.PHONY: all
all: build

.PHONY: build
build: build-cli build-source

.PHONY: build-cli
build-cli:
	@echo ">> build cli"
	$(GO) build $(GOFLAGS) -o $(BIN_DIR)/$(CLI_NAME) ./$(CMD_DIR)/cli

.PHONY: build-source
build-source:
	@echo ">> build source"
	$(GO) build $(GOFLAGS) -o $(BIN_DIR)/$(APP_NAME) ./$(CMD_DIR)/server

.PHONY: run
run:
	$(GO) run ./$(CMD_DIR)/server

.PHONY: run-cli
run-cli:
	$(GO) run ./$(CMD_DIR)/cli

.PHONY: test
test:
	$(GO) test ./...

.PHONY: lint
lint:
	golangci-lint run

.PHONY: clean
clean:
	rm -rf $(BIN_DIR)

.PHONY: docker-build
docker-build:
	docker build -t $(APP_NAME):latest .

.PHONY: docker-up
docker-up:
	docker-compose up -d

.PHONY: docker-down
docker-down:
	docker-compose down
