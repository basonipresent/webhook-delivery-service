# Portable across GNU make 3.81 (GnuWin32) and newer; recipes run under sh or cmd.exe.
# Recipes use only &&, || and ( ).

APP_PORT ?= 8080
BASE_URL ?= http://localhost:$(APP_PORT)
MAIN_PKG ?= .
PKG      ?= ./...

export APP_PORT
export BASE_URL
export MAIN_PKG
export CGO_ENABLED := 1

ifeq ($(OS),Windows_NT)
DEMO_BASH ?= C:/Program Files/Git/bin/bash.exe
else
DEMO_BASH ?= bash
endif

.PHONY: help run test test-race docker-up docker-down docker-reset docker-logs \
        docker-test-race test-integration integration-run demo

help:
	@echo run               Run the service locally
	@echo test              Unit tests with coverage
	@echo test-race         Unit tests with the race detector - needs gcc locally
	@echo docker-up         Build and start the service in Docker
	@echo docker-down       Stop and remove containers
	@echo docker-reset      Rebuild and recreate the container - resets in-memory state
	@echo docker-logs       Follow service logs
	@echo docker-test-race  Race tests inside a Go container - fallback if local -race fails
	@echo test-integration  Reset service, run integration tests, always tear down
	@echo demo              Run scripts/demo.sh against BASE_URL - use: make docker-up demo

run:
	go run $(MAIN_PKG)

test:
	go test -count=1 -cover $(PKG)

test-race:
	go test -race -count=1 $(PKG)

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down

docker-reset:
	docker compose up -d --build --force-recreate

docker-logs:
	docker compose logs -f app

docker-test-race:
	docker compose run --rm test go test -race -count=1 $(PKG)

test-integration: docker-reset
	"$(MAKE)" integration-run || ("$(MAKE)" docker-down && exit 1)
	"$(MAKE)" docker-down

integration-run:
	go test -tags=integration -count=1 ./integration/...

demo:
	"$(DEMO_BASH)" scripts/demo.sh