GO             := go
GOLINT         := golangci-lint
THRIFT         := thrift
TARGET_DIR     ?= ./example/bin
MAINMODULE     := github.com/alex-cos/thriftobs
GOOS           := $(shell go env GOOS)
BINARY_EXT     := ""
TEST_TIMEOUT   ?= 900s

ifeq ($(GOOS),windows)
	BINARY_EXT = ".exe"
	GO = go.exe
	THRIFT = thrift.exe
endif

LDFLAGS := -s -w
VENDOR := vendor/modules.txt
IDL := ./example/*.thrift
SERVICES := ./example/gen/.done

default: build

$(VENDOR):
	$(GO) mod vendor

$(SERVICES): $(VENDOR) $(IDL)
	@mkdir -p ./example/gen 2>/dev/null
	@for i in $(IDL); do \
		echo "Generating $$i ..."; \
		$(THRIFT) -r -gen \
			go:package_prefix="$(MAINMODULE)/,skip_remote" \
			-out "./example/gen" \
			"./$$i"; \
	done;
	@touch $(SERVICES)

.PHONY: build
build: $(VENDOR) $(SERVICES)
	$(GO) build \
		-ldflags '$(LDFLAGS)' \
		-o "$(TARGET_DIR)/client$(BINARY_EXT)" \
		"./example/client";
	$(GO) build \
		-ldflags '$(LDFLAGS)' \
		-o "$(TARGET_DIR)/server$(BINARY_EXT)" \
		"./example/server";

.PHONY: test
test: $(VENDOR) $(SERVICES)
	$(GO) test ./...

.PHONY: test-short
test-short: $(VENDOR)
	$(GO) test \
		-short \
		-timeout $(TEST_TIMEOUT) \
		./...

.PHONY: test-cover
test-cover: $(VENDOR)
	mkdir -p ./tmp/coverage 2>/dev/null
	$(GO) test \
		-timeout $(TEST_TIMEOUT) \
		-coverprofile tmp/coverage/coverage.out \
		-covermode=count \
		-json \
		./... 1>tmp/coverage/report.json \
		|| true
	$(GO) tool cover \
		-html tmp/coverage/coverage.out \
		-o tmp/coverage/coverage.html \
		|| true

.PHONY: lint
lint: $(VENDOR) $(SERVICES)
	$(GOLINT) run \
		--issues-exit-code=0 \
		--output.checkstyle.path=stdout \
		--show-stats=false \
		./...

.PHONY: print-%
print-%:
	@echo '$($*)'
