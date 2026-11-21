GO             := go
GOLINT         := golangci-lint
THRIFT         := thrift
TARGET_DIR     ?= ./example/bin
MAINMODULE     := github.com/alex-cos/thriftobs
GOOS           := $(shell go env GOOS)
GIT_TAG_NAME   ?= $(shell git describe --abbrev=1 --tags 2>/dev/null || git describe --always)
SHORT_TAG_NAME ?= $(shell git describe --abbrev=0 --tags 2>/dev/null | sed -rn 's/(v)?([0-9]+(.[0-9]+){2}).*/\2/p')
BRANCH_NAME    ?= $(shell git rev-parse --abbrev-ref HEAD)
BUILDDATE      ?= $(shell date '+%Y-%m-%d')
TEST_TIMEOUT   ?= 900s
BUILD_VERSION  ?= 0.0.0
BINARY_EXT     := ""

ifeq ($(BUILD_VERSION), 0.0.0)
	ifneq ($(SHORT_TAG_NAME),)
		BUILD_VERSION = $(SHORT_TAG_NAME)
	else 
	  BUILD_VERSION = 1.0.0
	endif
endif

ifeq ($(GOOS),windows)
	BINARY_EXT = ".exe"
endif

LDFLAGS := -s -w
VENDOR := vendor/modules.txt
IDL := ./example/*.thrift
SERVICES := ./example/gen/.done

default: build

.PHONY: clean
clean:
	$(GO) clean -i ./...
	rm -rf vendor 2>/dev/null
	rm -rf tmp 2>/dev/null
	rm -rf ./example/bin 2>/dev/null

.PHONY: install-tools
install-tools:
	$(GO) install golang.org/x/tools/cmd/godoc@latest
	$(GO) install github.com/go-delve/delve/cmd/dlv@latest
	$(GO) install github.com/go-critic/go-critic/cmd/gocritic@latest
	$(GO) install golang.org/x/tools/cmd/deadcode@latest
	$(GO) install github.com/chmouel/go-better-html-coverage@latest
	curl -sSfL https://golangci-lint.run/install.sh | \
		sh -s -- -b $$(go env GOPATH)/bin latest

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
	$(GO) build \
		-ldflags '$(LDFLAGS)' \
		-o "$(TARGET_DIR)/pool$(BINARY_EXT)" \
		"./example/pool";

.PHONY: lint
lint: $(VENDOR)
	mkdir -p ./tmp 2>/dev/null
	$(GOLINT) run \
	  --allow-parallel-runners \
		--output.text.path=stdout \
		--output.checkstyle.path=./tmp/lintreport.xml \
		--output.html.path=./tmp/lintreport.html \
		--show-stats=true \
		./...

.PHONY: test
test: $(VENDOR) $(SERVICES)
	$(GO) test \
		-timeout $(TEST_TIMEOUT) \
		$(MAINMODULE)

.PHONY: test-short
test-short: $(VENDOR)
	$(GO) test \
		-short \
		-timeout $(TEST_TIMEOUT) \
		$(MAINMODULE)

.PHONY: test-cover
test-cover: $(VENDOR)
	mkdir -p ./tmp 2>/dev/null
	$(GO) clean -testcache
	$(GO) test \
		-timeout $(TEST_TIMEOUT) \
		-coverprofile ./tmp/coverage.out \
		-covermode=count \
		-json \
		$(MAINMODULE)/... 1>./tmp/report.json
	go-better-html-coverage -n \
		-profile ./tmp/coverage.out \
		-o ./tmp/coverage.html

.PHONY: print-%
print-%:
	@echo '$($*)'
