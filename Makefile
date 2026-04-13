.PHONY: build test test-unit test-integration run clean

# When building for Windows (host GOOS), default to static MinGW CGO so the
# binary does not need libwinpthread-1.dll beside the exe. Faster dev links:
#   make build DYNAMIC=1
GOOS := $(shell go env GOOS)
ifeq ($(GOOS),windows)
ifeq ($(DYNAMIC),1)
WINDOWS_CGO_PREFIX :=
else
WINDOWS_CGO_PREFIX := CGO_LDFLAGS=-static
endif
else
WINDOWS_CGO_PREFIX :=
endif

build:
	$(WINDOWS_CGO_PREFIX) go build -o bin/pgrollback ./cmd/pgrollback

test: test-unit test-integration

test-unit:
	$(WINDOWS_CGO_PREFIX) go test -v ./pkg/... ./internal/...

test-integration:
	$(WINDOWS_CGO_PREFIX) go test -v ./test/integration/... -tags=integration

run: build
	./bin/pgrollback

clean:
	rm -rf bin/
