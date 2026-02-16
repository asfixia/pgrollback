.PHONY: build test test-unit test-integration run clean

build:
	go build -o bin/pgrollback ./cmd/pgtest

test: test-unit test-integration

test-unit:
	go test -v ./pkg/... ./internal/...

test-integration:
	go test -v ./test/integration/... -tags=integration

run: build
	./bin/pgrollback

clean:
	rm -rf bin/
