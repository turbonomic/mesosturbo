OUTPUT_DIR=./_output

SOURCE_DIRS = cmd pkg vendor
PACKAGES := go list ./... | grep -v /vendor | grep -v /out

build: clean
	go build -o ${OUTPUT_DIR}/mesosturbo ./cmd/

test: clean
	@go test -v -race ./pkg/...

.PHONY: clean
clean:
	@: if [ -f ${OUTPUT_DIR} ] then rm -rf ${OUTPUT_DIR} fi

.PHONY: fmtcheck
fmtcheck:
	@gofmt -l $(SOURCE_DIRS) | grep ".*\.go"; if [ "$$?" = "0" ]; then exit 1; fi

.PHONY: vet
vet:
	@go vet $(shell $(PACKAGES))
