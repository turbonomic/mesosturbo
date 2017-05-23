OUTPUT_DIR=./_output

build: clean
	go build -o ${OUTPUT_DIR}/mesosturbo ./cmd/

test: clean
	@go test -v -race ./pkg/...

.PHONY: clean
clean:
	@: if [ -f ${OUTPUT_DIR} ] then rm -rf ${OUTPUT_DIR} fi