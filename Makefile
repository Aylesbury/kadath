lint:
	golangci-lint run

build-postgres:
	go build -tags postgres -o bin/agent cmd/agent/main.go

build-mysql:
	go build -tags mysql -o bin/agent cmd/agent/main.go

# Requires:
# go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
# go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
gen-proto:
	protoc --go_out=./gen/ --go_opt=paths=source_relative \
    --go-grpc_out=./gen/ --go-grpc_opt=paths=source_relative \
    proto/*.proto

test-postgres:
	go test -tags postgres -v ./tests/engine/ ./internal/engine/postgres

test-mysql:
	go test -tags mysql -v ./tests/engine/ ./internal/engine/mysql

unit-test: test-postgres test-mysql

