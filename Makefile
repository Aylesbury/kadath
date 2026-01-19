lint:
			golangci-lint run

# Requires:
# go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
# go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
gen-proto:
	protoc --go_out=./gen/ --go_opt=paths=source_relative \
    --go-grpc_out=./gen/ --go-grpc_opt=paths=source_relative \
    proto/*.proto
