# Engine selection via build arg: all (default, every engine — used by
# platform runners) or postgres|mysql for slim single-engine client images.
FROM golang:1.25-alpine AS build

ARG ENGINE=all

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -tags ${ENGINE} -o /out/agent cmd/agent/main.go

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S kadath && adduser -S kadath -G kadath

COPY --from=build /out/agent /usr/local/bin/agent

USER kadath

ENTRYPOINT ["/usr/local/bin/agent"]
