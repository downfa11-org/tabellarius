FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -o cdc-cli ./cmd/cli

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -o cdc-server ./cmd/server

FROM alpine:3.19

WORKDIR /app

COPY --from=builder /app/cdc-cli /usr/local/bin/cdc-cli
COPY --from=builder /app/cdc-server /usr/local/bin/cdc-server
