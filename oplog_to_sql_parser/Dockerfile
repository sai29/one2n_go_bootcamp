# ---- builder ----
FROM golang:1.23.3 AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux \
    go build -ldflags="-s -w" -trimpath -o oplog-parser ./cmd/parser

# ---- prod runtime (distroless) ----

FROM gcr.io/distroless/base-debian12 AS prod-run

WORKDIR /app

COPY --from=builder /app/oplog-parser .

ENTRYPOINT ["./oplog-parser"]