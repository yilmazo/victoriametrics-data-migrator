# Build stage
FROM golang:1.26-alpine AS builder

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build migrator
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-X main.version=e2e" \
    -o /bin/vm-migrator ./cmd/vm-migrator/

# Build datagen
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/datagen ./e2e/datagen/

# Runtime stage
FROM alpine:3.20
RUN apk add --no-cache ca-certificates curl
COPY --from=builder /bin/vm-migrator /usr/local/bin/
COPY --from=builder /bin/datagen /usr/local/bin/
