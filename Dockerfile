FROM golang:1.22-alpine

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o CryptoFlow ./cmd/CryptoFlow

VOLUME ["/app/data"]
CMD ["./CryptoFlow"]
