FROM golang:1.22-alpine

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o cryptoflow .

VOLUME ["/app/data"]
ENV LOG_LEVEL=INFO
CMD ["./CryptoFlow"]
