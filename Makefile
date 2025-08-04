.PHONY: build run stop clean docker-build docker-run

build:
	go build -o CryptoFlow ./cmd/CryptoFlow

run:
	SYMBOLS=BTCUSDT,ETHUSDT,XRPUSDT ./CryptoFlow

stop:
	docker stop CryptoFlow_collector || true

clean:
	rm -rf CryptoFlow data

docker-build:
	docker build -t CryptoFlow .

docker-run:
	docker run --rm \
		--env-file .env \
		-p 2112:2112 \
		-v $(PWD)/data:/app/data \
		CryptoFlow
