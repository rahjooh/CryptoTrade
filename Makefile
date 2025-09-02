.PHONY: build run stop clean docker-build docker-run

build:
	go build -o cryptoflow .

run:
	SYMBOLS=BTCUSDT,ETHUSDT,XRPUSDT ./CryptoFlow

stop:
	docker stop cryptoflow_collector || true

clean:
	rm -rf cryptoflow data

docker-build:
	docker build -t cryptoflow .

docker-run:
	docker run --rm \
		--env-file .env \
		-p 2112:2112 \
		-v $(PWD)/data:/app/data \
		cryptoflow
