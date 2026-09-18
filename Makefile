.PHONY: build run test clippy docker-up docker-down demo fmt clean

build:
	cargo build --release

run:
	cargo run --release

test:
	cargo test

clippy:
	cargo clippy --all-targets -- -D warnings

fmt:
	cargo fmt

docker-up:
	docker compose up --build

docker-down:
	docker compose down

demo:
	./scripts/demo.sh

clean:
	cargo clean
