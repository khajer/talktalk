.PHONY: build release run test lint fmt docker-up docker-down podman-up podman-down

build:
	cargo build

release:
	cargo build --release

run:
	cargo run

test:
	cargo test

lint:
	cargo clippy

fmt:
	cargo fmt

docker-up:
	docker-compose up --build

docker-down:
	docker-compose down

podman-up:
	podman-compose up --build

podman-down:
	podman-compose down
