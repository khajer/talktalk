.PHONY: build release run test lint fmt

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
