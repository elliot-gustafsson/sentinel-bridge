.PHONY:	build
SHELL := /bin/bash
MAKEFLAGS += --no-print-directory

build:
	cargo build --release

localrun:
	cargo run

test:
	cargo test

container:
ifndef TAG
	$(error TAG is required. Use: make container TAG=<tag>)
endif
	$(shell command -v podman &>/dev/null && echo "podman" || echo "docker") build -t $(TAG) -f Containerfile .
