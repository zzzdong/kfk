.PHONY: help build-all build-x86_64-musl build-aarch64-musl clean

TARGET_DIR := target
BINARY_NAME := kfk

help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@echo "  build-x86_64-musl    Build for x86_64-unknown-linux-musl"
	@echo "  build-aarch64-musl   Build for aarch64-unknown-linux-musl"
	@echo "  build-all            Build both musl targets"
	@echo "  clean                Clean build artifacts"

build-x86_64-musl:
	rustup target add x86_64-unknown-linux-musl
	cargo build --release --target x86_64-unknown-linux-musl
	@mkdir -p dist
	cp $(TARGET_DIR)/x86_64-unknown-linux-musl/release/$(BINARY_NAME) \
		dist/$(BINARY_NAME)-x86_64-unknown-linux-musl
	@echo "✓ dist/$(BINARY_NAME)-x86_64-unknown-linux-musl"

build-aarch64-musl:
	rustup target add aarch64-unknown-linux-musl
	cargo build --release --target aarch64-unknown-linux-musl
	@mkdir -p dist
	cp $(TARGET_DIR)/aarch64-unknown-linux-musl/release/$(BINARY_NAME) \
		dist/$(BINARY_NAME)-aarch64-unknown-linux-musl
	@echo "✓ dist/$(BINARY_NAME)-aarch64-unknown-linux-musl"

build-all: build-x86_64-musl build-aarch64-musl
	@echo "✓ All builds complete"

clean:
	cargo clean
	rm -rf dist
