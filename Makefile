.PHONY: help build-all build-x86_64-musl build-aarch64-musl package clean

TARGET_DIR := target
BINARY_NAME := kfk
DIST_DIR := dist
VERSION := $(shell grep '^version' Cargo.toml | head -1 | cut -d'"' -f2)

package: build-all
	@mkdir -p $(DIST_DIR)
	@echo "Creating packages..."
	@tar czf $(DIST_DIR)/$(BINARY_NAME)-$(VERSION)-x86_64-unknown-linux-musl.tar.gz -C $(TARGET_DIR)/x86_64-unknown-linux-musl/release $(BINARY_NAME)
	@tar czf $(DIST_DIR)/$(BINARY_NAME)-$(VERSION)-aarch64-unknown-linux-musl.tar.gz -C $(TARGET_DIR)/aarch64-unknown-linux-musl/release $(BINARY_NAME)
	@cd $(DIST_DIR) && sha256sum *.tar.gz > checksums.txt
	@echo "✓ Packages created in $(DIST_DIR)/"
	@ls -lh $(DIST_DIR)/

build-x86_64-musl:
	cargo build --release --target x86_64-unknown-linux-musl

build-aarch64-musl:
	cargo build --release --target aarch64-unknown-linux-musl

build-all: build-x86_64-musl build-aarch64-musl
	@echo "✓ All builds complete"

help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@echo "  build-x86_64-musl    Build for x86_64-unknown-linux-musl"
	@echo "  build-aarch64-musl   Build for aarch64-unknown-linux-musl"
	@echo "  build-all            Build both musl targets"
	@echo "  package (default)    Create release packages (tar.gz + checksums)"
	@echo "  clean                Clean build artifacts"

clean:
	cargo clean
	rm -rf $(DIST_DIR)
