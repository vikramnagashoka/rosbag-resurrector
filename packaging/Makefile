# RosBag Resurrector — Package Build System
#
# Targets:
#   make build-base     Build base executable (current platform)
#   make macos          Build macOS DMG
#   make ubuntu         Build Ubuntu DEB
#   make obfuscate      Run Pyarmor obfuscation only
#   make clean          Clean build artifacts
#
# Prerequisites:
#   pip install pyinstaller pyarmor
#   macOS: brew install create-dmg
#   Ubuntu: sudo gem install fpm

.PHONY: all build-base macos ubuntu obfuscate clean

VERSION ?= 0.2.0
MKFILE_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
ROOT := $(abspath $(MKFILE_DIR)/..)
DIST := $(ROOT)/dist
SRC := $(ROOT)/resurrector
PKG := $(ROOT)/packaging

# Detect platform
UNAME := $(shell uname -s)

all:
ifeq ($(UNAME),Darwin)
	$(MAKE) macos
else ifeq ($(UNAME),Linux)
	$(MAKE) ubuntu
else
	@echo "Unsupported platform: $(UNAME). Use macOS or Linux."
	@exit 1
endif

# --- Obfuscation ---

obfuscate:
	@echo "=== Obfuscating source with Pyarmor ==="
	cd $(ROOT) && pyarmor gen \
		--output $(DIST)/obfuscated \
		--recursive \
		$(SRC)/
	@echo "=== Obfuscation complete ==="

# --- Base executable ---

build-base:
	@echo "=== Building base executable with PyInstaller ==="
	cd $(ROOT) && pyinstaller \
		--onefile \
		--name resurrector \
		--distpath $(DIST)/$(shell echo $(UNAME) | tr '[:upper:]' '[:lower:]') \
		--workpath $(DIST)/build \
		--specpath $(DIST) \
		--add-data "resurrector/bridge/web:resurrector/bridge/web" \
		--hidden-import uvicorn.workers \
		--hidden-import uvicorn.logging \
		--hidden-import uvicorn.protocols.http.auto \
		--hidden-import uvicorn.protocols.websockets.auto \
		--hidden-import uvicorn.lifespan.on \
		--hidden-import mcap \
		--hidden-import mcap.reader \
		--hidden-import polars \
		--hidden-import duckdb \
		--hidden-import starlette.responses \
		--exclude-module torch \
		--exclude-module sentence_transformers \
		--exclude-module cv2 \
		--exclude-module rclpy \
		--exclude-module pytest \
		resurrector/cli/main.py
	@echo "=== Build complete ==="

# --- macOS DMG ---

macos: build-base
	bash $(PKG)/macos/create_dmg.sh $(VERSION)

# --- Ubuntu DEB ---

ubuntu: build-base
	bash $(PKG)/ubuntu/build_deb.sh $(VERSION)

# --- Production builds ---
#
# Originally these went through Pyarmor obfuscation when we were
# considering a closed-source binary. The project is MIT-licensed
# open source on GitHub, so source obfuscation adds zero protection
# (anyone can read the repo) while costing real money in Pyarmor
# license fees once the file count crosses the free tier — which
# happened in v0.3.1. Drop obfuscation; build straight from source.

macos-release:
	@echo "=== Building macOS release ==="
	cd $(ROOT) && pyinstaller \
		--onefile \
		--name resurrector \
		--distpath $(DIST)/macos \
		--workpath $(DIST)/build \
		--specpath $(DIST) \
		--add-data "resurrector/bridge/web:resurrector/bridge/web" \
		--hidden-import uvicorn.workers \
		--hidden-import uvicorn.logging \
		--hidden-import uvicorn.protocols.http.auto \
		--hidden-import uvicorn.protocols.websockets.auto \
		--hidden-import uvicorn.lifespan.on \
		--hidden-import mcap \
		--hidden-import mcap.reader \
		--hidden-import polars \
		--hidden-import duckdb \
		--hidden-import starlette.responses \
		--exclude-module torch \
		--exclude-module sentence_transformers \
		--exclude-module cv2 \
		--exclude-module rclpy \
		--exclude-module pytest \
		resurrector/cli/main.py
	bash $(PKG)/macos/create_dmg.sh $(VERSION)

ubuntu-release:
	@echo "=== Building Ubuntu release ==="
	cd $(ROOT) && pyinstaller \
		--onefile \
		--name resurrector \
		--distpath $(DIST)/ubuntu \
		--workpath $(DIST)/build \
		--specpath $(DIST) \
		--add-data "resurrector/bridge/web:resurrector/bridge/web" \
		--hidden-import uvicorn.workers \
		--hidden-import uvicorn.logging \
		--hidden-import uvicorn.protocols.http.auto \
		--hidden-import uvicorn.protocols.websockets.auto \
		--hidden-import uvicorn.lifespan.on \
		--hidden-import mcap \
		--hidden-import mcap.reader \
		--hidden-import polars \
		--hidden-import duckdb \
		--hidden-import starlette.responses \
		--exclude-module torch \
		--exclude-module sentence_transformers \
		--exclude-module cv2 \
		--exclude-module rclpy \
		--exclude-module pytest \
		resurrector/cli/main.py
	bash $(PKG)/ubuntu/build_deb.sh $(VERSION)

# --- Clean ---

clean:
	rm -rf $(DIST) $(ROOT)/build
	rm -f $(ROOT)/*.spec
	@echo "Cleaned build artifacts."
