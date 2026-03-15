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
ROOT := $(shell cd .. && pwd)
DIST := $(ROOT)/dist
SRC := $(ROOT)/resurrector

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
	cd $(ROOT) && python -m pyarmor gen \
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
	bash packaging/macos/create_dmg.sh $(VERSION)

# --- Ubuntu DEB ---

ubuntu: build-base
	bash packaging/ubuntu/build_deb.sh $(VERSION)

# --- With obfuscation (production builds) ---

macos-release: obfuscate
	@echo "=== Building obfuscated macOS release ==="
	cd $(ROOT) && pyinstaller \
		--onefile \
		--name resurrector \
		--distpath $(DIST)/macos \
		--workpath $(DIST)/build \
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
		--exclude-module torch \
		--exclude-module sentence_transformers \
		--exclude-module pytest \
		$(DIST)/obfuscated/resurrector/cli/main.py
	bash packaging/macos/create_dmg.sh $(VERSION)

ubuntu-release: obfuscate
	@echo "=== Building obfuscated Ubuntu release ==="
	cd $(ROOT) && pyinstaller \
		--onefile \
		--name resurrector \
		--distpath $(DIST)/ubuntu \
		--workpath $(DIST)/build \
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
		--exclude-module torch \
		--exclude-module sentence_transformers \
		--exclude-module pytest \
		$(DIST)/obfuscated/resurrector/cli/main.py
	bash packaging/ubuntu/build_deb.sh $(VERSION)

# --- Clean ---

clean:
	rm -rf $(DIST) $(ROOT)/build
	rm -f $(ROOT)/*.spec
	@echo "Cleaned build artifacts."
