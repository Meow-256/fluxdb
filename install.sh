#!/usr/bin/env bash
set -e

REPO="Meow-256/fluxdb"
INSTALL_DIR="/usr/local/bin"
CONFIG_DIR="/etc/fluxdb"
DATA_DIR="/var/lib/fluxdb/data"
DOCKER_IMAGE="ghcr.io/meow-256/fluxdb:latest"

MODE="auto" # auto, docker, native

while [ $# -gt 0 ]; do
  case "$1" in
    --docker)
      MODE="docker"
      shift
      ;;
    --native)
      MODE="native"
      shift
      ;;
    --help|-h)
      echo "FluxDB Installer"
      echo "Usage: curl -fsSL https://raw.githubusercontent.com/${REPO}/main/install.sh | sudo bash [options]"
      echo ""
      echo "Options:"
      echo "  --docker    Force container installation using Docker"
      echo "  --native    Force bare-metal / native systemd installation"
      echo "  --help      Show this help message"
      exit 0
      ;;
    *)
      shift
      ;;
  esac
done

echo "============================================================"
echo "⚡ Installing FluxDB (Ultra-High Performance LSM Database)"
echo "============================================================"

# Check root privileges
if [ "$EUID" -ne 0 ]; then
  echo "❌ Error: Please run as root or with sudo:"
  echo "   curl -fsSL https://raw.githubusercontent.com/${REPO}/main/install.sh | sudo bash"
  exit 1
fi

mkdir -p "$CONFIG_DIR"
mkdir -p "$DATA_DIR"
mkdir -p "$INSTALL_DIR"

TEMP_DIR=$(mktemp -d)
cleanup() {
  rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

# Decide execution mode
if [ "$MODE" = "auto" ]; then
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    MODE="docker"
    echo "🐳 Docker detected on system. Using Docker container mode."
  else
    MODE="native"
    echo "🖥️  Docker not detected. Using Native Bare-Metal mode."
  fi
fi

# ==========================================
# 1. DOCKER CONTAINER MODE
# ==========================================
if [ "$MODE" = "docker" ]; then
  RUN_IMAGE="$DOCKER_IMAGE"
  echo "🔍 Checking remote Docker image ($DOCKER_IMAGE)..."

  # Attempt pulling remote image
  if ! docker pull "$DOCKER_IMAGE" >/dev/null 2>&1; then
    echo "ℹ️  Remote Docker image not found or not yet published."
    echo "🔨 Building FluxDB Docker container locally from source..."
    git clone --depth 1 "https://github.com/${REPO}.git" "${TEMP_DIR}/docker_source"
    docker build -t fluxdb:local "${TEMP_DIR}/docker_source"
    RUN_IMAGE="fluxdb:local"
  fi

  # Remove existing fluxdb container if running
  if docker ps -a --format '{{.Names}}' | grep -Eq "^fluxdb\$"; then
    echo "🔄 Stopping and replacing existing 'fluxdb' container..."
    docker rm -f fluxdb >/dev/null 2>&1 || true
  fi

  echo "🚀 Launching FluxDB container (${RUN_IMAGE})..."
  docker run -d \
    --name fluxdb \
    --restart always \
    -p 7379:7379 \
    -p 7380:7380 \
    -v "${DATA_DIR}:/app/data" \
    "$RUN_IMAGE"

  # Create host CLI wrapper script: 'fluxdb' -> 'docker exec -it fluxdb fluxdb-cli'
  cat << 'EOF' > "${INSTALL_DIR}/fluxdb"
#!/usr/bin/env bash
if [ -t 0 ]; then
  exec docker exec -it fluxdb fluxdb-cli "$@"
else
  exec docker exec -i fluxdb fluxdb-cli "$@"
fi
EOF
  chmod 755 "${INSTALL_DIR}/fluxdb"
  ln -sf "${INSTALL_DIR}/fluxdb" "${INSTALL_DIR}/fluxdb-cli"

  echo ""
  echo "============================================================"
  echo "🎉 FluxDB started successfully in Docker!"
  echo "============================================================"
  echo "  • Container Name:     fluxdb"
  echo "  • Interactive CLI:    fluxdb (or fluxdb-cli)"
  echo "  • Data Storage:       ${DATA_DIR}"
  echo "  • TCP Port (RESP):    7379"
  echo "  • Web Management UI:  http://localhost:7380"
  echo ""
  echo "To connect to the database now, simply run:"
  echo "  fluxdb"
  echo "============================================================"
  exit 0
fi

# ==========================================
# 2. NATIVE BARE-METAL MODE
# ==========================================
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

case "$ARCH" in
  x86_64|amd64)
    ARCH="x86_64"
    ;;
  aarch64|arm64)
    ARCH="aarch64"
    ;;
  *)
    ARCH="unknown"
    ;;
esac

INSTALLED_FROM_RELEASE=0

# Try downloading pre-built release binary from GitHub Releases
if [ "$ARCH" != "unknown" ]; then
  TARGET=""
  if [ "$OS" = "linux" ]; then
    TARGET="${ARCH}-unknown-linux-gnu"
  elif [ "$OS" = "darwin" ]; then
    TARGET="${ARCH}-apple-darwin"
  fi

  if [ -n "$TARGET" ]; then
    echo "🔍 Fetching latest FluxDB release for $TARGET..."
    DOWNLOAD_URL="https://github.com/${REPO}/releases/latest/download/fluxdb-${TARGET}.tar.gz"
    
    if curl -sLf "$DOWNLOAD_URL" -o "${TEMP_DIR}/fluxdb.tar.gz" 2>/dev/null; then
      echo "📦 Extracting pre-built binaries..."
      tar -xzf "${TEMP_DIR}/fluxdb.tar.gz" -C "$TEMP_DIR"
      INSTALLED_FROM_RELEASE=1
    fi
  fi
fi

# Fallback to source compilation if pre-built release binary is not available yet
if [ $INSTALLED_FROM_RELEASE -eq 0 ]; then
  echo "ℹ️  Compiling from source with Cargo (release mode)..."
  if ! command -v cargo >/dev/null 2>&1; then
    echo "🔧 Installing minimal Rust toolchain..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
    export PATH="$HOME/.cargo/bin:$PATH"
  fi

  git clone --depth 1 "https://github.com/${REPO}.git" "${TEMP_DIR}/source"
  cd "${TEMP_DIR}/source"
  cargo build --release --bins
  mkdir -p "${TEMP_DIR}/bin"
  cp target/release/fluxdb-server target/release/fluxdb-cli target/release/fluxdb-bench target/release/fluxdb-dump target/release/fluxdb-load target/release/fluxdb-check "${TEMP_DIR}/bin/"
  cp fluxdb.toml "${TEMP_DIR}/"
fi

# Install Binaries to /usr/local/bin
echo "🚀 Installing binaries to ${INSTALL_DIR}..."
if [ -d "${TEMP_DIR}/bin" ]; then
  cp "${TEMP_DIR}/bin/"* "$INSTALL_DIR/"
else
  cp "${TEMP_DIR}/fluxdb-"* "$INSTALL_DIR/" 2>/dev/null || true
fi

chmod 755 "${INSTALL_DIR}/fluxdb-"*
ln -sf "${INSTALL_DIR}/fluxdb-cli" "${INSTALL_DIR}/fluxdb"

if [ ! -f "${CONFIG_DIR}/fluxdb.toml" ]; then
  if [ -f "${TEMP_DIR}/fluxdb.toml" ]; then
    cp "${TEMP_DIR}/fluxdb.toml" "${CONFIG_DIR}/fluxdb.toml"
  elif [ -f "${TEMP_DIR}/source/fluxdb.toml" ]; then
    cp "${TEMP_DIR}/source/fluxdb.toml" "${CONFIG_DIR}/fluxdb.toml"
  else
    cat << 'EOF' > "${CONFIG_DIR}/fluxdb.toml"
[server]
host = "0.0.0.0"
port = 7379
http_port = 7380
data_dir = "/var/lib/fluxdb/data"

[engine]
memtable_size_bytes = 268435456
block_cache_mb = 256
compaction_trigger = 4
commit_delay_us = 1000
async_fsync = true
EOF
  fi
fi

# Setup systemd service on Linux
if [ "$OS" = "linux" ] && command -v systemctl >/dev/null 2>&1; then
  echo "⚙️  Configuring systemd service (/etc/systemd/system/fluxdb.service)..."
  cat << 'EOF' > /etc/systemd/system/fluxdb.service
[Unit]
Description=FluxDB Ultra-High Performance LSM Database
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/var/lib/fluxdb
ExecStart=/usr/local/bin/fluxdb-server --config /etc/fluxdb/fluxdb.toml
Restart=always
RestartSec=3
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

  systemctl daemon-reload
  systemctl enable --now fluxdb
  echo "✅ FluxDB systemd service started and enabled on boot."
fi

echo ""
echo "============================================================"
echo "🎉 FluxDB installed successfully (Native Mode)!"
echo "============================================================"
echo "  • Server Binary:      ${INSTALL_DIR}/fluxdb-server"
echo "  • Interactive CLI:    ${INSTALL_DIR}/fluxdb"
echo "  • Configuration:      ${CONFIG_DIR}/fluxdb.toml"
echo "  • Data Storage:       ${DATA_DIR}"
echo "  • TCP Port (RESP):    7379"
echo "  • Web Management UI:  http://localhost:7380"
echo ""
if [ "$OS" = "linux" ] && command -v systemctl >/dev/null 2>&1; then
  echo "Service Management Commands:"
  echo "  sudo systemctl status fluxdb"
  echo "  sudo systemctl restart fluxdb"
  echo "  sudo systemctl stop fluxdb"
  echo ""
fi
echo "To connect to the database now, simply run:"
echo "  fluxdb"
echo "============================================================"
