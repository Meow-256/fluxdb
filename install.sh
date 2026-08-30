#!/usr/bin/env bash
set -e

REPO="Meow-256/fluxdb"
INSTALL_DIR="/usr/local/bin"
CONFIG_DIR="/etc/fluxdb"
DATA_DIR="/var/lib/fluxdb/data"

echo "============================================================"
echo "⚡ Installing FluxDB (Ultra-High Performance LSM Database)"
echo "============================================================"

# Check root privileges
if [ "$EUID" -ne 0 ]; then
  echo "❌ Error: Please run as root or with sudo:"
  echo "   curl -fsSL https://raw.githubusercontent.com/${REPO}/main/install.sh | sudo bash"
  exit 1
fi

# Detect OS and Architecture
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
    echo "⚠️  Unsupported architecture: $ARCH. Attempting build from source..."
    ARCH="unknown"
    ;;
esac

TEMP_DIR=$(mktemp -d)
cleanup() {
  rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

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
  echo "ℹ️  Downloading source and compiling with Cargo (release mode)..."
  if ! command -v cargo >/dev/null 2>&1; then
    echo "🔧 Rust/Cargo not found. Installing minimal Rust toolchain..."
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
mkdir -p "$INSTALL_DIR"

if [ -d "${TEMP_DIR}/bin" ]; then
  cp "${TEMP_DIR}/bin/"* "$INSTALL_DIR/"
else
  cp "${TEMP_DIR}/fluxdb-"* "$INSTALL_DIR/" 2>/dev/null || true
fi

chmod 755 "${INSTALL_DIR}/fluxdb-"*
# Create alias link: 'fluxdb' -> 'fluxdb-cli'
ln -sf "${INSTALL_DIR}/fluxdb-cli" "${INSTALL_DIR}/fluxdb"

# Create Config and Data directories
mkdir -p "$CONFIG_DIR"
mkdir -p "$DATA_DIR"

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
echo "🎉 FluxDB installed successfully!"
echo "============================================================"
echo "  • Server Binary:      ${INSTALL_DIR}/fluxdb-server"
echo "  • Interactive CLI:    ${INSTALL_DIR}/fluxdb (or fluxdb-cli)"
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
