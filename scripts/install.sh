#!/bin/bash

set -e

REPO_URL="https://github.com/midassystems/midas-shell.git"
BUILD_DIR="$(mktemp -d)"
INSTALL_PATH="$HOME/.config/midas/bin"

# Ensure Rust is installed
if ! command -v cargo >/dev/null 2>&1; then
	echo "'cargo' is not installed. Please install Rust before running this script."
	exit 1
fi

# Clone most recent commit
git clone --depth 1 "$REPO_URL" "$BUILD_DIR"

# Build
cd "$BUILD_DIR"
cargo build --release

# Install
mkdir -p "$INSTALL_PATH"
cp target/release/midas-cli "$INSTALL_PATH/midas-cli"
cp target/release/midas-shell "$INSTALL_PATH/midas-shell"

# Copy configuration files conditionally
files=("config.toml" "midas_starship.toml")

for file in "${files[@]}"; do
	src_file="config/$file"
	dest_file="$HOME/.config/midas/$file"

	if [ -e "$dest_file" ]; then
		echo "Skipped $file as it already exists"
	else
		cp "$src_file" "$dest_file"
		echo "Copied $file to $dest_file"
	fi
done

# Cleanup
rm -rf "$BUILD_DIR"

# Post-install message
echo ""
echo "To use 'midas-shell' or 'midas-cli' from anywhere, add this to your shell config:"
echo ""
echo "    export PATH=\"\$HOME/.config/midas/bin:\$PATH\""
echo ""
echo "Then restart your shell or run: source ~/.bashrc (or ~/.zshrc)"
