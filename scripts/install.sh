#!/bin/bash

echo "Installing Midas shell ..."

# Copy binaries to /usr/local/bin
cp bin/midas-shell /usr/local/bin/midas-shell
cp bin/midas-cli /usr/local/bin/midas-cli

# Make binaries executable
chmod +x /usr/local/bin/midas-shell
chmod +x /usr/local/bin/midas-cli

# Ensure ~/.config/midas directory exists
mkdir -p ~/.config/midas

# Copy configuration files
cp config/* ~/.config/midas

echo "Installation complete! You can now run 'midas-shell'."
