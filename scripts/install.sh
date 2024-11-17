#!/bin/bash

echo "Installing Midas shell ..."

# Copy binaries to /usr/local/bin
cp bin/midas-shell /usr/local/bin/midas-shell

# Make binaries executable
chmod +x /usr/local/bin/midas-shell

# Ensure ~/.config/midas directory exists
mkdir -p ~/.config/midas

# Copy configuration files
cp config/* ~/.config/midas

echo "Installation complete! You can now run 'midas-shell'."
