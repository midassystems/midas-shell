#!/bin/bash

echo "Installing Midas shell ..."

# Clean out old version
rm /usr/local/bin/midas-shell
rm /usr/local/bin/midas-cli

# Copy binaries to /usr/local/bin
cp bin/midas-shell /usr/local/bin/midas-shell
cp bin/midas-cli /usr/local/bin/midas-cli

# Make binaries executable
chmod +x /usr/local/bin/midas-shell
chmod +x /usr/local/bin/midas-cli

# Ensure ~/.config/midas directory exists
mkdir -p ~/.config/midas

# List of configuration files
files=("config.toml" "midas_starship.toml")

# Copy configuration files conditionally
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

echo "Installation complete! You can now run 'midas-shell'."
