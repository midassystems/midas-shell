#!/bin/bash

cd "$(dirname "$0")/.." || exit 1
env_file=".env"

# Function to load .env file
if [ -f $env_file ]; then
	set -a
	source $env_file
	set +a
fi

export RAW_DIR=tests/data
export PROCESSED_DIR=../midas-server/data/processed_data

cargo test -- --nocapture
