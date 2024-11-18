#!/bin/bash

dev() {
	echo "Building in development mode..."
	make -f build/Makefile.dev
}

run_dev() {
	if cd tests; then
		RUST_ENV=dev ../target/debug/midas-shell
	fi
}

prodution() {
	echo "Building in production mode..."
	make -f build/Makefile.prod
}

options() {
	echo "Which would you like to run?"
	echo "1 - Build Dev"
	echo "2 - Build Production"
	echo "3 - Run Dev"

}

# Main
while true; do
	options
	read -r option

	case $option in
	1)
		dev
		break
		;;
	2)
		prodution
		break
		;;
	3)
		run_dev
		break
		;;
	*) echo "Please choose a different one." ;;
	esac
done
