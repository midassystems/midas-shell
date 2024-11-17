#!/bin/bash

dev() {
	echo "Building in development mode..."
	make -f Makefile.dev
}

prodution() {
	echo "Building in production mode..."
	make -f Makefile.prod
}

options() {
	echo "Which would you like to run?"
	echo "1 - Build Dev"
	echo "2 - Build Production"
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
	*) echo "Please choose a different one." ;;
	esac
done
