#!/bin/bash

dev() {
	echo "Building in development mode..."
	make -f Makefile.dev
}

prodution() {
	echo "Building in production mode..."
	make -f Makefile.prod
}

shell() {
	dev
	dotenv -f .env.dev cargo test -p shell -- --nocapture
}

# vendors() {
# 	cargo test -p vendors -- --nocapture
# }

gui() {
	RUST_ENV=dev cargo test -- --nocapture

}

shell_old() {
	dev
	echo "Running tests..."

	RUST_ENV=dev cargo test -- --nocapture
}

options() {
	echo "Which would you like to run?"
	echo "1 - Test Shell"
	echo "2 - Test Vendors"
	echo "3 - Test GUI"
	echo "4 - Test All"
	echo "5 - Build Dev"
	echo "6 - Build Production"
}

# Main
while true; do
	options
	read -r option

	case $option in
	1)
		shell
		break
		;;
	2)
		vendors
		break
		;;
	3)
		gui
		break
		;;
	4)
		shell
		vendors
		gui
		break
		;;
	5)
		dev
		break
		;;
	6)
		production
		break
		;;
	*) echo "Please choose a different one." ;;
	esac
done
