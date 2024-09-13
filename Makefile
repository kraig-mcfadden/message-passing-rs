.PHONY: default
default: verify

.PHONY: verify
verify:
	cargo check --all-features
	cargo test --all-features
	cargo fmt
	cargo fix --allow-dirty --allow-staged --all-features
	cargo fmt
	make lint

.PHONY: lint
lint:
	cargo clippy --all-targets --all-features -- -D warnings
