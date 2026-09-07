ROOM ?= 42
INPUT ?= 0
OUTPUT ?= 1

.PHONY: server client check test clean

server:
	cargo run --bin server

client:
	cargo run --bin ms -- $(ROOM) $(INPUT) $(OUTPUT)

check:
	cargo check

test:
	@echo "Starting server..."
	@cargo run --bin server & \
	SERVER_PID=$$!; \
	trap 'kill $$SERVER_PID 2>/dev/null || true' EXIT INT TERM; \
	sleep 2; \
	echo "Starting client in room $(ROOM)..."; \
	cargo run --bin ms -- $(ROOM) $(INPUT) $(OUTPUT)

clean:
	cargo clean