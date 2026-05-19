.PHONY: up down build-rust run-bot help

help:
	@echo "Доступные команды:"
	@echo "  make up         - Запустить инфраструктуру (Redis, Postgres)"
	@echo "  make down       - Остановить инфраструктуру"
	@echo "  make build-rust - Собрать ядро на Rust"
	@echo "  make run-bot    - Запустить Telegram бота (Python)"

up:
	docker compose up -d

down:
	docker compose down

build-rust:
	cd rust_core && cargo build --release

run-bot:
	uv run python python/main.py
