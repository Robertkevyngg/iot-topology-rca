.PHONY: test test-unit lint up down

test-unit:
	pytest tests/unit -v --cov=src/iot_rca --cov-report=term-missing

test:
	pytest tests -v

lint:
	ruff check src tests

up:
	docker compose up --build -d

down:
	docker compose down -v
