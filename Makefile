.PHONY: help up down restart status logs clean test lint format ingest api

help:
	@echo "Available commands:"
	@echo "  up       - Start docker services"
	@echo "  down     - Stop docker services and remove containers"
	@echo "  restart  - Restart docker services"
	@echo "  status   - Show docker services status"
	@echo "  logs     - Show docker logs"
	@echo "  clean    - Clean local cache and raw data (excluding raw datasets)"
	@echo "  test     - Run unit tests with pytest"
	@echo "  lint     - Check formatting and linting errors"
	@echo "  format   - Run auto-formatting"
	@echo "  ingest   - Run Python ingestion script to upload JSON to MinIO"
	@echo "  api      - Start FastAPI dev server"

up:
	docker-compose --env-file .env.example up -d

down:
	docker-compose down -v

restart:
	docker-compose down && docker-compose --env-file .env.example up -d

status:
	docker-compose ps

logs:
	docker-compose logs -f

clean:
	rm -rf .pytest_cache .mypy_cache .ipynb_checkpoints
	find . -type d -name "__pycache__" -exec rm -rf {} +

test:
	pytest tests/

lint:
	black --check src/ api/ tests/
	isort --check-only src/ api/ tests/
	flake8 src/ api/

format:
	black src/ api/ tests/
	isort src/ api/ tests/

ingest:
	python src/ingestion/ingest.py

api:
	uvicorn src.api.main:app --reload --port 8000
