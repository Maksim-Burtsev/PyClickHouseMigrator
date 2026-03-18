.PHONY: test test-up test-down test-cluster cluster-up cluster-down lint

test-up:
	docker compose -f docker-compose.test.yml up -d --wait

test-down:
	docker compose -f docker-compose.test.yml down

test: test-up
	uv run pytest -v
	$(MAKE) test-down

cluster-up:
	docker compose -f docker-compose.cluster.yml up -d --wait

cluster-down:
	docker compose -f docker-compose.cluster.yml down

test-cluster: cluster-up
	uv run pytest tests/ -m cluster -v
	$(MAKE) cluster-down

lint:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy py_clickhouse_migrator/
