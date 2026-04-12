.PHONY: test test-up test-down test-cluster test-all cluster-up cluster-wait cluster-down lint

test-up:
	docker compose -f docker-compose.test.yml up -d --wait

test-down:
	docker compose -f docker-compose.test.yml down

test: test-up
	uv run pytest -v
	$(MAKE) test-down

cluster-up:
	docker compose -f docker-compose.cluster.yml up -d --wait

cluster-wait:
	uv run python scripts/wait_cluster_ready.py

cluster-down:
	docker compose -f docker-compose.cluster.yml down

test-cluster: cluster-up cluster-wait
	uv run pytest tests/ -m cluster -v
	$(MAKE) cluster-down

test-all:
	$(MAKE) test
	$(MAKE) test-cluster

lint:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy py_clickhouse_migrator/
