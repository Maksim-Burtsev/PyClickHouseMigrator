# Contributing

Thanks for taking the time. Small, focused pull requests are the easiest to merge.

## Setup

Requires [uv](https://docs.astral.sh/uv/) and Docker.

```sh
git clone https://github.com/Maksim-Burtsev/PyClickHouseMigrator.git
cd PyClickHouseMigrator
uv sync --dev
```

## Running tests

Most tests need a live ClickHouse:

```sh
docker compose -f docker-compose.test.yml up -d --wait
uv run pytest -v
```

Cluster tests run against a separate two-node compose file:

```sh
docker compose -f docker-compose.cluster.yml up -d --wait
uv run pytest tests/ -m cluster -v
```

## Before opening a PR

The same three checks CI runs:

```sh
uv run ruff check .
uv run ruff format --check .
uv run mypy py_clickhouse_migrator/
```

## Guidelines

- Add a test for any behavior change — the test suite is the reason this tool is
  safe to point at a production schema.
- Keep the dependency set small. `click` and `clickhouse-driver` are the only
  runtime dependencies, and that is a feature.
- Update `docs/` and `README.md` when you change CLI behavior, plus `llms.txt` /
  `llms-full.txt` if the change affects the documented surface.
- Add an entry to `CHANGELOG.md` under an "Unreleased" heading.

## Reporting bugs

Open an issue with the migrator version (`migrator --version`), ClickHouse
version, the migration SQL if relevant, and the full command plus output.
