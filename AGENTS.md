# AGENTS.md

`CLAUDE.md` is a symlink to this file: edit `AGENTS.md` only.

PyClickHouseMigrator (`py-clickhouse-migrator` on PyPI, CLI command `migrator`) is a SQL-first
schema migration runner for ClickHouse. Migrations are plain `.sql` files; applied state lives in a
`db_migrations` table inside the target database. User-facing behavior is specified in `README.md`
and `docs/`: read `docs/migration-format.md` and `docs/known-limitations.md` before touching parsing
or execution.

## Commands

- Setup: `uv sync --dev`. Run every tool through `uv run`.
- `make lint` runs the same checks as the CI lint job. Format with `uv run ruff format .`.
- Tests need a live ClickHouse: start it once with `docker compose -f docker-compose.test.yml up -d --wait`,
  then run `uv run pytest` from the repo root, because tests write to `./db/migrations` relative to it.
- `pytest` deselects tests marked `cluster`. `make test-cluster` starts a two-node cluster, runs them,
  and stops it.
- CI runs the suite on Python 3.11–3.14 against ClickHouse 24.8, 25.3, and latest: code must work on
  the oldest of each.

## Invariants

Existing users depend on these. Changing one is a breaking change: it needs the maintainer's
decision and a major version bump.

- **Checksums.** Stored checksums come from `compute_checksum_from_statements`. Any change to statement
  extraction, `normalize_content`, or serialization changes the checksum of already-applied migrations,
  and every existing user's `migrator up` starts failing with a mismatch.
- **Service tables.** `db_migrations` (`Migrator.check_migrations_table`) and `_migrations_lock`
  (`MigrationLock.ensure_table`) already exist in user databases and are created with `IF NOT EXISTS`.
  A schema change needs a migration path for tables created by older versions.
- **Migration file format.** Exactly one `-- migrator:up` and one `-- migrator:down` marker, up first;
  all SQL inside `-- @stmt` blocks; each block is executed as one query. Never split SQL on `;`.
- **CLI surface.** Command names, options, `CLICKHOUSE_MIGRATE_*` environment variables, exit codes,
  and output that CI pipelines parse.
- **Python API.** Everything in `py_clickhouse_migrator.__all__` and in `docs/python-api.md`, including
  the parameter names and positions of `Migrator` and `MigrationLock` methods.
- **Dependencies.** `click` and `clickhouse-driver` are the only runtime dependencies. Add dev
  dependencies with `uv add --dev`.

## Code conventions

- **SQL safety.** Database and cluster names are interpolated into SQL, so validate them as identifiers
  first. Pass values as clickhouse-driver parameters (`%(name)s`).
- **Cluster mode.** Service-table DDL uses `ON CLUSTER` with Replicated engines, and every query on a
  service table passes `self._settings` (`insert_quorum`, `select_sequential_consistency`). Keep new
  queries consistent with that, and cover cluster behavior in `tests/test_cluster.py`.
- **Errors.** Raise a specific exception from `errors.py`. For a clean `Error: ...` message instead of
  a traceback, add it to `_HANDLED_EXCEPTIONS` in the CLI.
- **Output.** Results for the user go through `click.echo`; diagnostics go through the
  `py_clickhouse_migrator` logger. `--quiet` silences the logger only, so dry-run SQL uses `click.echo`.

## Tests

- Every behavior change comes with a test.
- Prefer a real ClickHouse over mocks whenever SQL is involved; mocks are for CLI wiring, retries, and
  failure paths.
- Build migration files with the helpers in `tests/`, and drop the tables and rows a test creates.

## Docs and changelog

- When CLI behavior or the documented surface changes, update `README.md`, the matching `docs/*.md`,
  and `llms.txt` / `llms-full.txt`.
- Add a line under an `## Unreleased` heading at the top of `CHANGELOG.md` for user-visible changes.

## Git

Conventional Commits (`feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `ci:`, `chore:`). Keep pull
requests small and fill in `.github/PULL_REQUEST_TEMPLATE.md`.
