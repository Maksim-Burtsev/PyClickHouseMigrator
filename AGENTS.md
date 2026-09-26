# AGENTS.md

Instructions for AI coding agents working in this repository.
`CLAUDE.md` is a symlink to this file: edit `AGENTS.md` only.

## Project

PyClickHouseMigrator (`py-clickhouse-migrator` on PyPI, CLI command `migrator`) is a SQL-first
schema migration runner for ClickHouse. Migrations are plain `.sql` files; applied state lives in a
`db_migrations` table inside the target database. Python 3.11+. The only runtime dependencies are
`click` and `clickhouse-driver`, and keeping it that way is a feature.

User-facing behavior is specified in `README.md` and `docs/`. Read `docs/migration-format.md` and
`docs/known-limitations.md` before touching parsing or execution semantics.

## Commands

Setup: `uv sync --dev`. Always run tools through `uv run`.

| Task | Command |
|---|---|
| Lint | `uv run ruff check .` |
| Format | `uv run ruff format .` (CI runs `uv run ruff format --check .`) |
| Types | `uv run mypy py_clickhouse_migrator/` |
| All CI lint checks | `make lint` |
| Start test ClickHouse | `docker compose -f docker-compose.test.yml up -d --wait` |
| Tests | `uv run pytest` |
| Single test | `uv run pytest tests/test_migrator.py::test_init_base -v` |
| Cluster tests | `make cluster-up cluster-wait && uv run pytest -m cluster` |
| Docs site | `make docs` |

- Run the lint checks and the affected tests before every commit. CI runs lint first, then tests on
  Python 3.11–3.14 against ClickHouse 24.8, 25.3, and latest, plus a two-node cluster job.
- Most tests need a live ClickHouse on `localhost:19000` (database `test`). `test_cli.py`,
  `test_migration_parser.py`, `test_retry.py`, and `test_validation.py` mock ClickHouse and run without it.
- `pytest` deselects `cluster`-marked tests by default. They need the compose cluster on ports 19001/19002.
- Tests write to `./db/migrations` relative to the working directory: run `pytest` from the repo root.

## Layout

```text
py_clickhouse_migrator/
  cli.py               click entry point; SafeGroup turns known errors into "Error: ..." and exit code 1
  migrator.py          Migrator: db_migrations table, up / rollback / baseline / repair / show, checksum checks
  migration_parser.py  splits a .sql file into up/down sections and -- @stmt blocks
  checksum.py          SHA-256 over normalized statement blocks
  lock.py              MigrationLock: advisory lock stored in _migrations_lock (ReplacingMergeTree)
  errors.py            exception types
  __init__.py          public API, re-exported via __all__
tests/                 pytest suite; conftest.py has DB fixtures, helpers.py builds migration files
scripts/               wait_cluster_ready.py for the cluster CI job
docs/                  documentation site (zensical, configured in mkdocs.yml)
```

## Invariants

Existing users depend on these. Changing any of them is a breaking change that needs an explicit
maintainer decision and a major version bump.

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
- **Python API.** Everything in `py_clickhouse_migrator.__all__` and in `docs/python-api.md`.

## Code conventions

- **SQL safety.** Database and cluster names are interpolated into SQL, so validate them against the
  identifier regexes before use. Pass values as clickhouse-driver parameters (`%(name)s`), never via f-strings.
- **Cluster mode.** Service-table DDL uses `ON CLUSTER` with Replicated engines, and every query on a
  service table passes `self._settings` (`insert_quorum`, `select_sequential_consistency`). Keep new
  queries consistent with that, and cover cluster behavior in `tests/test_cluster.py`.
- **Errors.** Raise a specific exception from `errors.py`. If the CLI should print it as a clean
  message instead of a traceback, add it to `_HANDLED_EXCEPTIONS` in `cli.py`.
- **Output.** Results for the user go through `click.echo`; diagnostics go through the
  `py_clickhouse_migrator` logger. `--quiet` silences logs only, so dry-run SQL must use `click.echo`.
- **Typing.** Every function is fully annotated; mypy runs in strict mode.
- **Style.** Ruff with line length 120; configuration lives in `pyproject.toml`. Fix lint findings
  in the code instead of adding ignores.
- **Dependencies.** Do not add runtime dependencies. Manage dev dependencies with `uv add --dev`;
  never edit `uv.lock` by hand.

## Tests

- Every behavior change comes with a test.
- Prefer a real ClickHouse over mocks whenever SQL is involved. Mocks are for CLI wiring, retries,
  and failure paths.
- Create migration files through `tests/helpers.py` (`create_test_migration`), not by hand.
- Tests that create tables or rows must remove them at the end; follow the fixtures in `conftest.py`.

## Docs and changelog

- When CLI behavior or the documented surface changes, update `README.md`, the matching `docs/*.md`,
  and `llms.txt` / `llms-full.txt`.
- Add a line under an `## Unreleased` heading at the top of `CHANGELOG.md` for user-visible changes.

## Git

- Conventional Commits in the imperative mood: `feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `ci:`, `chore:`.
- Keep pull requests small and focused, and fill in `.github/PULL_REQUEST_TEMPLATE.md`.
