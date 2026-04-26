<p align="center">
  <img src="https://raw.githubusercontent.com/Maksim-Burtsev/PyClickHouseMigrator/master/assets/logo.png" alt="PyClickHouseMigrator" width="200">
</p>

# PyClickHouseMigrator

[![CI](https://github.com/Maksim-Burtsev/PyClickHouseMigrator/actions/workflows/ci.yml/badge.svg)](https://github.com/Maksim-Burtsev/PyClickHouseMigrator/actions)
[![PyPI](https://img.shields.io/pypi/v/py-clickhouse-migrator)](https://pypi.org/project/py-clickhouse-migrator/)
[![Python](https://img.shields.io/badge/python-3.11%20|%203.12%20|%203.13%20|%203.14-blue)](https://pypi.org/project/py-clickhouse-migrator/)
[![codecov](https://codecov.io/gh/Maksim-Burtsev/PyClickHouseMigrator/branch/master/graph/badge.svg)](https://codecov.io/gh/Maksim-Burtsev/PyClickHouseMigrator)
[![Downloads](https://static.pepy.tech/personalized-badge/py-clickhouse-migrator?period=total&units=INTERNATIONAL_SYSTEM&left_color=grey&right_color=brightgreen&left_text=downloads)](https://pepy.tech/projects/py-clickhouse-migrator)

**SQL-first ClickHouse migrations for Python teams.**

PyClickHouseMigrator is a small, predictable migration runner for ClickHouse. It keeps schema changes in plain `.sql` files, applies them in order, stores migration state inside ClickHouse, validates checksums, supports rollback SQL, and fits naturally into CI/CD.

No ORM. No schema diff engine. No framework. Just a clean migration flow for teams that want their ClickHouse DDL to live in Git.

## Why this exists

ClickHouse schema changes are often written directly in SQL. That is usually the right choice, but the deployment flow around those changes can become fragile:

- which migrations have already run?
- did someone edit an applied migration?
- what exactly will run in the next deployment?
- how do we adopt an existing database?
- how do we avoid two deployment jobs applying the same migration at the same time?
- how do we keep cluster service metadata consistent without turning the migration tool into a platform?

PyClickHouseMigrator focuses on that operational layer.

## Highlights

- **Plain SQL migrations** — migration files are `.sql`, not Python modules.
- **Explicit statement blocks** — each `-- @stmt` block is executed as one ClickHouse query; the migrator does not split SQL by semicolons.
- **Rollback SQL** — `-- migrator:down` stores the rollback SQL used by `migrator rollback`.
- **Checksum validation** — detects edited or missing applied migration files.
- **Dry-run mode** — preview pending migrations without writing migration state.
- **Baseline** — adopt an existing database without executing historical migrations.
- **Advisory locking** — protects common CI/CD concurrency cases.
- **Cluster-aware service tables** — migration metadata and lock tables can be created with `ON CLUSTER` and replicated engines.
- **Small dependency set** — depends on `click` and `clickhouse-driver`.

## Install

```sh
pip install py-clickhouse-migrator
```

With `uv`:

```sh
uv add py-clickhouse-migrator
```

The CLI command is:

```sh
migrator --help
```

## Quick start

Set the ClickHouse connection URL. The database in the URL must already exist.

```sh
export CLICKHOUSE_MIGRATE_URL=clickhouse://default@localhost:9000/mydb
```

Create a migrations directory and a first migration:

```sh
migrator init
migrator new create_users_table
```

Edit the generated file in `./db/migrations`:

```sql
-- migrator:up
-- @stmt
CREATE TABLE IF NOT EXISTS users
(
    id UInt64,
    name String,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY id

-- migrator:down
-- @stmt
DROP TABLE IF EXISTS users
```

Preview what will run:

```sh
migrator up --dry-run
```

Apply pending migrations:

```sh
migrator up
```

Check status:

```sh
migrator show
```

Rollback the last applied migration:

```sh
migrator rollback
```

`init` and `new` work offline. Commands that read or write migration state require a ClickHouse connection.

## Migration format

A migration is a `.sql` file with two required sections:

```sql
-- migrator:up
-- @stmt
-- SQL to apply the migration

-- migrator:down
-- @stmt
-- SQL to roll it back
```

Rules:

- each file must contain exactly one `-- migrator:up` section and exactly one `-- migrator:down` section;
- `-- migrator:up` must appear before `-- migrator:down`;
- SQL must be placed inside `-- @stmt` blocks;
- the `up` section must contain at least one non-empty statement block;
- the `down` section may be empty;
- each `-- @stmt` block is sent to ClickHouse as one query;
- the migrator does not split SQL by `;`.

This is intentional. It avoids fragile semicolon splitting and makes multi-statement migrations explicit.

Good:

```sql
-- migrator:up
-- @stmt
CREATE TABLE IF NOT EXISTS events
(
    id UInt64,
    message String
)
ENGINE = MergeTree
ORDER BY id

-- @stmt
ALTER TABLE events ADD COLUMN IF NOT EXISTS created_at DateTime DEFAULT now()

-- migrator:down
-- @stmt
ALTER TABLE events DROP COLUMN IF EXISTS created_at

-- @stmt
DROP TABLE IF EXISTS events
```

Avoid putting multiple ClickHouse queries into one block:

```sql
-- bad
-- @stmt
CREATE TABLE a (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE b (id UInt64) ENGINE = MergeTree ORDER BY id;
```

Use separate blocks instead:

```sql
-- good
-- @stmt
CREATE TABLE a (id UInt64) ENGINE = MergeTree ORDER BY id

-- @stmt
CREATE TABLE b (id UInt64) ENGINE = MergeTree ORDER BY id
```

See [Migration format](docs/migration-format.md) for more examples.

## Commands

### `init`

Create the migrations directory. Default: `./db/migrations`.

```sh
migrator init
migrator --path ./migrations init
```

Works offline.

### `new`

Create a timestamped `.sql` migration file.

```sh
migrator new create_users_table
```

Generated filename format:

```text
YYYYMMDDHHmmss_create_users_table.sql
```

The name suffix is optional, but recommended. Only letters, digits, and underscores are allowed.

Works offline.

### `up`

Apply pending migrations in filename order.

```sh
migrator up          # apply all pending migrations
migrator up 3        # apply next 3 pending migrations
migrator up --dry-run
```

| Option | Default | Description |
|---|---:|---|
| `N` | all | Optional positional limit: number of pending migrations to apply. |
| `--lock / --no-lock` | `--lock` | Enable or disable the migration lock. |
| `--lock-ttl` | `600` | Lock TTL in seconds. |
| `--lock-retry` | `3` | Lock acquire retry attempts. |
| `--dry-run` | off | Print pending migration SQL without executing it. |
| `--validate / --no-validate` | `--validate` | Enable or disable preflight validation with `EXPLAIN AST`. |
| `--allow-dirty` | off | Skip checksum mismatch failures for this run. |

Example output:

```text
20260421140000_create_users_table.sql applied [✔]
20260421143000_add_events_table.sql applied [✔]
```

### `rollback`

Rollback applied migrations in reverse order.

```sh
migrator rollback        # rollback last applied migration
migrator rollback 3      # rollback last 3 applied migrations
migrator rollback --dry-run
```

| Option | Default | Description |
|---|---:|---|
| `N` | `1` | Optional positional limit: number of migrations to rollback. |
| `--lock / --no-lock` | `--lock` | Enable or disable the migration lock. |
| `--lock-ttl` | `600` | Lock TTL in seconds. |
| `--lock-retry` | `3` | Lock acquire retry attempts. |
| `--dry-run` | off | Print rollback SQL without executing it. |
| `--validate / --no-validate` | `--validate` | Enable or disable preflight validation with `EXPLAIN AST`. |

Rollback uses the `down` SQL stored in `db_migrations` at the time the migration was applied, not the current file content.

### `show`

Display applied migrations, pending migrations, HEAD, baseline markers, and integrity warnings.

```sh
migrator show
migrator show --all
```

| Option | Default | Description |
|---|---:|---|
| `--all` | off | Show all applied migrations. By default, only the latest 5 applied migrations are shown. |

Example:

```text
Applied:
  [X] 20260421143000_add_events_table.sql (HEAD)
  [X] 20260421140000_create_users_table.sql

Pending:
  [ ] 20260421150000_add_status_column.sql

Applied: 2 | Pending: 1
```

Possible applied migration markers:

| Marker | Meaning |
|---|---|
| `HEAD` | Latest applied migration row. |
| `baseline` | Migration was recorded by `baseline`, not executed. |
| `modified` | Applied migration file exists but its checksum no longer matches. |
| `missing` | Applied migration file is missing locally. |

### `baseline`

Record existing `.sql` migration files as already applied without executing them.

```sh
migrator baseline
```

Use this when you introduce the migrator to an existing ClickHouse database whose schema already exists.

Baseline behavior:

- requires an empty `db_migrations` table;
- records current `.sql` files as `baseline` rows;
- does not execute SQL;
- does not validate that the ClickHouse schema matches the files;
- baseline rows are not selected by `rollback`;
- baseline rows are excluded from checksum validation.

See [Baseline existing databases](docs/baseline.md).

### `repair`

Update stored checksums to match current migration files.

```sh
migrator repair
```

Use only after intentionally editing an already-applied migration file. `repair` does not execute SQL and does not modify your application schema. Missing files are reported and skipped.

### `lock-info`

Show active migration lock information.

```sh
migrator lock-info
```

Example:

```text
Locked by: runner-01:1234:abc123ef
Locked at: 2026-04-21 14:30:00
Expires at: 2026-04-21 14:40:00
```

### `force-unlock`

Manually release a stuck lock.

```sh
migrator force-unlock
```

Use this when a deployment process crashed and the lock did not get released. Locks also expire automatically after their TTL.

## Configuration

Global options can be provided through CLI flags or environment variables.

| CLI option | Environment variable | Default | Description |
|---|---|---:|---|
| `--url` | `CLICKHOUSE_MIGRATE_URL` | — | ClickHouse connection URL. Required for DB commands. |
| `--path` | `CLICKHOUSE_MIGRATE_DIR` | `./db/migrations` | Migrations directory. |
| `--cluster` | `CLICKHOUSE_MIGRATE_CLUSTER` | — | ClickHouse cluster name for service table DDL. |
| `--connect-retries` | `CLICKHOUSE_MIGRATE_CONNECT_RETRIES` | `0` | Connection retry attempts. |
| `--connect-retries-interval` | `CLICKHOUSE_MIGRATE_CONNECT_RETRIES_INTERVAL` | `1` | Seconds between connection retries. |
| `--send-receive-timeout` | `CLICKHOUSE_MIGRATE_SEND_RECEIVE_TIMEOUT` | `600` | ClickHouse client send/receive timeout in seconds. |
| `-v`, `--verbose` | — | off | Enable DEBUG logging. |
| `-q`, `--quiet` | — | off | Suppress INFO/WARNING logs; command output such as dry-run SQL is still printed. |

Connection URL example:

```sh
clickhouse://user:password@host:9000/database
```

For TLS/secure connections, use connection parameters supported by `clickhouse-driver` URLs, for example:

```sh
clickhouse://user:password@host:9440/database?secure=True
```

## Docker

```sh
docker pull maksimburtsev/py-clickhouse-migrator:2
```

Run migrations:

```sh
docker run --rm \
  -v "$PWD/db/migrations:/migrations" \
  -e CLICKHOUSE_MIGRATE_URL=clickhouse://default@clickhouse:9000/mydb \
  maksimburtsev/py-clickhouse-migrator:2 \
  up
```

Inside the Docker image, the default migrations directory is `/migrations`.

Pin to a major version tag for stable automation:

```text
maksimburtsev/py-clickhouse-migrator:2
```

Or pin to an exact release:

```text
maksimburtsev/py-clickhouse-migrator:2.0.0
```

See [Docker usage](docs/docker.md).

## CI/CD

In CI/CD, the usual deployment step is intentionally simple:

```sh
migrator up
```

GitHub Actions example:

```yaml
- name: Run ClickHouse migrations
  run: migrator up
  env:
    CLICKHOUSE_MIGRATE_URL: ${{ secrets.CLICKHOUSE_MIGRATE_URL }}
```

Recommended deployment pattern:

1. build and test application code;
2. run `migrator up` once per deployment;
3. deploy application code that depends on the new schema.

The CLI exits with `0` on success and `1` for handled migration errors such as invalid migrations, connection failures, checksum mismatches, missing databases, and lock errors.

See [CI/CD usage](docs/ci-cd.md).

## Checksum validation

When a migration is applied, PyClickHouseMigrator stores a SHA-256 checksum in `db_migrations`.

The checksum is computed from the parsed `up` and `down` statement blocks, not from raw file bytes. This makes the checksum tied to the migration SQL that the tool actually understands and executes.

On `migrator up`, applied migration checksums are compared with the current local files. If an applied migration was modified or deleted, the command fails unless `--allow-dirty` is used.

Useful commands:

```sh
migrator show          # display modified/missing markers
migrator up            # fail fast on checksum mismatch
migrator up --allow-dirty
migrator repair        # update stored checksums after intentional edits
```

## Preflight validation

By default, `up`, `rollback`, and their dry-run variants validate statements with `EXPLAIN AST` before execution.

```sh
migrator up --no-validate
migrator rollback --no-validate
```

Validation is best-effort. It catches many syntax and parse problems early, but it is not a guarantee that execution will succeed and it is not a production-safety analyzer.

## Locking

`up`, `rollback`, and `baseline` use an advisory migration lock by default.

The lock is stored in a ClickHouse service table named `_migrations_lock`. It has a TTL, a lock owner identity, retry behavior, and manual recovery commands.

```sh
migrator up --lock-ttl 900
migrator up --lock-retry 10
migrator up --no-lock
migrator lock-info
migrator force-unlock
```

The lock is meant to protect common deployment races, for example two CI jobs starting at the same time. It is still best practice to run migrations from a single deployment job or Kubernetes Job.

## Cluster mode

Set a cluster name when you want the migrator's own service tables to be created across a ClickHouse cluster:

```sh
export CLICKHOUSE_MIGRATE_CLUSTER=my_cluster
migrator up
```

When cluster mode is enabled:

- `db_migrations` is created with `ON CLUSTER` and a replicated engine;
- `_migrations_lock` is created with `ON CLUSTER` and a replicated replacing engine;
- service table writes use cluster consistency settings;
- your migration SQL is executed exactly as written.

PyClickHouseMigrator does not inject `ON CLUSTER` into user migrations. If your ClickHouse DDL must run on the whole cluster, write `ON CLUSTER` in the migration yourself.

See [Cluster mode](docs/cluster-mode.md).

## Python API

```python
from py_clickhouse_migrator import Migrator

migrator = Migrator(
    database_url="clickhouse://default@localhost:9000/mydb",
    migrations_dir="./db/migrations",
)

migrator.up()
```

See [Python API](docs/python-api.md).

## What PyClickHouseMigrator does not do

PyClickHouseMigrator is intentionally narrow. It does not:

- generate schema diffs;
- inspect your database and produce migrations;
- rewrite or approve your SQL;
- generate rollback statements;
- inject `ON CLUSTER` into user migrations;
- create the target database;
- provide a web UI, RBAC, approvals, or deployment orchestration;
- sandbox migration SQL.

Migration files are trusted input. The tool executes them as written.

## Known limitations

- ClickHouse DDL is not transactional. A multi-statement migration can partially apply if a later statement fails.
- The advisory lock is best-effort. It reduces common concurrency problems, but it is not a substitute for a single well-defined migration job.
- The target database must exist before the migrator runs.
- Baseline does not compare migration files with the existing database schema.
- Preflight validation is best-effort and can be disabled with `--no-validate`.
- Each `-- @stmt` block must contain one ClickHouse query.

See [Known limitations](docs/known-limitations.md).

## Documentation

- [Migration format](docs/migration-format.md)
- [Baseline existing databases](docs/baseline.md)
- [Cluster mode](docs/cluster-mode.md)
- [CI/CD usage](docs/ci-cd.md)
- [Docker usage](docs/docker.md)
- [Python API](docs/python-api.md)
- [Troubleshooting](docs/troubleshooting.md)
- [Known limitations](docs/known-limitations.md)
- [2.0 release notes](docs/release-2.0.md)

## License

[MIT](LICENCE.txt)
