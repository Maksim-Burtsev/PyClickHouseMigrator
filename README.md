<p align="center">
  <img src="https://raw.githubusercontent.com/Maksim-Burtsev/PyClickHouseMigrator/master/assets/logo.png" alt="PyClickHouseMigrator" width="200">
</p>

# PyClickHouseMigrator

[![CI](https://github.com/Maksim-Burtsev/PyClickHouseMigrator/actions/workflows/ci.yml/badge.svg)](https://github.com/Maksim-Burtsev/PyClickHouseMigrator/actions)
[![PyPI](https://img.shields.io/pypi/v/py-clickhouse-migrator)](https://pypi.org/project/py-clickhouse-migrator/)
[![Python](https://img.shields.io/badge/python-3.11%20|%203.12%20|%203.13%20|%203.14-blue)](https://pypi.org/project/py-clickhouse-migrator/)
[![codecov](https://codecov.io/gh/Maksim-Burtsev/PyClickHouseMigrator/branch/master/graph/badge.svg)](https://codecov.io/gh/Maksim-Burtsev/PyClickHouseMigrator)
[![Downloads](https://static.pepy.tech/personalized-badge/py-clickhouse-migrator?period=total&units=INTERNATIONAL_SYSTEM&left_color=grey&right_color=brightgreen&left_text=downloads)](https://pepy.tech/projects/py-clickhouse-migrator)

Lightweight Python tool for managing ClickHouse schema migrations. Minimal dependencies, no ORM.

Features: distributed locking, checksum validation, dry-run mode, cluster support (`ON CLUSTER` DDL, replicated service tables), rollback, migration status dashboard, auto-retry on connection failure.

## Install

```sh
pip install py-clickhouse-migrator
```

## Docker

```sh
docker pull maksimburtsev/py-clickhouse-migrator
```

```sh
docker run --rm \
  -v ./migrations:/migrations \
  -e CLICKHOUSE_MIGRATE_URL=clickhouse://default@clickhouse:9000/mydb \
  maksimburtsev/py-clickhouse-migrator:1 \
  up
```

Mount your migrations directory to `/migrations` inside the container.

Pin to a major version tag (`:1`) or an exact version (`:1.0.0`).

## Quick Start

```sh
export CLICKHOUSE_MIGRATE_URL=clickhouse://default@localhost:9000/mydb

migrator init                    # create migrations directory
migrator new create_users_table  # create migration file
migrator up                      # apply pending migrations
migrator show                    # check status
```

`init` and `new` work offline — no ClickHouse connection required.

## Migration Format

```python
def up() -> str:
    return """
    CREATE TABLE IF NOT EXISTS users (
        id UInt64,
        name String,
        created_at DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY id
    """


def rollback() -> str:
    return """
    DROP TABLE IF EXISTS users
    """
```

Each migration is a Python file with `up()` and `rollback()` functions that return SQL strings. Multiple statements can be separated by `;`.

## Commands

### `init`

Create the migrations directory (default `./db/migrations`).

```sh
migrator init
migrator --path ./my/migrations init
```

### `new`

Create a new timestamped migration file.

```sh
migrator new create_users_table
```

Name is optional. Only letters, digits, and underscores allowed.

### `up`

Apply pending migrations.

```sh
migrator up          # apply all pending
migrator up 3        # apply next 3
```

| Option | Default | Description |
|--------|---------|-------------|
| `N` | all | Number of migrations to apply |
| `--lock / --no-lock` | `--lock` | Enable/disable distributed lock |
| `--lock-ttl` | `300` | Lock TTL in seconds |
| `--lock-retry` | `3` | Lock acquire retry attempts |
| `--dry-run` | off | Print SQL without executing |
| `--allow-dirty` | off | Skip checksum validation |

Example output:

```
20250318090000_create_users.py applied [✔]
20250319120000_create_events.py applied [✔]
```

### `rollback`

Rollback applied migrations in reverse order.

```sh
migrator rollback        # rollback last 1
migrator rollback 3      # rollback last 3
```

| Option | Default | Description |
|--------|---------|-------------|
| `N` | `1` | Number of migrations to rollback |
| `--lock / --no-lock` | `--lock` | Enable/disable distributed lock |
| `--lock-ttl` | `300` | Lock TTL in seconds |
| `--lock-retry` | `3` | Lock acquire retry attempts |
| `--dry-run` | off | Print SQL without executing |

Example output:

```
20250319120000_create_events.py rolled back [✔].
```

### `show`

Display migration status, integrity information, and HEAD pointer.

```sh
migrator show        # last 5 applied + all pending
migrator show --all  # all applied + all pending
```

| Option | Default | Description |
|--------|---------|-------------|
| `--all` | off | Show all applied migrations (default: last 5) |

Example output:

```
Applied:
  [X] 20250320143022_add_indexes.py (HEAD)
  [X] 20250319120000_create_events.py
  [X] 20250318090000_create_users.py

Pending:
  [ ] 20250321100000_add_status_column.py

Applied: 3 | Pending: 1
```

Modified or missing migration files are flagged with `(modified)` or `(missing)` next to the name.

### `repair`

Update stored checksums in `db_migrations` to match current migration files. Use after intentionally editing an already-applied migration.

```sh
migrator repair
```

### `force-unlock`

Manually release a stuck migration lock. Use when a deployment crashed mid-migration and the lock wasn't released.

```sh
migrator force-unlock
```

### `lock-info`

Show current lock holder and expiration time.

```sh
migrator lock-info
```

## Configuration

All global options can be set via environment variables:

| Option | Env Variable | Default | Description |
|--------|-------------|---------|-------------|
| `--url` | `CLICKHOUSE_MIGRATE_URL` | — | ClickHouse connection URL |
| `--path` | `CLICKHOUSE_MIGRATE_DIR` | `./db/migrations` | Migrations directory |
| `--cluster` | `CLICKHOUSE_MIGRATE_CLUSTER` | — | Cluster name for ON CLUSTER DDL |
| `--connect-retries` | `CLICKHOUSE_MIGRATE_CONNECT_RETRIES` | `0` | Connection retry attempts |
| `--connect-retries-interval` | `CLICKHOUSE_MIGRATE_CONNECT_RETRIES_INTERVAL` | `1` | Seconds between retries |
| `-v, --verbose` | — | off | Enable DEBUG logging |
| `-q, --quiet` | — | off | Suppress all output except errors |

## Distributed Locking

When multiple CI/CD runners or replicas run `migrator up` simultaneously, the distributed lock prevents double-applying migrations. Enabled by default on `up` and `rollback`.

The lock uses a dedicated table with TTL-based expiration (default 300 seconds) and automatic retry (default 3 attempts).

If a deployment crashes mid-migration and the lock isn't released, use `lock-info` to inspect and `force-unlock` to release it manually. Locks also expire automatically after the TTL.

```sh
migrator up --no-lock              # disable locking
migrator up --lock-ttl 600         # 10 minute TTL
migrator up --lock-retry 5         # 5 acquire attempts
```

## Checksum Validation

After a migration is applied, its SHA-256 file hash is stored in `db_migrations`. On subsequent `up` runs, stored hashes are compared with current files. If someone edited an already-applied migration, the tool fails — because the database state no longer matches what the migration file describes.

`--allow-dirty` skips the check for a single run (e.g. you fixed a typo in a comment). `repair` updates all stored hashes to match current files. `show` displays integrity status per migration — ok, modified, or missing.

## Cluster Support

When `--cluster` is set, the migrator creates its own service tables (`db_migrations`, `_migrations_lock`) with `ON CLUSTER` and replicated engines. Your migration SQL is used as-is — if you need `ON CLUSTER` in your DDL, include it in the migration yourself.

```sh
export CLICKHOUSE_MIGRATE_CLUSTER=my_cluster
migrator up
```

## Python API

```python
from py_clickhouse_migrator import Migrator

migrator = Migrator(
    database_url="clickhouse://default@localhost:9000/mydb",
    migrations_dir="./db/migrations",
)
migrator.up()
```

## Known Limitations

**SQL splitting by `;`** — migration SQL is split into statements by `;`. Semicolons inside string literals will break parsing. If you need a literal `;` in a value, use a separate migration or encode the value differently.

**No DDL transactions** — if a migration with multiple statements fails halfway, some statements will have been applied. Always use `IF NOT EXISTS` / `IF EXISTS` to make migrations idempotent and safe to re-run.

## License

[MIT](LICENSE)
