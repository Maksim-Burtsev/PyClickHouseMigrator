# Python API

The CLI is the primary interface, but PyClickHouseMigrator also exposes a small Python API.

## Basic usage

```python
from py_clickhouse_migrator import Migrator

migrator = Migrator(
    database_url="clickhouse://default@localhost:9000/mydb",
    migrations_dir="./db/migrations",
)

migrator.up()
```

## Constructor

```python
Migrator(
    database_url: str,
    migrations_dir: str = "./db/migrations",
    cluster: str = "",
    connect_retries: int = 0,
    connect_retries_interval: int = 1,
    send_receive_timeout: int = 600,
)
```

Parameters:

| Parameter | Description |
|---|---|
| `database_url` | ClickHouse connection URL. Required. |
| `migrations_dir` | Directory containing `.sql` migration files. |
| `cluster` | Optional ClickHouse cluster name for migrator service tables. |
| `connect_retries` | Number of connection retry attempts during startup. |
| `connect_retries_interval` | Seconds between connection retries. |
| `send_receive_timeout` | ClickHouse client send/receive timeout in seconds. |

Creating a `Migrator` instance checks the ClickHouse connection and ensures the `db_migrations` service table exists.

## Apply migrations

```python
migrator.up()
```

Limit the number of pending migrations:

```python
migrator.up(n=3)
```

Dry-run:

```python
migrator.up(dry_run=True)
```

Skip checksum enforcement for a single run:

```python
migrator.up(allow_dirty=True)
```

Disable preflight validation:

```python
migrator.up(validate=False)
```

## Rollback

```python
migrator.rollback()
```

Rollback multiple migrations:

```python
migrator.rollback(number=3)
```

Dry-run rollback:

```python
migrator.rollback(number=1, dry_run=True)
```

## Show migrations

```python
result = migrator.show_migrations()
print(result.output)

if result.warning:
    print(result.warning)
```

Show all applied migrations:

```python
result = migrator.show_migrations(show_all=True)
```

## Checksum validation and repair

```python
mismatches = migrator.validate_checksums()

for mismatch in mismatches:
    print(mismatch.name, mismatch.stored, mismatch.actual)
```

Repair checksums after intentional file edits:

```python
repaired = migrator.repair()
print(repaired)
```

`repair()` does not execute migration SQL. It updates checksum metadata for applied migrations whose current files exist.

## Baseline

```python
names = migrator.baseline()
```

This records existing `.sql` files as baseline rows without executing them. It requires an empty `db_migrations` table.

## MigrationLock

The CLI uses `MigrationLock` automatically on `up`, `rollback`, and `baseline` unless locking is disabled.

You can also use it directly:

```python
from py_clickhouse_migrator import Migrator, MigrationLock

migrator = Migrator(
    database_url="clickhouse://default@localhost:9000/mydb",
    migrations_dir="./db/migrations",
)

with MigrationLock(
    client=migrator.ch_client,
    db=migrator.get_db_name(),
    ttl=600,
    retry_count=3,
):
    migrator.up()
```

## Public exports

The package exports:

```python
from py_clickhouse_migrator import (
    Migrator,
    MigrationLock,
    LockError,
    LockTimeoutError,
    ChecksumMismatchError,
    ClickHouseServerIsNotHealthyError,
    DatabaseNotFoundError,
    InvalidMigrationError,
    MigrationDirectoryNotFoundError,
    MissingDatabaseUrlError,
    create_migration_file,
    create_migrations_dir,
    make_migration_filename,
    compute_checksum,
    normalize_content,
)
```

The CLI remains the recommended integration path for deployment automation.
