<p align="center">
  <img src="https://raw.githubusercontent.com/Maksim-Burtsev/PyClickHouseMigrator/master/assets/logo.png" alt="PyClickHouseMigrator" width="200">
</p>

# PyClickHouseMigrator

[![CI](https://github.com/Maksim-Burtsev/PyClickHouseMigrator/actions/workflows/ci.yml/badge.svg)](https://github.com/Maksim-Burtsev/PyClickHouseMigrator/actions)
[![PyPI](https://img.shields.io/pypi/v/py-clickhouse-migrator)](https://pypi.org/project/py-clickhouse-migrator/)
[![Python](https://img.shields.io/badge/python-3.11%20|%203.12%20|%203.13%20|%203.14-blue)](https://pypi.org/project/py-clickhouse-migrator/)
[![codecov](https://codecov.io/gh/Maksim-Burtsev/PyClickHouseMigrator/branch/master/graph/badge.svg)](https://codecov.io/gh/Maksim-Burtsev/PyClickHouseMigrator)
[![Downloads](https://static.pepy.tech/personalized-badge/py-clickhouse-migrator?period=total&units=INTERNATIONAL_SYSTEM&left_color=grey&right_color=brightgreen&left_text=downloads)](https://pepy.tech/projects/py-clickhouse-migrator)

Lightweight ClickHouse schema migration tool for SQL-first workflows. Minimal dependencies, no ORM.

Features: SQL migration files, checksum validation, baseline workflow for existing databases, dry-run mode, best-effort preflight validation, cluster support, rollback, status dashboard, connect retries, and concurrent execution protection.

## Install

```sh
pip install py-clickhouse-migrator
```

## Quick Start

```sh
export CLICKHOUSE_MIGRATE_URL=clickhouse://default@localhost:9000/mydb

migrator init
migrator new create_users_table
# edit the generated .sql file
migrator up
migrator show
```

`init` and `new` work offline. Other commands require a ClickHouse connection.

## Comparison

| Approach | Good at | Tradeoffs |
|----------|---------|-----------|
| Hand-written SQL scripts | Maximum flexibility, no tooling overhead | No applied-history ledger, no checksum validation, no built-in rollback workflow |
| GUI DB tools / schema export | Fast inspection and one-off schema export | Not a migration workflow, no deterministic apply history, no CLI-first deployment path |
| General migration frameworks | Rich ecosystems, cross-database abstractions | More moving parts, often less ClickHouse-native, often heavier than needed |
| PyClickHouseMigrator | Lightweight SQL-first ClickHouse migrations, baseline, checksums, dry-run, locking | No schema diff engine, no ORM layer, no GUI |

## Migration Format

Migrations are `.sql` files discovered from the migrations directory. Canonical filename format:

```text
YYYYMMDDHHMMSS_name.sql
```

Canonical file format:

```sql
-- migrator:up
-- @stmt
CREATE TABLE IF NOT EXISTS users (
    id UInt64,
    name String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id;

-- migrator:down
-- @stmt
DROP TABLE IF EXISTS users;
```

Rules:

- Only `.sql` files are discovered.
- Each file must contain exactly one `-- migrator:up` section and one `-- migrator:down` section.
- Every non-empty SQL statement block must start with `-- @stmt`.
- Multiple statements use multiple `-- @stmt` blocks.
- Empty `down` is allowed.
- Pending migrations are applied in `sorted(filename)` order.

Example with multiple statements:

```sql
-- migrator:up
-- @stmt
CREATE TABLE IF NOT EXISTS users (
    id UInt64,
    name String
) ENGINE = MergeTree()
ORDER BY id;

-- @stmt
CREATE TABLE IF NOT EXISTS events (
    id UInt64,
    user_id UInt64
) ENGINE = MergeTree()
ORDER BY id;

-- migrator:down
-- @stmt
DROP TABLE IF EXISTS events;

-- @stmt
DROP TABLE IF EXISTS users;
```

## Commands

### `init`

Create the migrations directory (default `./db/migrations`).

```sh
migrator init
migrator --path ./my/migrations init
```

### `new`

Create a new timestamped SQL migration file.

```sh
migrator new create_users_table
migrator new
```

Name is optional. Only letters, digits, and underscores are allowed.

Generated template:

```sql
-- migrator:up
-- @stmt


-- migrator:down
-- @stmt
```

### `up`

Apply pending migrations.

```sh
migrator up
migrator up 3
migrator up --dry-run
migrator up --no-validate
migrator up --allow-dirty
```

| Option | Default | Description |
|--------|---------|-------------|
| `N` | all | Number of migrations to apply |
| `--lock / --no-lock` | `--lock` | Enable or disable distributed lock |
| `--lock-ttl` | `600` | Lock TTL in seconds |
| `--lock-retry` | `3` | Lock acquire retry attempts |
| `--dry-run` | off | Print migration SQL without executing it |
| `--validate / --no-validate` | `--validate` | Enable or disable preflight validation |
| `--allow-dirty` | off | Skip checksum validation for modified files |

Example output:

```text
20250318090000_create_users.sql applied [✔]
20250319120000_create_events.sql applied [✔]
```

### `rollback`

Rollback applied migrations in reverse order.

```sh
migrator rollback
migrator rollback 3
migrator rollback --dry-run
migrator rollback --no-validate
```

| Option | Default | Description |
|--------|---------|-------------|
| `N` | `1` | Number of migrations to rollback |
| `--lock / --no-lock` | `--lock` | Enable or disable distributed lock |
| `--lock-ttl` | `600` | Lock TTL in seconds |
| `--lock-retry` | `3` | Lock acquire retry attempts |
| `--dry-run` | off | Print rollback SQL without executing it |
| `--validate / --no-validate` | `--validate` | Enable or disable preflight validation |

Example output:

```text
20250319120000_create_events.sql rolled back [✔].
```

### `show`

Display migration status, integrity info, and the current HEAD.

```sh
migrator show
migrator show --all
```

| Option | Default | Description |
|--------|---------|-------------|
| `--all` | off | Show all applied migrations instead of only the latest 5 |

Example output:

```text
Applied:
  [X] 20250320143022_existing_schema.sql (HEAD, baseline)
  [X] 20250319120000_create_events.sql

Pending:
  [ ] 20250321100000_add_status_column.sql

Applied: 2 | Pending: 1
```

Behavior:

- `HEAD` marks the most recently recorded applied migration.
- `baseline` marks migrations stamped by `baseline`.
- `modified` means checksum mismatch.
- `missing` means an applied migration file no longer exists on disk.
- Integrity warnings are printed to `stderr`.

### `baseline`

Mark all current `.sql` migration files as already applied without executing their SQL. Use this to start managing an existing database.

```sh
migrator baseline
migrator baseline --no-lock
```

| Option | Default | Description |
|--------|---------|-------------|
| `--lock / --no-lock` | `--lock` | Enable or disable distributed lock |
| `--lock-ttl` | `600` | Lock TTL in seconds |
| `--lock-retry` | `3` | Lock acquire retry attempts |

Semantics:

- `baseline` requires an empty `db_migrations` table.
- `baseline` writes ledger rows only.
- `baseline` does not execute migration SQL.
- Later `up` applies only new migrations.
- `rollback` ignores rows recorded as `baseline`.

`dry-run` and `baseline` solve different problems:

- `--dry-run` prints and optionally validates SQL, but writes nothing.
- `baseline` writes ledger state, but executes no migration SQL.

## Existing Database Baseline

Use `baseline` when the schema already exists in ClickHouse and you want to start managing future changes with the migrator.

Recommended flow:

1. Create a migrations directory with SQL files that represent the schema state you want to treat as the starting point.
2. Verify the target database already contains that schema.
3. Run `baseline` once to stamp the ledger without executing any SQL.
4. Add new migration files for later changes and use `up` from that point forward.

Example:

```sh
export CLICKHOUSE_MIGRATE_URL=clickhouse://default@localhost:9000/mydb

migrator init
migrator new initial_schema
# replace template with the SQL that describes the existing schema

migrator baseline
migrator show
```

Expected result:

- existing objects in ClickHouse are left untouched
- the SQL files are recorded in `db_migrations` as `baseline`
- the next `migrator up` run applies only newer migration files

### `repair`

Update stored checksums in `db_migrations` to match current migration files.

```sh
migrator repair
```

Use after intentionally editing an already-applied migration.

### `force-unlock`

Manually release a stuck migration lock.

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
| `--cluster` | `CLICKHOUSE_MIGRATE_CLUSTER` | — | Cluster name for `ON CLUSTER` service tables |
| `--connect-retries` | `CLICKHOUSE_MIGRATE_CONNECT_RETRIES` | `0` | Connection retry attempts |
| `--connect-retries-interval` | `CLICKHOUSE_MIGRATE_CONNECT_RETRIES_INTERVAL` | `1` | Seconds between retries |
| `--send-receive-timeout` | `CLICKHOUSE_MIGRATE_SEND_RECEIVE_TIMEOUT` | `600` | Query timeout in seconds |
| `-v, --verbose` | — | off | Enable DEBUG logging |
| `-q, --quiet` | — | off | Suppress logger output except errors |

Only global options have environment variable equivalents. Per-command options such as `--dry-run`, `--lock-ttl`, and `--all` are CLI-only.

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

The image sets `CLICKHOUSE_MIGRATE_DIR=/migrations` by default.

Pin to a major version tag (`:1`) or an exact version (`:1.1.0`).

## Deployment Recipes

### Docker Compose

Example `compose.yaml`:

```yaml
services:
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    ports:
      - "9000:9000"
      - "8123:8123"
    environment:
      CLICKHOUSE_DB: mydb
      CLICKHOUSE_USER: default
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: 1
    healthcheck:
      test: ["CMD", "clickhouse-client", "--query", "SELECT 1"]
      interval: 2s
      timeout: 5s
      retries: 10

  migrator:
    image: maksimburtsev/py-clickhouse-migrator:1
    depends_on:
      clickhouse:
        condition: service_healthy
    volumes:
      - ./db/migrations:/migrations:ro
    environment:
      CLICKHOUSE_MIGRATE_URL: clickhouse://default@clickhouse:9000/mydb
    entrypoint: ["migrator"]
```

Run migrations:

```sh
docker compose run --rm migrator up
```

Baseline an existing database:

```sh
docker compose run --rm migrator baseline
```

### Kubernetes Job

For production-style deployments, prefer a dedicated Job. It gives the cleanest execution model and avoids tying schema changes to application pod startup.

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: clickhouse-migrate
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: migrator
          image: maksimburtsev/py-clickhouse-migrator:1
          args: ["up"]
          env:
            - name: CLICKHOUSE_MIGRATE_URL
              value: clickhouse://default@clickhouse:9000/mydb
          volumeMounts:
            - name: migrations
              mountPath: /migrations
              readOnly: true
      volumes:
        - name: migrations
          configMap:
            name: app-migrations
```

Use a Job when you want one explicit migration step per deploy.

### Kubernetes `initContainer`

An `initContainer` works when application startup must be blocked until schema changes are applied, but it is less explicit than a separate Job.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
spec:
  replicas: 1
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      initContainers:
        - name: migrator
          image: maksimburtsev/py-clickhouse-migrator:1
          args: ["up"]
          env:
            - name: CLICKHOUSE_MIGRATE_URL
              value: clickhouse://default@clickhouse:9000/mydb
          volumeMounts:
            - name: migrations
              mountPath: /migrations
              readOnly: true
      containers:
        - name: app
          image: ghcr.io/example/my-app:latest
      volumes:
        - name: migrations
          configMap:
            name: app-migrations
```

Prefer a Job if you want clearer operational ownership or stricter control over retries and rollout order.

## Locking

When multiple processes run `migrator up`, `rollback`, or `baseline` simultaneously, the advisory lock helps prevent conflicting writes. Locking is enabled by default on those commands.

The lock uses a dedicated table with expiration-based staleness handling and optional retries. If you increase `--send-receive-timeout`, increase `--lock-ttl` accordingly.

If a deployment crashes mid-migration and the lock is not released, use `lock-info` to inspect and `force-unlock` to release it manually. Expired locks are ignored automatically during acquisition.

```sh
migrator up --no-lock
migrator up --lock-ttl 600
migrator up --lock-retry 5
```

## Checksum Validation

After a migration is applied, the tool stores a checksum in `db_migrations`. The checksum is computed from the parsed statement lists in `up` and `down`, not from raw file bytes.

Implications:

- service markers like `-- @stmt` do not count as semantic changes by themselves
- edited SQL in an already-applied migration is detected on later `up`
- `repair` updates stored checksums to match current files
- baseline rows are excluded from checksum validation

## Cluster Support

When `--cluster` is set, the migrator creates its own service tables (`db_migrations`, `_migrations_lock`) with `ON CLUSTER` and replicated engines. Your migration SQL is executed as-is. If you need `ON CLUSTER` inside your DDL, include it in the migration yourself.

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

Useful methods:

- `up()`
- `rollback()`
- `baseline()`
- `show_migrations()`
- `validate_checksums()`
- `repair()`

## Contributing

Local setup:

```sh
uv sync --dev
pre-commit install
```

Useful commands:

```sh
make lint
make test
make test-cluster
```

What they do:

- `make lint` runs `ruff`, `ruff format --check`, and `mypy`
- `make test` starts a local ClickHouse via `docker compose`, runs the default test suite, then tears it down
- `make test-cluster` starts the 2-node ClickHouse cluster, waits for readiness, runs cluster tests, then tears it down

The CI workflow runs:

- lint
- tests on Python `3.11`, `3.12`, `3.13`, `3.14`
- cluster tests

## Further Reading

- [2.0 Migration Guide](docs/2.0-migration-guide.md)

## Known Limitations

**Only `.sql` files are discovered.** Legacy `.py` migrations are outside the supported workflow and are not executed by the current runner.

**Statement markers are mandatory.** Non-empty SQL outside `-- @stmt` blocks is treated as an invalid migration format.

**No DDL transactions.** If a migration with multiple statements fails halfway, some statements may already be applied. Prefer idempotent DDL such as `IF NOT EXISTS` / `IF EXISTS`.

**Advisory lock is best-effort.** There is still a race window between insert and verification. For stricter guarantees, run migrations from a single orchestrated process.

**Target database must already exist.** The migrator creates service tables, not the database itself.
