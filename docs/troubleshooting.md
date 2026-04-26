# Troubleshooting

This guide covers common problems and the fastest way to diagnose them.

## `ClickHouse url was not provided`

Commands that touch migration state require a ClickHouse URL.

Set it with an environment variable:

```sh
export CLICKHOUSE_MIGRATE_URL=clickhouse://default@localhost:9000/mydb
```

Or pass it explicitly:

```sh
migrator --url clickhouse://default@localhost:9000/mydb up
```

`init` and `new` work offline. `up`, `rollback`, `show`, `baseline`, `repair`, `lock-info`, and `force-unlock` require a connection.

## `Database '...' does not exist`

PyClickHouseMigrator does not create the target database.

Create it manually before running migrations:

```sql
CREATE DATABASE mydb
```

Then run:

```sh
migrator up
```

## Migration directory not found

Initialize it:

```sh
migrator init
```

Or specify a different path:

```sh
migrator --path ./migrations up
```

Equivalent environment variable:

```sh
export CLICKHOUSE_MIGRATE_DIR=./migrations
```

## Invalid migration format

A migration must contain:

```sql
-- migrator:up
-- @stmt
...

-- migrator:down
-- @stmt
...
```

Common mistakes:

- missing `-- migrator:up`;
- missing `-- migrator:down`;
- SQL outside a `-- @stmt` block;
- `-- migrator:down` before `-- migrator:up`;
- empty `up` section.

Check the [Migration format](migration-format.md) guide.

## Preflight validation failed

By default, the migrator validates statements with `EXPLAIN AST` before execution.

If validation fails, inspect the statement printed in the error message. It may be invalid SQL, refer to missing objects, or be a statement that does not behave well with preflight validation in your ClickHouse version.

You can disable preflight validation:

```sh
migrator up --no-validate
migrator rollback --no-validate
```

Disabling validation does not make execution safer. It only skips the preflight step.

## Checksum mismatch

This means already-applied migration file(s) changed or are missing locally.

Check status:

```sh
migrator show
```

If the edit was accidental, restore the original file from Git.

If the edit was intentional, the database state is still consistent, and you only want to accept the current file content for future checksum checks:

```sh
migrator repair
```

For a one-time run without failing on checksum mismatch:

```sh
migrator up --allow-dirty
```

Prefer `repair` only when you are confident the stored database state and the edited file are still consistent.

## Migration lock is held

Inspect the lock:

```sh
migrator lock-info
```

If the process is still running, wait for it to finish.

If the process crashed, the lock will expire after its TTL. You can also release it manually:

```sh
migrator force-unlock
```

For long-running DDL, increase the lock TTL:

```sh
migrator up --lock-ttl 1800
```

If you also increase the ClickHouse client timeout, increase the lock TTL accordingly:

```sh
migrator --send-receive-timeout 1800 up --lock-ttl 2100
```

## No pending migrations

This is a normal successful state:

```text
There are no migrations to apply.
```

Use `show` to confirm:

```sh
migrator show
```

## A migration failed halfway

ClickHouse DDL is not transactional. If a migration has several `-- @stmt` blocks and one fails, earlier blocks may already be applied.

Recommended recovery flow:

1. Inspect ClickHouse state manually.
2. Decide whether to complete the migration manually, revert it manually, or edit the migration to be safely re-runnable.
3. Re-run `migrator up` when the migration file and database state are consistent.
4. Use `migrator repair` only if you intentionally changed already-applied migration file(s) and need to update stored checksums.

Use `IF EXISTS` and `IF NOT EXISTS` in migration SQL where appropriate to make recovery easier.

## Cluster migration ran only on one server

Cluster mode does not rewrite your migration SQL.

If a DDL statement must run across the cluster, include `ON CLUSTER` in the migration:

```sql
CREATE TABLE IF NOT EXISTS events ON CLUSTER my_cluster (...)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY id
```

The `--cluster` option affects migrator service tables. It does not inject `ON CLUSTER` into user DDL.

## Docker container cannot find migrations

Mount your migrations directory to `/migrations`:

```sh
docker run --rm \
  -v "$PWD/db/migrations:/migrations" \
  -e CLICKHOUSE_MIGRATE_URL=clickhouse://default@clickhouse:9000/mydb \
  maksimburtsev/py-clickhouse-migrator:latest \
  up
```

Inside the Docker image, `CLICKHOUSE_MIGRATE_DIR` defaults to `/migrations`.
