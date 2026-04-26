# Known limitations

PyClickHouseMigrator is intentionally small. It tracks and applies migrations; it does not try to become a schema platform.

This page documents the boundaries clearly.

## No DDL transactions

ClickHouse DDL is not wrapped in a transaction by the migrator.

If a migration has several statement blocks and one of the later blocks fails, earlier blocks may already be applied.

Example:

```sql
-- migrator:up
-- @stmt
CREATE TABLE IF NOT EXISTS a (id UInt64) ENGINE = MergeTree ORDER BY id

-- @stmt
CREATE TABLE broken syntax
```

The first table may already exist after the second statement fails.

Use idempotent DDL where practical:

```sql
CREATE TABLE IF NOT EXISTS ...
ALTER TABLE ... ADD COLUMN IF NOT EXISTS ...
DROP TABLE IF EXISTS ...
```

## One statement block equals one query

Each `-- @stmt` block is executed as one ClickHouse query.

The migrator does not split blocks by semicolon.

This avoids fragile parsing, but it also means you should not place several queries in one block.

## SQL is trusted input

Migration files are trusted input.

PyClickHouseMigrator executes SQL as written. It does not sandbox SQL, restrict DDL, prevent destructive operations, or inspect whether a migration is safe for your production workload.

Review migrations the same way you review other production database changes.

## No schema diff

The tool does not inspect ClickHouse and generate migrations.

It is not a declarative schema management system. You write migration SQL explicitly and keep it in Git.

## No rollback generation

The tool does not generate rollback SQL.

You write the `-- migrator:down` section yourself. The migrator stores that rollback SQL when the migration is applied and uses the stored version during rollback.

## Baseline is not schema validation

`migrator baseline` marks existing `.sql` files as already applied (`baseline` rows). It does not compare those files with the actual ClickHouse schema.

Use baseline as an adoption tool, not as a schema audit.

## Target database must exist

The migrator creates its own service tables in the target database, but it does not create the database itself.

Create the database manually before running migrations.

## Advisory lock is best-effort

The lock is designed to protect common concurrent execution cases, especially CI/CD races.

It is still best practice to run migrations from a single runner per deployment.

Use:

```sh
migrator lock-info
migrator force-unlock
```

for operational recovery.

## Cluster mode does not rewrite user SQL

When `--cluster` is set, migrator service tables are created with `ON CLUSTER` and replicated engines.

User migration SQL is still executed exactly as written.

If your DDL must run across the ClickHouse cluster, include `ON CLUSTER` yourself.

## Preflight validation is best-effort

The migrator uses `EXPLAIN AST` by default before `up` and `rollback` execution.

This can catch many SQL problems early, but it does not guarantee execution success and does not evaluate operational safety.

Disable it when necessary:

```sh
migrator up --no-validate
```

## No deployment orchestration

PyClickHouseMigrator does not provide:

- web UI;
- approvals;
- RBAC;
- deployment scheduling;
- schema drift dashboards;
- automatic application rollout coordination.

It is meant to be a focused migration runner that fits into tools you already use.
