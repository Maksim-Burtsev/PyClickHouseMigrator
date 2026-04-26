# Cluster mode

PyClickHouseMigrator can create its own service tables in cluster-aware mode.

Set the cluster name with a CLI flag:

```sh
migrator --cluster my_cluster up
```

Or with an environment variable:

```sh
export CLICKHOUSE_MIGRATE_CLUSTER=my_cluster
migrator up
```

## What cluster mode changes

When `--cluster` / `CLICKHOUSE_MIGRATE_CLUSTER` is set, the migrator changes how it creates and writes to its own service tables.

### `db_migrations`

In single-node mode, the migration ledger uses `MergeTree()`.

In cluster mode, the migration ledger uses a replicated engine and is created with `ON CLUSTER`:

```sql
CREATE TABLE IF NOT EXISTS db_migrations ON CLUSTER my_cluster (...)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY dt
```

### `_migrations_lock`

In single-node mode, the lock table uses `ReplacingMergeTree(locked_at)`.

In cluster mode, the lock table uses a replicated replacing engine and is created with `ON CLUSTER`:

```sql
CREATE TABLE IF NOT EXISTS mydb._migrations_lock ON CLUSTER my_cluster (...)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', locked_at)
ORDER BY lock_id
```

### Consistency settings

Writes to service tables use cluster-oriented settings:

```text
insert_quorum = auto
select_sequential_consistency = 1
```

These settings are applied to migrator service table operations when cluster mode is enabled.

## What cluster mode does not change

Cluster mode does not rewrite user migration SQL.

This is important:

```text
PyClickHouseMigrator executes your migration SQL exactly as written.
```

If your ClickHouse DDL must run on all cluster hosts, include `ON CLUSTER` in the migration yourself.

Example:

```sql
-- migrator:up
-- @stmt
CREATE TABLE IF NOT EXISTS events ON CLUSTER my_cluster
(
    id UInt64,
    created_at DateTime,
    event_name String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (created_at, id)

-- migrator:down
-- @stmt
DROP TABLE IF EXISTS events ON CLUSTER my_cluster
```

## Recommended cluster workflow

Use one migration runner process per deployment.

Good examples:

- one CI/CD migration job;
- one Kubernetes Job;
- one deployment step that runs before application rollout.

Avoid starting migration commands independently on several application pods.

The advisory lock protects common concurrency mistakes, but a single migration runner is still the cleanest operational model.

## Cluster name validation

Cluster names must look like SQL identifiers:

```text
[a-zA-Z_][a-zA-Z0-9_]*
```

Examples:

```text
my_cluster
prod_ch_cluster
cluster1
```

Invalid examples:

```text
prod-cluster
prod.cluster
cluster name
```

## Common mistake

This migration creates a table only on the current server:

```sql
-- migrator:up
-- @stmt
CREATE TABLE IF NOT EXISTS events
(
    id UInt64
)
ENGINE = MergeTree
ORDER BY id
```

This migration asks ClickHouse to create the table across the cluster:

```sql
-- migrator:up
-- @stmt
CREATE TABLE IF NOT EXISTS events ON CLUSTER my_cluster
(
    id UInt64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY id
```

PyClickHouseMigrator does not decide which one is right for your schema. It only executes the SQL you wrote and tracks migration state.
