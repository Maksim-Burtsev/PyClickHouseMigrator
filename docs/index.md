# PyClickHouseMigrator

PyClickHouseMigrator is a SQL-first ClickHouse migration runner for teams that keep schema changes in Git and run them through a small, predictable CLI.

It stores migration state inside ClickHouse, validates checksums, supports rollback SQL, handles advisory locking, and works with single-node or cluster deployments.

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

## Guides

- [Migration format](migration-format.md)
- [Baseline existing databases](baseline.md)
- [Cluster mode](cluster-mode.md)
- [CI/CD usage](ci-cd.md)
- [Docker usage](docker.md)
- [Python API](python-api.md)
- [Troubleshooting](troubleshooting.md)
- [Known limitations](known-limitations.md)
- [2.0 release notes](release-2.0.md)

## Examples

- [Basic migration](examples/basic-migration.sql)
- [Cluster migration](examples/cluster-migration.sql)
- [GitHub Actions workflow](examples/github-actions.yml)
- [Kubernetes Job](examples/kubernetes-job.yml)
