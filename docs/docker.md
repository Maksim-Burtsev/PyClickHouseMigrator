# Docker usage

PyClickHouseMigrator can run as a containerized CLI.

## Pull image

```sh
docker pull maksimburtsev/py-clickhouse-migrator:latest
```

Use `latest` for a quick start. For repeatable automation, pin to a major version tag:

```text
maksimburtsev/py-clickhouse-migrator:2
```

Or pin an exact version:

```text
maksimburtsev/py-clickhouse-migrator:2.0.0
```

## Run migrations

Mount your migrations directory to `/migrations`:

```sh
docker run --rm \
  -v "$PWD/db/migrations:/migrations" \
  -e CLICKHOUSE_MIGRATE_URL=clickhouse://default@clickhouse:9000/mydb \
  maksimburtsev/py-clickhouse-migrator:latest \
  up
```

The Docker image sets:

```text
CLICKHOUSE_MIGRATE_DIR=/migrations
```

So you do not need to pass `--path /migrations` unless you want to override it.

## Preview migrations

```sh
docker run --rm \
  -v "$PWD/db/migrations:/migrations" \
  -e CLICKHOUSE_MIGRATE_URL=clickhouse://default@clickhouse:9000/mydb \
  maksimburtsev/py-clickhouse-migrator:latest \
  up --dry-run
```

## Initialize migrations directory locally

`init` works offline, but if you run it through Docker you need to mount the parent directory:

```sh
docker run --rm \
  -v "$PWD:/workspace" \
  -w /workspace \
  maksimburtsev/py-clickhouse-migrator:latest \
  --path ./db/migrations init
```

## Create a migration locally

```sh
docker run --rm \
  -v "$PWD:/workspace" \
  -w /workspace \
  maksimburtsev/py-clickhouse-migrator:latest \
  --path ./db/migrations new create_users_table
```

## Docker Compose example

```yaml
services:
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    ports:
      - "9000:9000"
      - "8123:8123"

  migrations:
    image: maksimburtsev/py-clickhouse-migrator:latest
    depends_on:
      - clickhouse
    environment:
      CLICKHOUSE_MIGRATE_URL: clickhouse://default@clickhouse:9000/default
      CLICKHOUSE_MIGRATE_CONNECT_RETRIES: "30"
      CLICKHOUSE_MIGRATE_CONNECT_RETRIES_INTERVAL: "2"
    volumes:
      - ./db/migrations:/migrations
    command: ["up"]
```

## Cluster mode

```sh
docker run --rm \
  -v "$PWD/db/migrations:/migrations" \
  -e CLICKHOUSE_MIGRATE_URL=clickhouse://default@clickhouse:9000/mydb \
  -e CLICKHOUSE_MIGRATE_CLUSTER=my_cluster \
  maksimburtsev/py-clickhouse-migrator:latest \
  up
```

Remember: cluster mode affects migrator service tables. Your migration SQL is still executed as written. Include `ON CLUSTER` in your own DDL when needed.
