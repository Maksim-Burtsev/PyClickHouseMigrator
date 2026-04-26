# CI/CD usage

PyClickHouseMigrator is designed to run as a simple deployment step.

The usual CI/CD command is:

```sh
migrator up
```

If the command succeeds, the deployment can continue. If it fails, the deployment should stop.

## Exit codes

The intended CLI contract is simple:

| Exit code | Meaning |
|---:|---|
| `0` | Command completed successfully. |
| `1` | Command failed with a handled migration error. |

Handled errors include invalid migration files, ClickHouse connection failures, missing database errors, checksum mismatches, lock errors, baseline errors, and migration directory errors.

Unexpected Python/runtime failures may also exit non-zero.

## GitHub Actions

```yaml
name: Run ClickHouse migrations

on:
  workflow_dispatch:
  push:
    branches: [master]

jobs:
  migrate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6

      - uses: actions/setup-python@v6
        with:
          python-version: '3.14'

      - name: Install migrator
        run: pip install "py-clickhouse-migrator>=2,<3"

      - name: Preview migrations
        run: migrator up --dry-run
        env:
          CLICKHOUSE_MIGRATE_URL: ${{ secrets.CLICKHOUSE_MIGRATE_URL }}

      - name: Apply migrations
        run: migrator up
        env:
          CLICKHOUSE_MIGRATE_URL: ${{ secrets.CLICKHOUSE_MIGRATE_URL }}
```

For production deployments, some teams remove the preview step and run only `migrator up`.

## Docker in CI

```sh
docker run --rm \
  -v "$PWD/db/migrations:/migrations" \
  -e CLICKHOUSE_MIGRATE_URL="$CLICKHOUSE_MIGRATE_URL" \
  maksimburtsev/py-clickhouse-migrator:2 \
  up
```

The Docker image uses `/migrations` as the default migrations directory.

## Kubernetes Job

A migration Job is a clean way to ensure a single runner applies migrations before application rollout.

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: clickhouse-migrations
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: migrator
          image: maksimburtsev/py-clickhouse-migrator:2
          args: ["up"]
          env:
            - name: CLICKHOUSE_MIGRATE_URL
              valueFrom:
                secretKeyRef:
                  name: clickhouse-migrations
                  key: url
          volumeMounts:
            - name: migrations
              mountPath: /migrations
      volumes:
        - name: migrations
          configMap:
            name: clickhouse-migrations
```

In real deployments, you may package migrations into your application image or mount them from another source instead of using a ConfigMap.

## Recommended deployment order

A typical flow:

1. Build and test application code.
2. Run `migrator up` once.
3. Deploy application code that depends on the new schema.

For backward-compatible schema changes, this fits most rolling deployment strategies.

For destructive schema changes, use the same multi-step discipline you would use with any database:

1. deploy additive migration;
2. deploy application code that no longer depends on old schema;
3. deploy cleanup migration later.

PyClickHouseMigrator does not judge whether a migration is safe for production. It executes and tracks the SQL you provide.

## Concurrency

`up`, `rollback`, and `baseline` use a migration lock by default.

The lock helps when two runners start at the same time, but CI/CD should still be configured so migrations normally run from one place.

Recommended:

```text
one deployment = one migration runner
```

## Dry run

Use dry-run to print pending migration SQL without executing it:

```sh
migrator up --dry-run
```

Dry-run still performs checksum checks and preflight validation unless disabled.

```sh
migrator up --dry-run --no-validate
```

## Timeouts and retries

If ClickHouse may not be ready when the migration step starts:

```sh
migrator \
  --connect-retries 30 \
  --connect-retries-interval 2 \
  up
```

For long-running DDL, increase the client send/receive timeout:

```sh
migrator --send-receive-timeout 1800 up
```

If you increase the query timeout, consider increasing the lock TTL as well:

```sh
migrator --send-receive-timeout 1800 up --lock-ttl 2100
```
