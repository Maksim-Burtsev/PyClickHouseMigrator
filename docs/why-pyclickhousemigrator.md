---
title: Why PyClickHouseMigrator?
description: Decide whether PyClickHouseMigrator fits your ClickHouse schema migration workflow, and compare it with raw SQL, Goose, Flyway, Liquibase, and clickhouse-migrations for Python.
---

# Why PyClickHouseMigrator?

PyClickHouseMigrator is a focused migration runner for Python teams that want to write ClickHouse DDL themselves, keep it in Git, and apply it through a small CLI or library.

It is deliberately not an ORM, schema diff engine, database provisioning tool, or deployment platform.

!!! note
    This is a decision guide, not a benchmark. It compares documented operating models and trade-offs. Follow the linked project documentation when evaluating current versions.

## A good fit

Choose PyClickHouseMigrator when your team wants:

- plain SQL migration files with explicit up and down sections;
- exact statement boundaries instead of semicolon-based SQL splitting;
- stored SQL and SHA-256 checksums for applied migrations;
- preflight validation with `EXPLAIN AST` before execution;
- a dry-run that follows the same checksum and validation path;
- baseline adoption for an existing database;
- a small advisory lock for common CI/CD races;
- cluster-aware migrator service tables while keeping user DDL explicit.

It fits particularly well when Python already exists in the delivery environment and the team prefers reviewable SQL over generated schema changes.

## Choose another approach when

Use another tool or process when you need:

- declarative schema inspection and automatic diff generation;
- one migration system already standardized across many database engines;
- a central UI with approvals, RBAC, environment promotion, and audit workflows;
- ORM-generated migrations;
- automatic creation of the target database;
- automatic rewriting of user DDL with `ON CLUSTER`;
- transactional guarantees across multiple ClickHouse DDL statements.

No migration runner can make ClickHouse DDL transactional. Design multi-statement migrations so that partial execution can be diagnosed and safely resumed.

## Compare the operating models

<div class="pcm-comparison" markdown>

| Approach | Consider it when | Important trade-off to evaluate |
| --- | --- | --- |
| **PyClickHouseMigrator** | A Python team wants ClickHouse-only, explicit up/down SQL plus checksum enforcement, preflight validation, baseline, locking, and cluster-aware state. | Requires Python 3.11+, uses `clickhouse-driver`, does not generate schema diffs, and keeps validation and locking deliberately best-effort. |
| **Raw SQL scripts** | The schema is tiny, changes are rare, or the deployment is intentionally one-off. | Your team owns ordering, applied-state tracking, integrity checks, rollback conventions, concurrency control, and recovery from partial execution. |
| [**Goose**](https://github.com/pressly/goose) | A Go or multi-database team wants a mature CLI/library with SQL and optional Go migrations. | Its general-purpose model and ClickHouse cluster workflow should be evaluated against the ClickHouse-specific safeguards your deployment needs. |
| [**Flyway**](https://documentation.red-gate.com/fd/supported-databases-for-flyway-143754067.html) | An organization already standardizes migrations and governance around Flyway or JVM tooling. | ClickHouse is listed for foundational migration capabilities; verify the exact capability and licensing level your workflow requires. |
| [**Liquibase**](https://github.com/MEDIARITHMICS/liquibase-clickhouse) | An organization already uses Liquibase across databases and values one consistent process. | ClickHouse support uses an extension; validate extension compatibility, cluster behavior, and the operational footprint for your environment. |
| [**clickhouse-migrations for Python**](https://pypi.org/project/clickhouse-migrations/) | A Python 3.9+ team wants a file-based runner with native and HTTP drivers, a GitHub Action, Docker image, dry-run, and paired down files. | Its file format and safety model differ. Compare statement splitting, checksum behavior, rollback semantics, validation, baseline, and locking against your requirements. |

</div>

The [ClickHouse schema migration tools guide](https://clickhouse.com/docs/resources/support-center/knowledge-base/tables-schema/schema-migration-tools) gives a broader ecosystem overview and recommends choosing for team familiarity and operating process, not for feature count alone.

## What the migrator records

After a migration succeeds, `db_migrations` stores its name, applied timestamp, up SQL, down SQL, and checksum. Future runs compare applied history with the current files before executing pending SQL.

That model provides two useful boundaries:

1. application schema changes remain ordinary SQL reviewed in Git;
2. migration state remains inspectable inside ClickHouse.

See the [migration format](migration-format.md) for exact parsing and checksum rules.

## Production deployment flow

Run migrations once per environment, before the application rollout:

```text
reviewed migration files
          │
          ▼
dedicated migration job
  connect → lock → verify → validate → apply → record
          │
          ▼
application rollout
```

A typical job uses the same artifact that was reviewed:

```sh
migrator up --dry-run
migrator up
```

Keep a single migration runner even when advisory locking is enabled. The lock reduces common races; it is not a deployment orchestrator.

For concrete pipelines, see [CI/CD usage](ci-cd.md), [Docker usage](docker.md), and [cluster mode](cluster-mode.md).

## Boundaries to accept before adoption

- ClickHouse DDL is not transactional; a later statement can fail after earlier statements ran.
- SQL is trusted input and executes as written.
- rollback SQL is written by the migration author, not generated.
- baseline records history but does not compare it with the live schema.
- preflight validation cannot prove that production execution will succeed or be operationally cheap.
- cluster mode changes migrator service tables, not user migration SQL.

Read [Known limitations](known-limitations.md) before using the tool in production.

## Start with the smallest proof

Install the CLI, create one migration against a disposable ClickHouse database, inspect the dry-run, and apply it:

```sh
uv tool install py-clickhouse-migrator
export CLICKHOUSE_MIGRATE_URL=clickhouse://default@localhost:9000/mydb
migrator init
migrator new create_events
migrator up --dry-run
migrator up
```

Continue with the [migration format](migration-format.md) when that operating model matches your team.
