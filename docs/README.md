# PyClickHouseMigrator documentation

PyClickHouseMigrator is a SQL-first ClickHouse migration runner for teams that want migration files in Git and a simple, reliable CLI in CI/CD.

Start with the root [README](../README.md) if you are new to the project.

## Guides

- [Migration format](migration-format.md) — `.sql` file structure, `-- @stmt` blocks, examples, parser rules.
- [Baseline existing databases](baseline.md) — how to adopt a database that already has schema objects.
- [Cluster mode](cluster-mode.md) — service tables, replicated engines, and `ON CLUSTER` boundaries.
- [CI/CD usage](ci-cd.md) — GitHub Actions, Kubernetes Job, exit codes, deployment recommendations.
- [Docker usage](docker.md) — running the migrator in containers.
- [Python API](python-api.md) — using the migrator from Python code.
- [Troubleshooting](troubleshooting.md) — common errors and recovery steps.
- [Known limitations](known-limitations.md) — what the tool intentionally does and does not guarantee.
- [2.0 release notes](release-2.0.md) — SQL-first release notes and breaking changes.

## Examples

- [Basic migration](examples/basic-migration.sql)
- [Cluster migration](examples/cluster-migration.sql)
- [GitHub Actions workflow](examples/github-actions.yml)
- [Kubernetes Job](examples/kubernetes-job.yml)
