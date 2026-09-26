# Changelog

All notable changes to this project are documented here.
This project follows [Semantic Versioning](https://semver.org/): breaking changes only in major releases.

## Unreleased

- `migrator --help` and `migrator <command> --help` now show a one-line description of each command
- Internal refactoring to pass wemake-python-styleguide and a stricter ruff and mypy setup; the migration format, CLI options and output, and the Python API are unchanged

## 2.1.0 — 2026-08-23

- Published the image to GitHub Container Registry (`ghcr.io/maksim-burtsev/pyclickhousemigrator`) alongside Docker Hub, with identical tags and both architectures
- Added ClickHouse 24.8 and 25.3 to the CI test matrix; previously only `latest` was tested
- Fixed the `org.opencontainers.image.licenses` label in the image (the singular `license` key is not valid OCI and was ignored) and added title, description, and documentation labels
- Added `SECURITY.md`, `CONTRIBUTING.md`, issue forms, and a pull request template
- Renamed `LICENCE.txt` to `LICENSE` so GitHub detects the license, and `CHANGELOG.txt` to `CHANGELOG.md`
- Added a demo GIF to the README, rendered from a VHS tape in CI
- Documented the comparison with golang-migrate, Atlas, dbt, and Alembic
- No CLI or migration behavior changes

## 2.0.1 — 2026-08-02

- Added the hosted documentation site and linked it from README and package metadata
- Added the `py.typed` marker for typed-package discovery
- Fixed release Docker images to install the matching package version
- Refreshed PyPI summary, keywords, project links, and CLI installation examples
- Updated CI and documentation workflows
- No CLI or migration behavior changes

## 2.0.0 — 2026-04-26

- SQL-first migration format: migrations are `.sql` files with `-- migrator:up`, `-- migrator:down`, and explicit `-- @stmt` blocks
- Removed the old documented Python migration workflow from user-facing documentation
- No blind SQL splitting by semicolon: each `-- @stmt` block is executed as one ClickHouse query
- Added baseline workflow for adopting existing databases without executing historical migrations
- Checksum validation is based on parsed statement blocks
- Added preflight validation with `EXPLAIN AST` before `up` and `rollback`; dry-run uses the same validation path and can opt out with `--no-validate`
- Improved migration status output with HEAD, baseline, modified, and missing markers
- Improved missing migrations directory handling
- Hardened lock cluster name validation
- Full documentation refresh for README, llms.txt, llms-full.txt, and docs/*

## 1.1.0 — 2026-03-30

- New --send-receive-timeout option
- Docker image (Docker Hub)
- llms.txt / llms-full.txt
- Distributed lock fixes (ownership check, server-side timestamps, UUID suffix, TTL 600s)
- Checksum computed from SQL output instead of file content
- Removed unused termcolor dependency

## 1.0.0 — 2026-03-22

- Distributed locking with TTL
- Checksum validation & repair
- Cluster support (ON CLUSTER)
- Dry-run mode
- Connect retries
- Input validation
- CLI error handling
- --version flag

## 0.3 — 2024-03-19

- Fix queries parsing

## 0.2 — 2023-12-26

- Add .env loading

## 0.1 — 2023-12-24

- First release
