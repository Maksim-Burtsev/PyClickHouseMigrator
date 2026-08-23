# Security Policy

## Supported versions

| Version | Supported |
| ------- | --------- |
| 2.x     | ✅ |
| 1.x     | ❌ |
| < 1.0   | ❌ |

Security fixes are released for the latest `2.x` version only.

## Reporting a vulnerability

Please **do not open a public issue** for security problems.

Report privately through GitHub Security Advisories:
[Report a vulnerability](https://github.com/Maksim-Burtsev/PyClickHouseMigrator/security/advisories/new)

Include the version, ClickHouse version, a description of the problem, and a
reproduction if you have one. You should get a first response within a week.

## Scope

PyClickHouseMigrator connects to ClickHouse and executes SQL you wrote. The
migration SQL itself is trusted input — this tool does not sandbox it. Relevant
security concerns are things like credential handling (`--url`,
`CLICKHOUSE_MIGRATE_URL`, `.env` loading), credential leakage into logs, and
checksum/lock validation being bypassable.
