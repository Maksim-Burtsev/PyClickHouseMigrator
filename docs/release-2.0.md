# PyClickHouseMigrator 2.0 release notes

PyClickHouseMigrator 2.0 is the SQL-first release.

The goal of 2.0 is not to make the tool bigger. The goal is to make the migration contract clearer, safer to document, and easier to adopt in real deployment flows.

## Summary

2.0 centers the product around plain `.sql` migration files:

```sql
-- migrator:up
-- @stmt
CREATE TABLE IF NOT EXISTS users
(
    id UInt64,
    name String
)
ENGINE = MergeTree
ORDER BY id

-- migrator:down
-- @stmt
DROP TABLE IF EXISTS users
```

The migrator now treats explicit statement blocks as the migration format. It does not split SQL by semicolon.

## What changed

### SQL files are the migration format

Migration files are `.sql` files in the migrations directory.

`migrator new` generates SQL templates with:

```text
-- migrator:up
-- @stmt

-- migrator:down
-- @stmt
```

### Explicit statement blocks

Each `-- @stmt` block is executed as one ClickHouse query.

This makes multi-statement migrations explicit and avoids fragile parsing based on semicolons.

### Baseline workflow

`migrator baseline` marks existing migration files as already applied (`baseline` rows) without executing them.

This is useful when adopting PyClickHouseMigrator in a project that already has a ClickHouse schema.

### Stored rollback SQL

Rollback uses the `down` SQL stored in `db_migrations` when the migration was applied.

This keeps rollback behavior tied to the applied migration record instead of relying entirely on the current file content.

### Checksum validation based on parsed statements

Checksums are computed from parsed `up` and `down` statement blocks.

If an applied migration file changes or disappears, `migrator up` fails by default.

### Preflight validation

`up` and `rollback` validate statements with `EXPLAIN AST` by default.

Validation can be disabled with:

```sh
migrator up --no-validate
migrator rollback --no-validate
```

## Existing command surface

2.0 keeps the CLI small:

```text
migrator init
migrator new
migrator up
migrator rollback
migrator show
migrator baseline
migrator repair
migrator lock-info
migrator force-unlock
```

No config profiles, schema diff engine, JSON output mode, or DDL safety analyzer were added in 2.0. That is intentional.

## Breaking change from the old documented workflow

The old README described Python migration files with `up()` and `rollback()` functions returning SQL strings.

2.0 documentation and generated templates use SQL migration files instead.

If you have older local migrations in Python format, convert them manually to `.sql` files with `-- migrator:up`, `-- migrator:down`, and `-- @stmt` blocks.

Example conversion:

```python
# old style
def up() -> str:
    return """
    CREATE TABLE users (id UInt64) ENGINE = MergeTree ORDER BY id
    """

def rollback() -> str:
    return """
    DROP TABLE users
    """
```

```sql
-- new style
-- migrator:up
-- @stmt
CREATE TABLE users (id UInt64) ENGINE = MergeTree ORDER BY id

-- migrator:down
-- @stmt
DROP TABLE users
```

## Adoption checklist

```text
[ ] Ensure migration files use .sql extension.
[ ] Ensure each file has -- migrator:up and -- migrator:down.
[ ] Ensure SQL is inside -- @stmt blocks.
[ ] Run migrator up --dry-run in a test environment.
[ ] Run migrator show and check applied/pending output.
[ ] Confirm CI/CD uses migrator up as a single migration step.
[ ] For clusters, confirm user DDL includes ON CLUSTER where needed.
```

## Known limitations

The core limitations are unchanged:

- no DDL transactions;
- advisory lock is best-effort;
- target database must already exist;
- baseline does not verify existing schema;
- migration SQL is trusted input and executed as written.

See [Known limitations](known-limitations.md).
