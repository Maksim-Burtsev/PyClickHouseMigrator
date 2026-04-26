# Migration format

PyClickHouseMigrator 2.0 uses SQL migration files.

A migration file is a `.sql` file stored in the migrations directory. The default directory is:

```text
./db/migrations
```

Files are executed in lexicographic filename order. The `migrator new` command creates timestamped names so natural sorting matches creation order:

```text
20260421140000_create_users_table.sql
20260421143000_add_events_table.sql
20260421150000_add_status_column.sql
```

## Required sections

Every migration must contain exactly one `up` section and exactly one `down` section:

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

The `up` section is applied by:

```sh
migrator up
```

The `down` section is applied by:

```sh
migrator rollback
```

## Statement blocks

SQL statements must be placed inside `-- @stmt` blocks.

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

-- @stmt
ALTER TABLE users ADD COLUMN IF NOT EXISTS email Nullable(String)

-- migrator:down
-- @stmt
ALTER TABLE users DROP COLUMN IF EXISTS email

-- @stmt
DROP TABLE IF EXISTS users
```

Each `-- @stmt` block is sent to ClickHouse as one query.

The migrator does **not** split SQL by semicolons. A semicolon inside a string literal or comment is not treated as a separator by the migrator.

## Why explicit blocks?

ClickHouse DDL is usually written manually. The safest migration format is one that does not guess where statements begin and end.

Naive semicolon splitting is fragile:

```sql
INSERT INTO messages VALUES ('hello; world')
```

In that example, the semicolon is part of the string value, not a statement separator.

PyClickHouseMigrator avoids that class of parsing bugs by requiring explicit `-- @stmt` blocks.

## Parser rules

A valid migration file must satisfy these rules:

- exactly one `-- migrator:up` marker;
- exactly one `-- migrator:down` marker;
- `-- migrator:up` must come before `-- migrator:down`;
- non-empty SQL content must be inside `-- @stmt` blocks;
- the `up` section must contain at least one non-empty statement block;
- the `down` section may be empty;
- empty statement blocks are ignored;
- only `.sql` files are discovered.

Invalid:

```sql
-- migrator:up
CREATE TABLE users (id UInt64) ENGINE = MergeTree ORDER BY id

-- migrator:down
-- @stmt
DROP TABLE users
```

The `CREATE TABLE` is outside a `-- @stmt` block.

Valid:

```sql
-- migrator:up
-- @stmt
CREATE TABLE users (id UInt64) ENGINE = MergeTree ORDER BY id

-- migrator:down
-- @stmt
DROP TABLE users
```

## Empty rollback

The `down` section may be empty when rollback is not meaningful or intentionally unsupported.

```sql
-- migrator:up
-- @stmt
CREATE TABLE IF NOT EXISTS audit_log
(
    id UInt64,
    message String
)
ENGINE = MergeTree
ORDER BY id

-- migrator:down
```

If this migration is selected by `migrator rollback`, no rollback SQL is executed for it, and the migration row is removed from `db_migrations`.

Use empty rollback sections deliberately. In production migrations, a reversible `down` section is usually easier to reason about.

## Idempotent SQL

ClickHouse DDL is not transactional. If a migration has several statement blocks and a later block fails, earlier blocks may already be applied.

Prefer idempotent DDL where possible:

```sql
CREATE TABLE IF NOT EXISTS users (...)
ALTER TABLE users ADD COLUMN IF NOT EXISTS email Nullable(String)
DROP TABLE IF EXISTS old_users
```

This makes recovery easier after partial failures.

## Preflight validation

By default, `up` and `rollback` validate each statement with `EXPLAIN AST` before execution.

Validation is best-effort. It is useful for catching many syntax errors before state is written, but it is not a guarantee that the statement will execute successfully and it does not judge whether a DDL operation is safe for production.

If needed, you can disable this preflight step explicitly:

```sh
migrator up --no-validate
migrator rollback --no-validate
```

## Checksum behavior

The checksum stored in `db_migrations` is computed from parsed `up` and `down` statement blocks.

This means the checksum reflects the SQL blocks the migrator understands and executes, not arbitrary raw file bytes.

If an applied migration file changes, `migrator up` fails by default:

```sh
migrator show
migrator repair
```

Use `repair` only after intentionally changing already-applied migration files.
