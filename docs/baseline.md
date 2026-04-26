# Baseline existing databases

`baseline` is for adopting an existing ClickHouse database.

Use it when the database already contains schema objects and you have `.sql` migration files that represent that existing schema. Baseline marks those files as already applied so future `migrator up` runs only execute new migrations.

## Typical workflow

1. Write or collect `.sql` migration files that represent the existing schema.
2. Point the migrator at that directory with `--path` or `CLICKHOUSE_MIGRATE_DIR`, or put the files in the default `./db/migrations` directory.
3. Run `migrator baseline` once.
4. Add new migration files normally.
5. Run `migrator up` for future changes.

Example:

```sh
export CLICKHOUSE_MIGRATE_URL=clickhouse://default@localhost:9000/analytics

migrator init
migrator new initial_schema
# edit the generated SQL file so it describes the existing schema
migrator baseline
```

After that, create a new migration:

```sh
migrator new add_events_status
migrator up
```

Only the new pending migration is executed.

## What baseline does

`migrator baseline`:

- creates the `db_migrations` service table if it does not exist;
- requires `db_migrations` to have no rows;
- marks all current `.sql` files in the migrations directory as already applied (`baseline` rows);
- preserves migration ordering by filename;
- does not execute SQL;
- does not validate SQL;
- does not inspect the current ClickHouse schema.

Example output:

```text
Baselined 3 migration(s).
  [B] 20260421100000_initial_schema.sql
  [B] 20260421103000_initial_events.sql
  [B] 20260421110000_initial_views.sql
```

## What baseline does not do

Baseline is not a schema diff or schema verification command.

It does not check that:

- tables in your `.sql` files exist in ClickHouse;
- table engines match;
- columns match;
- views match;
- cluster objects exist on every node.

It only records migration filenames as already applied.

## Rollback behavior

Baseline rows are not selected by `migrator rollback`.

This prevents accidental rollback of historical schema that the migrator did not create.

Example:

```text
Applied:
  [X] 20260421120000_add_status_column.sql (HEAD)
  [X] 20260421100000_initial_schema.sql (baseline)

Pending: none
```

Running:

```sh
migrator rollback
```

rolls back `20260421120000_add_status_column.sql`, not the baseline row.

## Checksum behavior

Baseline rows are excluded from checksum validation.

Why: baseline files are treated as historical reference points, not executed migrations. They are useful for ordering and future migration flow, but they do not represent SQL that PyClickHouseMigrator applied.

## Safety notes

Baseline creates the `db_migrations` table if needed. If the table already contains rows, the command fails.

This is intentional. Baseline should be a one-time adoption step, not a way to mix arbitrary historical rows into an existing migration history.

Recommended pattern:

```sh
migrator show
migrator baseline
migrator show
```

Use `show` to confirm that the baseline rows are visible and that future migrations are pending as expected.
