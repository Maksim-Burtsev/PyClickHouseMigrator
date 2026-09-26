"""Queries that tests use to inspect and clean up the ClickHouse test database."""

from clickhouse_driver import Client


def table_exists(ch_client: Client, table_name: str) -> bool:
    """Tell whether ``table_name`` exists in the current database."""
    rows = ch_client.execute(
        "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = %(name)s",
        {"name": table_name},
    )
    return bool(rows[0][0] > 0)


def get_engine(ch_client: Client, table_name: str) -> str:
    """Return the engine of ``table_name``, or an empty string if the table does not exist."""
    rows = ch_client.execute(
        "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = %(name)s",
        {"name": table_name},
    )
    return str(rows[0][0]) if rows else ""


def count_migration_rows(ch_client: Client, name: str | None = None) -> int:
    """Count rows in ``db_migrations``; only the rows of migration ``name`` when it is given."""
    if name is None:
        rows = ch_client.execute("SELECT count() FROM db_migrations")
    else:
        rows = ch_client.execute("SELECT count() FROM db_migrations WHERE name = %(name)s", {"name": name})
    return int(rows[0][0])


def drop_tables(ch_client: Client, *tables: str) -> None:
    """Drop tables created by a test, ignoring the ones that do not exist."""
    for table in tables:
        ch_client.execute(f"DROP TABLE IF EXISTS {table}")
