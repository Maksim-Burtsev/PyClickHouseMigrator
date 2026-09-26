"""Fixtures for tests that run against the ClickHouse from ``docker-compose.test.yml``."""

import shutil
from collections.abc import Iterator
from pathlib import Path

import pytest
from clickhouse_driver import Client

from py_clickhouse_migrator.migrator import Migrator, create_migrations_dir
from tests.migration_files import create_test_migration
from tests.queries import drop_tables, table_exists

DB_URL = "clickhouse://default@localhost:19000/test"
MIGRATIONS_ROOT = Path("db")


@pytest.fixture(scope="session")
def test_db() -> str:
    """URL of the test database."""
    return DB_URL


@pytest.fixture
def migrator(test_db: str) -> Migrator:
    """Migrator for the test database; creating it also creates ``db_migrations``."""
    return Migrator(test_db)


@pytest.fixture(scope="session")
def ch_client(test_db: str) -> Client:
    """Plain ClickHouse client for checking what the migrator did."""
    return Client.from_url(test_db)


@pytest.fixture(autouse=True)
def clean_db_dir() -> Iterator[None]:
    """Remove the ``./db`` directory that tests create for migration files."""
    yield
    if MIGRATIONS_ROOT.exists():
        shutil.rmtree(MIGRATIONS_ROOT)


@pytest.fixture
def migrator_init(migrator: Migrator, ch_client: Client) -> Iterator[None]:
    """Create the default migrations directory, and drop ``db_migrations`` after the test."""
    create_migrations_dir()
    assert table_exists(ch_client, "db_migrations")

    yield

    drop_tables(ch_client, "db_migrations")


@pytest.fixture
def test_table_from_migration(migrator: Migrator, migrator_init: None, ch_client: Client) -> Iterator[str]:
    """Apply one migration that creates ``test_table``; yields its file name."""
    filename = create_test_migration(
        name="test_1",
        up="CREATE TABLE IF NOT EXISTS test_table (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table",
    )
    migrator.up()

    yield filename

    drop_tables(ch_client, "test_table")


@pytest.fixture
def test_tables_from_migration(migrator: Migrator, migrator_init: None, ch_client: Client) -> Iterator[list[str]]:
    """Apply three migrations that create ``test_table_1`` to ``test_table_3``; yields their file names."""
    filenames = [
        create_test_migration(
            name="test_1",
            up="CREATE TABLE IF NOT EXISTS test_table_1 (id Integer) Engine=MergeTree() ORDER BY id;",
            rollback="DROP TABLE IF EXISTS test_table_1",
        ),
        create_test_migration(
            name="test_2",
            up="CREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;",
            rollback="DROP TABLE IF EXISTS test_table_2",
        ),
        create_test_migration(
            name="test_3",
            up="CREATE TABLE IF NOT EXISTS test_table_3 (id String) Engine=MergeTree() ORDER BY id;",
            rollback="DROP TABLE IF EXISTS test_table_3",
        ),
    ]
    migrator.up()

    yield filenames

    drop_tables(ch_client, "test_table_1", "test_table_2", "test_table_3")
