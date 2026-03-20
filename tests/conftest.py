from __future__ import annotations

import os
import shutil
from collections.abc import Generator

import pytest
from clickhouse_driver import Client

from py_clickhouse_migrator.migrator import Migrator, create_migrations_dir

from tests.helpers import create_test_migration, table_exists

DB_URL = "clickhouse://default@localhost:19000/test"


@pytest.fixture(scope="session")
def test_db() -> str:
    return DB_URL


@pytest.fixture(scope="function")
def migrator(test_db: str) -> Generator[Migrator]:
    migrator = Migrator(test_db)
    yield migrator


@pytest.fixture(scope="session")
def ch_client(test_db: str) -> Generator[Client]:
    client: Client = Client.from_url(test_db)
    yield client


@pytest.fixture(autouse=True)
def clean_db_dir() -> Generator[None]:
    yield
    if os.path.exists("./db"):
        shutil.rmtree("./db")


@pytest.fixture(scope="function")
def migrator_init(migrator: Migrator, ch_client: Client) -> Generator[None]:
    create_migrations_dir()
    assert table_exists(ch_client, "db_migrations")

    yield

    ch_client.execute("DROP TABLE IF EXISTS db_migrations")


@pytest.fixture(scope="function")
def test_table_from_migration(migrator: Migrator, migrator_init: None, ch_client: Client) -> Generator[str]:
    filename: str = create_test_migration(
        name="test_1",
        up="CREATE TABLE IF NOT EXISTS test_table (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table",
    )
    migrator.up()

    yield filename

    ch_client.execute("DROP TABLE IF EXISTS test_table")


@pytest.fixture(scope="function")
def test_tables_from_migration(migrator: Migrator, migrator_init: None, ch_client: Client) -> Generator[list[str]]:
    filename_1: str = create_test_migration(
        name="test_1",
        up="CREATE TABLE IF NOT EXISTS test_table_1 (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_1",
    )
    filename_2: str = create_test_migration(
        name="test_2",
        up="CREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_2",
    )
    filename_3: str = create_test_migration(
        name="test_3",
        up="CREATE TABLE IF NOT EXISTS test_table_3 (id String) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_3",
    )
    migrator.up()

    yield [filename_1, filename_2, filename_3]

    ch_client.execute("DROP TABLE IF EXISTS test_table_1")
    ch_client.execute("DROP TABLE IF EXISTS test_table_2")
    ch_client.execute("DROP TABLE IF EXISTS test_table_3")
