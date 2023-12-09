import pytest
from testcontainers.clickhouse import ClickHouseContainer

from py_clickhouse_migrate.migrator import Migrator

DB_URL = str


@pytest.fixture(scope="session")
def test_db() -> DB_URL:
    with ClickHouseContainer() as ch:
        db_url: str = ch.get_connection_url()

        yield db_url


@pytest.fixture(scope="session")
def migrator(test_db) -> Migrator:
    migrator = Migrator(test_db)
    yield migrator
