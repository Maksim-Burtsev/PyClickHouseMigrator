import pytest
from clickhouse_driver import Client
from testcontainers.clickhouse import ClickHouseContainer

from clickhouse_migrator.migrator import Migrator

DB_URL = str


@pytest.fixture(scope="session")
def test_db() -> DB_URL:
    with ClickHouseContainer() as ch:
        db_url: str = ch.get_connection_url()
        yield db_url


@pytest.fixture(scope="function")
def migrator(test_db) -> Migrator:
    migrator = Migrator(test_db)
    yield migrator


@pytest.fixture(scope="session")
def ch_client(test_db) -> Client:
    client: Client = Client.from_url(test_db)
    yield client
