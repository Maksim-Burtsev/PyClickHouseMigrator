from collections.abc import Generator

import pytest
from clickhouse_driver import Client

from py_clickhouse_migrator.migrator import Migrator

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
