"""ClickHouse helpers shared by the migrator and the migration lock."""

import logging
import re
import time
from collections.abc import Mapping
from types import MappingProxyType
from typing import Final

from clickhouse_driver import Client
from clickhouse_driver.errors import ServerException

from py_clickhouse_migrator.errors import ClickHouseServerIsNotHealthyError, DatabaseNotFoundError

SQL = str
ClickHouseSettings = dict[str, str | int]

logger = logging.getLogger("py_clickhouse_migrator")

_UNKNOWN_DATABASE_CODE: Final = 81
_SQL_IDENTIFIER: Final = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*\Z")
_CLUSTER_SETTINGS: Final[Mapping[str, str | int]] = MappingProxyType({
    "insert_quorum": "auto",
    "select_sequential_consistency": 1,
})


def is_sql_identifier(name: str) -> bool:
    """Tell whether ``name`` is safe to interpolate into SQL as a database or cluster name."""
    return _SQL_IDENTIFIER.match(name) is not None


def cluster_settings(cluster: str) -> ClickHouseSettings:
    """Return query settings for service tables: quorum reads and writes in cluster mode, none otherwise."""
    return dict(_CLUSTER_SETTINGS) if cluster else {}


def on_cluster_clause(cluster: str) -> str:
    """Return the ``ON CLUSTER`` clause for service-table DDL, or an empty string outside cluster mode."""
    return f"ON CLUSTER {cluster}" if cluster else ""


def wait_until_healthy(client: Client, *, database: str, retries: int, interval: int) -> None:
    """Run ``SELECT 1`` until it succeeds, retrying ``retries`` times with ``interval`` seconds between tries.

    Raises:
        DatabaseNotFoundError: the server answered that ``database`` does not exist; this is not retried.
        ClickHouseServerIsNotHealthyError: every attempt failed.

    """
    for attempt in range(retries + 1):
        try:
            client.execute("SELECT 1")
        except Exception as exc:
            _raise_unless_retryable(exc, database=database, is_last_attempt=attempt == retries)
            logger.warning("Connection attempt %d/%d failed, retrying in %ds", attempt + 1, retries, interval)
            time.sleep(interval)
        else:
            return


def _raise_unless_retryable(exc: Exception, *, database: str, is_last_attempt: bool) -> None:
    if isinstance(exc, ServerException) and exc.code == _UNKNOWN_DATABASE_CODE:
        raise DatabaseNotFoundError(
            f"Database '{database}' does not exist.\n"
            f"Create it manually before running migrations:\n"
            f"  CREATE DATABASE {database}",
        ) from exc
    if is_last_attempt:
        raise ClickHouseServerIsNotHealthyError(f"ClickHouse server is not healthy: {exc}.") from exc
