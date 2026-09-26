from __future__ import annotations

import logging
from unittest.mock import MagicMock, call, patch

import pytest

from py_clickhouse_migrator.errors import ClickHouseServerIsNotHealthyError
from py_clickhouse_migrator.migrator import Migrator

FAKE_URL = "clickhouse://default@localhost:9000/test"
SLEEP_TARGET = "py_clickhouse_migrator.clickhouse.time.sleep"
RETRY_INTERVAL_SECONDS = 5


def _make_migrator(connect_retries: int = 0, connect_retries_interval: int = 1) -> Migrator:
    with (
        patch("py_clickhouse_migrator.migrator.Client.from_url", return_value=MagicMock()),
        patch.object(Migrator, "check_migrations_table"),
    ):
        return Migrator(
            database_url=FAKE_URL,
            connect_retries=connect_retries,
            connect_retries_interval=connect_retries_interval,
        )


def test_health_check_no_retries_default() -> None:
    """A failed health check propagates out of the Migrator constructor when retries are off."""
    with (
        patch("py_clickhouse_migrator.migrator.Client.from_url", return_value=MagicMock()),
        patch.object(Migrator, "check_migrations_table"),
        patch.object(Migrator, "health_check", side_effect=ClickHouseServerIsNotHealthyError("fail")),
        pytest.raises(ClickHouseServerIsNotHealthyError),
    ):
        Migrator(database_url=FAKE_URL, connect_retries=0)


def test_health_check_succeeds_on_third_attempt() -> None:
    """Two failed attempts are retried, and the third successful one ends the health check."""
    migrator = _make_migrator(connect_retries=3, connect_retries_interval=0)
    migrator.ch_client.execute.reset_mock()
    connection_error = ConnectionError("fail")
    select_one_rows = [[1]]
    migrator.ch_client.execute.side_effect = [connection_error, connection_error, select_one_rows]

    with patch(SLEEP_TARGET):
        migrator.health_check()

    assert migrator.ch_client.execute.call_count == 3


def test_health_check_retries_exhausted() -> None:
    """After the first attempt and every retry fail, the health check reports the server unhealthy."""
    migrator = _make_migrator(connect_retries=2, connect_retries_interval=0)
    migrator.ch_client.execute.reset_mock()
    migrator.ch_client.execute.side_effect = ConnectionError("fail")

    with (
        patch(SLEEP_TARGET),
        pytest.raises(ClickHouseServerIsNotHealthyError),
    ):
        migrator.health_check()

    assert migrator.ch_client.execute.call_count == 3


def test_health_check_retries_logs_warning(caplog: pytest.LogCaptureFixture) -> None:
    """Each failed attempt that is followed by a retry logs one warning."""
    migrator = _make_migrator(connect_retries=2, connect_retries_interval=0)
    migrator.ch_client.execute.side_effect = ConnectionError("fail")

    with (
        patch(SLEEP_TARGET),
        caplog.at_level(logging.WARNING, logger="py_clickhouse_migrator"),
        pytest.raises(ClickHouseServerIsNotHealthyError),
    ):
        migrator.health_check()

    warning_records = [record for record in caplog.records if record.levelno == logging.WARNING]
    assert len(warning_records) == 2


def test_health_check_retries_sleep_called() -> None:
    """Every retry waits the configured interval first."""
    migrator = _make_migrator(connect_retries=2, connect_retries_interval=RETRY_INTERVAL_SECONDS)
    migrator.ch_client.execute.side_effect = ConnectionError("fail")
    sleep = MagicMock()

    with (
        patch(SLEEP_TARGET, sleep),
        pytest.raises(ClickHouseServerIsNotHealthyError),
    ):
        migrator.health_check()

    assert sleep.call_args_list == [call(RETRY_INTERVAL_SECONDS), call(RETRY_INTERVAL_SECONDS)]
