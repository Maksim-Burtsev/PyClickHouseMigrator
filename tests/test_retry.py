from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from py_clickhouse_migrator.migrator import ClickHouseServerIsNotHealthyError, Migrator

FAKE_URL = "clickhouse://default@localhost:9000/test"


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
    with (
        patch("py_clickhouse_migrator.migrator.Client.from_url", return_value=MagicMock()),
        patch.object(Migrator, "check_migrations_table"),
        patch.object(Migrator, "health_check", side_effect=ClickHouseServerIsNotHealthyError("fail")),
    ):
        with pytest.raises(ClickHouseServerIsNotHealthyError):
            Migrator(database_url=FAKE_URL, connect_retries=0)


def test_health_check_retries_success_on_third_attempt() -> None:
    migrator = _make_migrator(connect_retries=3, connect_retries_interval=0)
    migrator.ch_client.execute.reset_mock()
    migrator.ch_client.execute.side_effect = [ConnectionError("fail"), ConnectionError("fail"), [[1]]]

    with patch("py_clickhouse_migrator.migrator.time.sleep"):
        migrator.health_check()

    assert migrator.ch_client.execute.call_count == 3


def test_health_check_retries_exhausted() -> None:
    migrator = _make_migrator(connect_retries=2, connect_retries_interval=0)
    migrator.ch_client.execute.reset_mock()
    migrator.ch_client.execute.side_effect = ConnectionError("fail")

    with (
        patch("py_clickhouse_migrator.migrator.time.sleep"),
        pytest.raises(ClickHouseServerIsNotHealthyError),
    ):
        migrator.health_check()

    assert migrator.ch_client.execute.call_count == 3


def test_health_check_retries_logs_warning(caplog: pytest.LogCaptureFixture) -> None:
    migrator = _make_migrator(connect_retries=2, connect_retries_interval=0)
    migrator.ch_client.execute.side_effect = ConnectionError("fail")

    with (
        patch("py_clickhouse_migrator.migrator.time.sleep"),
        caplog.at_level(logging.WARNING, logger="py_clickhouse_migrator"),
        pytest.raises(ClickHouseServerIsNotHealthyError),
    ):
        migrator.health_check()

    warning_messages = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warning_messages) == 2


def test_health_check_retries_sleep_called() -> None:
    migrator = _make_migrator(connect_retries=2, connect_retries_interval=5)
    migrator.ch_client.execute.side_effect = ConnectionError("fail")

    with (
        patch("py_clickhouse_migrator.migrator.time.sleep") as mock_sleep,
        pytest.raises(ClickHouseServerIsNotHealthyError),
    ):
        migrator.health_check()

    assert mock_sleep.call_count == 2
    mock_sleep.assert_called_with(5)
