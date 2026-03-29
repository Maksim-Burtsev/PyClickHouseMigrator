from __future__ import annotations

import datetime as dt
import logging
import re
from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest
from clickhouse_driver import Client

from py_clickhouse_migrator.lock import LockError, LockInfo, LockTimeoutError, MigrationLock

DB = "test"


@pytest.fixture()
def lock(ch_client: Client) -> Generator[MigrationLock]:
    ml = MigrationLock(client=ch_client, db=DB, ttl=300)
    yield ml
    ch_client.execute(f"DROP TABLE IF EXISTS {DB}.{MigrationLock._LOCK_TABLE}")


@pytest.fixture()
def second_lock(ch_client: Client) -> MigrationLock:
    return MigrationLock(client=ch_client, db=DB, ttl=300)


def _insert_expired_lock(ch_client: Client) -> None:
    """Insert a lock row with expires_at in the past."""
    past = dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(seconds=60)
    ch_client.execute(
        f"INSERT INTO {DB}.{MigrationLock._LOCK_TABLE} (lock_id, locked_by, locked_at, expires_at, is_locked) VALUES",
        [["migration", "other_host:999", past, past, 1]],
    )


def test_acquire_release(lock: MigrationLock) -> None:
    assert not lock.is_locked()

    lock.acquire()
    assert lock.is_locked()

    lock.release()
    assert not lock.is_locked()


def test_double_acquire_fails(lock: MigrationLock, second_lock: MigrationLock) -> None:
    assert lock._locked_by != second_lock._locked_by

    lock.acquire()
    assert lock.is_locked()

    with pytest.raises(LockError):
        second_lock.acquire()

    lock.release()


def test_expired_lock_can_be_reacquired(lock: MigrationLock, ch_client: Client) -> None:
    _insert_expired_lock(ch_client)

    assert not lock.is_locked()

    lock.acquire()
    assert lock.is_locked()

    lock.release()


def test_force_release(lock: MigrationLock, second_lock: MigrationLock) -> None:
    lock.acquire()
    assert lock.is_locked()

    second_lock.release(force=True)
    assert not lock.is_locked()


def test_context_manager(lock: MigrationLock) -> None:
    assert not lock.is_locked()

    with lock:
        assert lock.is_locked()

    assert not lock.is_locked()


def test_context_manager_on_exception(ch_client: Client) -> None:
    lock = MigrationLock(client=ch_client, db=DB, ttl=300)

    with pytest.raises(ValueError, match="test error"):
        with lock:
            assert lock.is_locked()
            raise ValueError("test error")

    assert not lock.is_locked()
    ch_client.execute(f"DROP TABLE IF EXISTS {DB}.{MigrationLock._LOCK_TABLE}")


def test_retry_acquire(lock: MigrationLock, second_lock: MigrationLock) -> None:
    lock.acquire()

    def release_on_sleep(_delay: float) -> None:
        lock.release()

    with patch("py_clickhouse_migrator.lock.time.sleep", side_effect=release_on_sleep):
        second_lock.acquire(retry_count=1, retry_delay=1.0)

    assert second_lock.is_locked()
    second_lock.release()


def test_retry_acquire_timeout(lock: MigrationLock, second_lock: MigrationLock) -> None:
    lock.acquire()

    with patch("py_clickhouse_migrator.lock.time.sleep"):
        with pytest.raises(LockTimeoutError):
            second_lock.acquire(retry_count=2, retry_delay=1.0)

    lock.release()


def test_lock_info(lock: MigrationLock) -> None:
    assert lock.get_lock_info() is None

    lock.acquire()
    info = lock.get_lock_info()
    assert info is not None
    assert isinstance(info, LockInfo)
    assert info.locked_by == lock._locked_by
    assert re.fullmatch(r".+:\d+:[0-9a-f]{8}", info.locked_by)
    assert isinstance(info.locked_at, dt.datetime)
    assert isinstance(info.expires_at, dt.datetime)

    lock.release()


def test_invalid_db_name(ch_client: Client) -> None:
    with pytest.raises(ValueError, match="Invalid database name"):
        MigrationLock(client=ch_client, db="bad-name!")

    with pytest.raises(ValueError, match="Invalid database name"):
        MigrationLock(client=ch_client, db="123abc")

    with pytest.raises(ValueError, match="Invalid database name"):
        MigrationLock(client=ch_client, db="db; DROP TABLE x")


def test_is_locked(lock: MigrationLock) -> None:
    assert not lock.is_locked()

    lock.acquire()
    assert lock.is_locked()

    lock.release()
    assert not lock.is_locked()


def test_lock_cluster_param_stored() -> None:
    mock_client = MagicMock(spec=Client)
    ml = MigrationLock(client=mock_client, db=DB, cluster="my_cluster")
    assert ml._cluster == "my_cluster"


def test_lock_cluster_param_empty_by_default(ch_client: Client) -> None:
    ml = MigrationLock(client=ch_client, db=DB)
    assert ml._cluster == ""
    ch_client.execute(f"DROP TABLE IF EXISTS {DB}.{MigrationLock._LOCK_TABLE}")


def test_context_manager_release_failure(ch_client: Client, caplog: pytest.LogCaptureFixture) -> None:
    """If release() raises inside __exit__, the exception is logged, not propagated."""
    lock = MigrationLock(client=ch_client, db=DB, ttl=300)
    lock.acquire()

    with (
        patch.object(lock, "release", side_effect=RuntimeError("connection lost")),
        caplog.at_level(logging.ERROR, logger="py_clickhouse_migrator"),
    ):
        lock.__exit__(None, None, None)

    assert "Failed to release migration lock" in caplog.text
    # force cleanup — release was mocked so lock row still exists
    ch_client.execute(f"DROP TABLE IF EXISTS {DB}.{MigrationLock._LOCK_TABLE}")


def test_try_acquire_race_condition(lock: MigrationLock, ch_client: Client) -> None:
    """When another process grabs the lock between insert and verify, _try_acquire returns holder info."""
    # Simulate: after our insert, _get_active_lock returns someone else's lock
    other_info = LockInfo(locked_by="other:999", locked_at=dt.datetime.now(), expires_at=dt.datetime.now())
    with patch.object(lock, "_get_active_lock", return_value=other_info):
        result = lock._try_acquire()

    assert result is not None
    assert result.locked_by == "other:999"
    ch_client.execute(f"DROP TABLE IF EXISTS {DB}.{MigrationLock._LOCK_TABLE}")


def test_acquire_race_on_try_acquire(ch_client: Client) -> None:
    """When lock appears free but _try_acquire returns a holder, acquire should raise LockError."""
    lock = MigrationLock(client=ch_client, db=DB, ttl=300)
    other_info = LockInfo(locked_by="other:999", locked_at=dt.datetime.now(), expires_at=dt.datetime.now())

    call_count = 0

    def mock_get_active_lock() -> LockInfo | None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return None  # first check: lock appears free
        return other_info  # verify after insert: someone else holds it

    with (
        patch.object(lock, "_get_active_lock", side_effect=mock_get_active_lock),
        patch.object(lock, "_try_acquire", return_value=other_info),
        pytest.raises(LockError),
    ):
        lock.acquire()

    ch_client.execute(f"DROP TABLE IF EXISTS {DB}.{MigrationLock._LOCK_TABLE}")


def test_acquire_release_no_cluster(lock: MigrationLock) -> None:
    assert lock._cluster == ""
    assert not lock.is_locked()

    lock.acquire()
    assert lock.is_locked()

    lock.release()
    assert not lock.is_locked()


def test_release_skips_when_lock_held_by_other(
    lock: MigrationLock, second_lock: MigrationLock, caplog: pytest.LogCaptureFixture
) -> None:
    """release() should not unlock when another process holds the lock."""
    lock.acquire()
    assert lock.is_locked()

    info = lock.get_lock_info()
    assert info is not None
    assert info.locked_by == lock._locked_by
    assert info.locked_by != second_lock._locked_by

    with caplog.at_level(logging.WARNING, logger="py_clickhouse_migrator"):
        second_lock.release()
    assert "Lock is held by another worker" in caplog.text
    assert lock.is_locked()

    lock.release()


def test_release_skips_when_no_active_lock(lock: MigrationLock, caplog: pytest.LogCaptureFixture) -> None:
    """release() without prior acquire should be a no-op."""
    assert not lock.is_locked()
    with caplog.at_level(logging.DEBUG, logger="py_clickhouse_migrator"):
        lock.release()
    assert "No active lock to release" in caplog.text
    assert not lock.is_locked()
    assert lock.get_lock_info() is None
