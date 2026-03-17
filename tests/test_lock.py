from __future__ import annotations

import datetime as dt
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

    second_lock.force_release()
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


def test_acquire_release_no_cluster(lock: MigrationLock) -> None:
    assert lock._cluster == ""
    assert not lock.is_locked()

    lock.acquire()
    assert lock.is_locked()

    lock.release()
    assert not lock.is_locked()
