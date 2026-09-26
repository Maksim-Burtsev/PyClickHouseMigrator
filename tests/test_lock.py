from __future__ import annotations

import datetime as dt
import logging
import re
from collections.abc import Iterator
from typing import Final
from unittest.mock import MagicMock, patch

import pytest
from clickhouse_driver import Client

from py_clickhouse_migrator.lock import LOCK_TABLE, LockError, LockInfo, LockTimeoutError, MigrationLock
from tests.queries import drop_tables, get_engine

DB: Final = "test"
LOGGER_NAME: Final = "py_clickhouse_migrator"
LOCK_SLEEP: Final = "py_clickhouse_migrator.lock.time.sleep"
LOCK_TTL_SECONDS: Final = 300
LOCK_TTL: Final = dt.timedelta(seconds=LOCK_TTL_SECONDS)
OTHER_WORKER: Final = "other_host:999"
EXPIRED_FOR: Final = dt.timedelta(seconds=60)
OWNER_ID_PATTERN: Final = re.compile(r".+:\d+:[0-9a-f]{8}")
UNLOCKED_LOCKED_UNLOCKED: Final = (False, True, False)


class _MigrationFailedError(Exception):
    """Stands in for a migration that fails while the lock is held."""


@pytest.fixture
def lock_table(ch_client: Client) -> Iterator[None]:
    """Drop the lock table after the test so lock rows never leak into the next one."""
    yield
    drop_tables(ch_client, LOCK_TABLE)


@pytest.fixture
def lock(ch_client: Client, lock_table: None) -> MigrationLock:
    """A worker's lock on the test database; creating it creates the lock table."""
    return MigrationLock(client=ch_client, db=DB, ttl=LOCK_TTL_SECONDS)


@pytest.fixture
def second_lock(ch_client: Client, lock_table: None) -> MigrationLock:
    """Another worker's lock on the same table, to play the competing process."""
    return MigrationLock(client=ch_client, db=DB, ttl=LOCK_TTL_SECONDS)


def _insert_expired_lock(ch_client: Client) -> None:
    """Insert a lock row with expires_at in the past."""
    expired_at = dt.datetime.now(dt.UTC) - EXPIRED_FOR
    ch_client.execute(
        f"INSERT INTO {DB}.{LOCK_TABLE} (lock_id, locked_by, locked_at, expires_at, is_locked) VALUES",
        [["migration", OTHER_WORKER, expired_at, expired_at, 1]],
    )


def _other_worker_lock() -> LockInfo:
    locked_at = dt.datetime.now(dt.UTC)
    return LockInfo(locked_by=OTHER_WORKER, locked_at=locked_at, expires_at=locked_at + LOCK_TTL)


def _owner_id(lock: MigrationLock) -> str:
    with lock:
        holder = lock.get_lock_info()
    assert holder is not None
    return holder.locked_by


def _observe_acquire_release(lock: MigrationLock, observer: MigrationLock) -> tuple[bool, bool, bool]:
    before = observer.is_locked()
    lock.acquire()
    held = observer.is_locked()
    lock.release()
    return before, held, observer.is_locked()


def _fail_while_holding(lock: MigrationLock) -> None:
    with lock:
        assert lock.is_locked()
        raise _MigrationFailedError


def _reported_holder(error: LockError) -> LockInfo:
    return LockInfo(locked_by=error.locked_by, locked_at=error.locked_at, expires_at=error.expires_at)


def _lock_table_ddl(client: MagicMock) -> str:
    client.execute.assert_called_once()
    return str(client.execute.call_args.args[0])


def test_acquire_release(lock: MigrationLock) -> None:
    """Acquire takes a free lock and release frees it again, so the next run can proceed."""
    assert _observe_acquire_release(lock, observer=lock) == UNLOCKED_LOCKED_UNLOCKED


def test_double_acquire_fails(lock: MigrationLock, second_lock: MigrationLock) -> None:
    """Each worker has its own owner id, and a second worker cannot take a lock the first one holds."""
    first_owner = _owner_id(lock)
    assert first_owner != _owner_id(second_lock)

    lock.acquire()
    with pytest.raises(LockError) as lock_error:
        second_lock.acquire()

    assert lock_error.value.locked_by == first_owner
    lock.release()


def test_expired_lock_can_be_reacquired(lock: MigrationLock, ch_client: Client) -> None:
    """A lock row past its expiry does not block anyone, so a crashed worker cannot hold the lock forever."""
    _insert_expired_lock(ch_client)

    assert not lock.is_locked()

    lock.acquire()
    assert lock.is_locked()

    lock.release()


def test_force_release(lock: MigrationLock, second_lock: MigrationLock) -> None:
    """Force release frees a lock held by another worker, which is what ``force-unlock`` relies on."""
    lock.acquire()
    assert lock.is_locked()

    second_lock.release(force=True)
    assert not lock.is_locked()


def test_context_manager(lock: MigrationLock) -> None:
    """The ``with`` block holds the lock for its body and frees it on exit."""
    assert not lock.is_locked()

    with lock:
        assert lock.is_locked()

    assert not lock.is_locked()


def test_context_manager_on_exception(lock: MigrationLock) -> None:
    """A failing migration still frees the lock, and its error reaches the caller unchanged."""
    with pytest.raises(_MigrationFailedError):
        _fail_while_holding(lock)

    assert not lock.is_locked()


@pytest.mark.parametrize("retry_count", [1, 2])
def test_retry_acquire(lock: MigrationLock, second_lock: MigrationLock, retry_count: int) -> None:
    """A waiting worker takes the lock on the first retry once the holder releases it, spare retries or not."""
    lock.acquire()

    with patch(LOCK_SLEEP, side_effect=lambda _delay: lock.release()):
        second_lock.acquire(retry_count=retry_count, retry_delay=1.0)

    assert second_lock.is_locked()
    second_lock.release()


def test_retry_acquire_timeout(lock: MigrationLock, second_lock: MigrationLock) -> None:
    """A worker gives up with LockTimeoutError when the lock stays held through every retry."""
    lock.acquire()

    with patch(LOCK_SLEEP), pytest.raises(LockTimeoutError):
        second_lock.acquire(retry_count=2, retry_delay=1.0)

    lock.release()


def test_lock_info(lock: MigrationLock) -> None:
    """Lock info names the holding worker as host:pid:token and is the id its own release recognizes."""
    assert lock.get_lock_info() is None

    lock.acquire()
    holder = lock.get_lock_info()
    assert isinstance(holder, LockInfo)
    assert OWNER_ID_PATTERN.fullmatch(holder.locked_by)

    lock.release()
    assert lock.get_lock_info() is None


def test_lock_info_times(lock: MigrationLock) -> None:
    """Lock info reports when the lock was taken and a later expiry, so operators can tell a stale lock."""
    lock.acquire()
    holder = lock.get_lock_info()
    lock.release()

    assert holder is not None
    assert isinstance(holder.locked_at, dt.datetime)
    assert isinstance(holder.expires_at, dt.datetime)
    assert holder.expires_at > holder.locked_at


@pytest.mark.parametrize("db", ["bad-name!", "123abc", "db; DROP TABLE x"])
def test_invalid_db_name(db: str) -> None:
    """The database name is interpolated into SQL, so a non-identifier is rejected before any query runs."""
    client = MagicMock(spec=Client)

    with pytest.raises(ValueError, match="Invalid database name"):
        MigrationLock(client=client, db=db)

    client.execute.assert_not_called()


@pytest.mark.parametrize("cluster", ["bad-name!", "123abc", "cluster; DROP TABLE x"])
def test_invalid_cluster_name(cluster: str) -> None:
    """The cluster name is interpolated into SQL, so a non-identifier is rejected before any query runs."""
    client = MagicMock(spec=Client)

    with pytest.raises(ValueError, match="Invalid cluster name"):
        MigrationLock(client=client, db=DB, cluster=cluster)

    client.execute.assert_not_called()


def test_is_locked(lock: MigrationLock, second_lock: MigrationLock) -> None:
    """is_locked reports a lock held by any worker, not only by the one asking."""
    assert _observe_acquire_release(lock, observer=second_lock) == UNLOCKED_LOCKED_UNLOCKED


def test_lock_cluster_param_stored() -> None:
    """With a cluster, the lock table is created ON CLUSTER with a Replicated engine so all nodes share one lock."""
    client = MagicMock(spec=Client)

    MigrationLock(client=client, db=DB, cluster="my_cluster")

    ddl = _lock_table_ddl(client)
    assert "ON CLUSTER my_cluster" in ddl
    assert "ENGINE = ReplicatedReplacingMergeTree(" in ddl


def test_lock_cluster_param_empty_by_default() -> None:
    """Without a cluster, the lock table is a local table, so a single server needs no cluster config."""
    client = MagicMock(spec=Client)

    MigrationLock(client=client, db=DB)

    ddl = _lock_table_ddl(client)
    assert "ON CLUSTER" not in ddl
    assert "ENGINE = ReplacingMergeTree(locked_at)" in ddl


def test_context_manager_release_failure(lock: MigrationLock, caplog: pytest.LogCaptureFixture) -> None:
    """A release that fails on exit is logged instead of raised, so it never hides the migration's own outcome."""
    with (
        patch.object(lock, "release", side_effect=RuntimeError("connection lost")),
        caplog.at_level(logging.ERROR, logger=LOGGER_NAME),
        lock,
    ):
        assert lock.is_locked()

    assert "Failed to release migration lock" in caplog.text


def test_try_acquire_race_condition(lock: MigrationLock) -> None:
    """If another worker wins between our insert and the read-back, acquire reports that worker's lock."""
    race_winner = _other_worker_lock()

    with (
        patch.object(lock, "_get_active_lock", side_effect=[None, race_winner]),
        pytest.raises(LockError) as lock_error,
    ):
        lock.acquire()

    assert _reported_holder(lock_error.value) == race_winner


def test_acquire_race_on_try_acquire() -> None:
    """A lock that looked free is not taken when the read-back after the insert shows another worker's row."""
    race_winner = _other_worker_lock()
    client = MagicMock(spec=Client)
    client.execute.side_effect = [
        [],
        [],
        [],
        [(race_winner.locked_by, race_winner.locked_at, race_winner.expires_at)],
    ]
    lock = MigrationLock(client=client, db=DB)

    with pytest.raises(LockError, match=OTHER_WORKER):
        lock.acquire()


def test_acquire_release_no_cluster(lock: MigrationLock, ch_client: Client) -> None:
    """Without a cluster the lock lives in a local ReplacingMergeTree table and still locks and unlocks."""
    assert get_engine(ch_client, LOCK_TABLE) == "ReplacingMergeTree"
    assert _observe_acquire_release(lock, observer=lock) == UNLOCKED_LOCKED_UNLOCKED


def test_release_skips_when_lock_held_by_other(
    lock: MigrationLock,
    second_lock: MigrationLock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A worker never releases a lock another worker holds, or two migrations could run at the same time."""
    lock.acquire()
    holder = lock.get_lock_info()
    assert holder is not None

    with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
        second_lock.release()

    assert f"Lock is held by another worker {holder.locked_by}" in caplog.text
    assert lock.get_lock_info() == holder

    lock.release()
    assert lock.get_lock_info() is None


def test_release_skips_when_no_active_lock(lock: MigrationLock, caplog: pytest.LogCaptureFixture) -> None:
    """Releasing when nobody holds the lock is a logged no-op, so a cleanup path can always call it."""
    assert not lock.is_locked()

    with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
        lock.release()

    assert "No active lock to release" in caplog.text
    assert not lock.is_locked()
    assert lock.get_lock_info() is None
