"""Advisory lock that keeps concurrent runs from applying migrations at the same time.

The lock is a row in the ``_migrations_lock`` ReplacingMergeTree table. Acquiring inserts a row and
then reads the table back to confirm ownership; the row expires after a TTL, so a crashed worker
cannot hold the lock forever.
"""

import datetime as dt
import logging
import os
import socket
import time
from dataclasses import dataclass
from types import TracebackType
from typing import Final, Self
from uuid import uuid4

from clickhouse_driver import Client

from py_clickhouse_migrator.clickhouse import ClickHouseSettings, cluster_settings, is_sql_identifier, on_cluster_clause

logger = logging.getLogger("py_clickhouse_migrator")

LOCK_TABLE: Final = "_migrations_lock"
DEFAULT_LOCK_TTL: Final = 300

_LOCK_ID: Final = "migration"
_WORKER_TOKEN_LENGTH: Final = 8
_DT_FMT: Final = "%Y-%m-%d %H:%M:%S"
_ENGINE: Final = "ReplacingMergeTree(locked_at)"
_REPLICATED_ENGINE: Final = "ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', locked_at)"


class LockError(Exception):
    """The migration lock is held by another worker."""

    def __init__(self, locked_by: str, locked_at: dt.datetime, expires_at: dt.datetime) -> None:
        """Describe the current holder of the lock."""
        self.locked_by = locked_by
        self.locked_at = locked_at
        self.expires_at = expires_at
        super().__init__(
            f"{_describe_holder(locked_by, locked_at, expires_at)}\nUse 'force-unlock' command to release it manually.",
        )


class LockTimeoutError(LockError):
    """The migration lock stayed held by another worker through every retry."""

    def __init__(self, locked_by: str, locked_at: dt.datetime, expires_at: dt.datetime, retries: int) -> None:
        """Describe the current holder of the lock and how many retries were made."""
        self.retries = retries
        super().__init__(locked_by, locked_at, expires_at)
        holder = _describe_holder(locked_by, locked_at, expires_at)
        self.args = (
            f"{holder}\nTimed out after {retries} retries. Use 'force-unlock' command to release it manually.",
        )


@dataclass
class LockInfo:
    """The active lock row: who holds it, since when, and when it expires."""

    locked_by: str
    locked_at: dt.datetime
    expires_at: dt.datetime


class MigrationLock:
    """Distributed advisory lock for safe concurrent migrations.

    Args:
        client: ClickHouse client connected to the target server.
        db: Database that holds the lock table.
        ttl: Lock expiration time in seconds.
        retry_count: Number of acquire retries when lock is held.
        retry_delay: Seconds between acquire retries.
        cluster: ClickHouse cluster name for replicated lock table.

    Raises:
        ValueError: ``db`` or ``cluster`` is not a valid SQL identifier.

    """

    def __init__(
        self,
        client: Client,
        db: str,
        ttl: int = DEFAULT_LOCK_TTL,
        retry_count: int = 0,
        retry_delay: float = 1.0,
        cluster: str = "",
    ) -> None:
        """Validate names, pick a unique owner id for this worker, and create the lock table."""
        if not is_sql_identifier(db):
            raise ValueError(f"Invalid database name: {db!r}")
        if cluster and not is_sql_identifier(cluster):
            raise ValueError(f"Invalid cluster name: {cluster!r}")
        self._client = client
        self._db = db
        self._ttl = ttl
        self._retry_count = retry_count
        self._retry_delay = retry_delay
        self._cluster = cluster
        self._settings: ClickHouseSettings = cluster_settings(cluster)
        self._locked_by = _new_owner_id()
        self.ensure_table()

    def __enter__(self) -> Self:
        """Acquire the lock with the retry settings given to the constructor."""
        self.acquire(retry_count=self._retry_count, retry_delay=self._retry_delay)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Release the lock; a failed release is logged so it never hides the original error."""
        try:
            self.release()
        except Exception:
            logger.exception("Failed to release migration lock")

    def ensure_table(self) -> None:
        """Create the lock table if it does not exist."""
        on_cluster = on_cluster_clause(self._cluster)
        engine = _REPLICATED_ENGINE if self._cluster else _ENGINE
        self._client.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._db}.{LOCK_TABLE} {on_cluster} (
                lock_id    String    DEFAULT 'migration',
                locked_by  String,
                locked_at  DateTime64(3) DEFAULT now64(3),
                expires_at DateTime64(3),
                is_locked  UInt8     DEFAULT 1
            ) ENGINE = {engine}
            ORDER BY lock_id
            """,
        )

    def acquire(self, retry_count: int = 0, retry_delay: float = 1.0) -> None:
        """Acquire the migration lock.

        Args:
            retry_count: Number of retries if lock is already held.
            retry_delay: Seconds between retries.

        Raises:
            LockError: the lock is held by another worker and ``retry_count`` is zero.
            LockTimeoutError: the lock is still held by another worker after all retries.

        """
        holder = self._acquire_once()
        for attempt in range(1, retry_count + 1):
            if holder is None:
                return
            logger.debug(
                "Lock held by %s, retrying in %.1fs (%d/%d)",
                holder.locked_by,
                retry_delay,
                attempt,
                retry_count,
            )
            time.sleep(retry_delay)
            holder = self._acquire_once()
        if holder is not None:
            raise _lock_held_error(holder, retries=retry_count)

    def release(self, *, force: bool = False) -> None:
        """Release the migration lock.

        Without ``force``, only the worker that holds the lock releases it.
        """
        if not force and not self._holds_lock():
            return
        locked_by = "force_release" if force else self._locked_by
        self._client.execute(
            f"""
            INSERT INTO {self._db}.{LOCK_TABLE}
                (lock_id, locked_by, locked_at, expires_at, is_locked)
            SELECT
                %(lock_id)s,
                %(locked_by)s,
                now64(3),
                now64(3),
                0
            """,
            {"lock_id": _LOCK_ID, "locked_by": locked_by},
            settings=self._settings,
        )
        logger.debug("Lock released by %s", locked_by)

    def is_locked(self) -> bool:
        """Check whether the migration lock is currently held."""
        return self._get_active_lock() is not None

    def get_lock_info(self) -> LockInfo | None:
        """Return info about the active lock, or None if unlocked."""
        return self._get_active_lock()

    def _acquire_once(self) -> LockInfo | None:
        holder = self._get_active_lock()
        if holder is None:
            return self._try_acquire()
        return holder

    def _try_acquire(self) -> LockInfo | None:
        self._client.execute(
            f"""
            INSERT INTO {self._db}.{LOCK_TABLE}
                (lock_id, locked_by, locked_at, expires_at, is_locked)
            SELECT
                %(lock_id)s,
                %(locked_by)s,
                now64(3),
                now64(3) + INTERVAL %(ttl)s SECOND,
                1
            """,
            {"lock_id": _LOCK_ID, "locked_by": self._locked_by, "ttl": self._ttl},
            settings=self._settings,
        )
        holder = self._get_active_lock()
        if holder is not None and holder.locked_by == self._locked_by:
            logger.debug("Lock acquired by %s", self._locked_by)
            return None
        return holder

    def _holds_lock(self) -> bool:
        holder = self._get_active_lock()
        if holder is None:
            logger.debug("No active lock to release")
            return False
        if holder.locked_by != self._locked_by:
            logger.warning(
                "Lock is held by another worker %s, skipping release (current worker: %s)",
                holder.locked_by,
                self._locked_by,
            )
            return False
        return True

    def _get_active_lock(self) -> LockInfo | None:
        rows = self._client.execute(
            f"""
            SELECT
                locked_by,
                locked_at,
                expires_at
            FROM {self._db}.{LOCK_TABLE} FINAL
            WHERE lock_id = %(lock_id)s
                AND is_locked = 1
                AND expires_at > now64(3)
            """,
            {"lock_id": _LOCK_ID},
            settings=self._settings,
        )
        if not rows:
            return None
        locked_by, locked_at, expires_at = rows[0]
        return LockInfo(locked_by=locked_by, locked_at=locked_at, expires_at=expires_at)


def _new_owner_id() -> str:
    host = socket.gethostname()
    token = uuid4().hex[:_WORKER_TOKEN_LENGTH]
    return f"{host}:{os.getpid()}:{token}"


def _describe_holder(locked_by: str, locked_at: dt.datetime, expires_at: dt.datetime) -> str:
    since = locked_at.strftime(_DT_FMT)
    expires = expires_at.strftime(_DT_FMT)
    return f"Migration lock is held by {locked_by} (since {since}, expires {expires})."


def _lock_held_error(holder: LockInfo, *, retries: int) -> LockError:
    if retries > 0:
        return LockTimeoutError(holder.locked_by, holder.locked_at, holder.expires_at, retries=retries)
    return LockError(holder.locked_by, holder.locked_at, holder.expires_at)
