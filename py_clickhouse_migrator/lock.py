from __future__ import annotations

import datetime as dt
import logging
import os
import re
import socket
import time
from dataclasses import dataclass
from types import TracebackType

from clickhouse_driver import Client

logger = logging.getLogger("py_clickhouse_migrator")

_DB_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

ClickHouseSettings = dict[str, str | int]

_CLUSTER_SETTINGS: ClickHouseSettings = {
    "insert_quorum": "auto",
    "select_sequential_consistency": 1,
}
_DT_FMT = "%Y-%m-%d %H:%M:%S"


def _fmt_dt(value: dt.datetime) -> str:
    return value.strftime(_DT_FMT)


class LockError(Exception):
    def __init__(self, locked_by: str, locked_at: dt.datetime, expires_at: dt.datetime) -> None:
        self.locked_by = locked_by
        self.locked_at = locked_at
        self.expires_at = expires_at
        super().__init__(
            f"Migration lock is held by {locked_by} (since {_fmt_dt(locked_at)}, expires {_fmt_dt(expires_at)}).\n"
            f"Use 'force-unlock' command to release it manually."
        )


class LockTimeoutError(LockError):
    def __init__(self, locked_by: str, locked_at: dt.datetime, expires_at: dt.datetime, retries: int) -> None:
        self.retries = retries
        super().__init__(locked_by, locked_at, expires_at)
        self.args = (
            f"Migration lock is held by {locked_by} "
            f"(since {_fmt_dt(locked_at)}, expires {_fmt_dt(expires_at)}).\n"
            f"Timed out after {retries} retries. Use 'force-unlock' command to release it manually.",
        )


@dataclass
class LockInfo:
    locked_by: str
    locked_at: dt.datetime
    expires_at: dt.datetime


class MigrationLock:
    _LOCK_TABLE = "_migrations_lock"
    _LOCK_ID = "migration"

    def __init__(
        self,
        client: Client,
        db: str,
        ttl: int = 300,
        retry_count: int = 0,
        retry_delay: float = 1.0,
        cluster: str = "",
    ) -> None:
        if not _DB_NAME_RE.match(db):
            raise ValueError(f"Invalid database name: {db!r}")
        self._client = client
        self._db = db
        self._ttl = ttl
        self._retry_count = retry_count
        self._retry_delay = retry_delay
        self._cluster = cluster
        self._settings: ClickHouseSettings = _CLUSTER_SETTINGS.copy() if self._cluster else {}
        self._locked_by = f"{socket.gethostname()}:{os.getpid()}"
        self.ensure_table()

    def ensure_table(self) -> None:
        on_cluster = f"ON CLUSTER {self._cluster}" if self._cluster else ""
        engine = (
            "ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', locked_at)"
            if self._cluster
            else "ReplacingMergeTree(locked_at)"
        )
        self._client.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._db}.{self._LOCK_TABLE} {on_cluster} (
                lock_id    String    DEFAULT 'migration',
                locked_by  String,
                locked_at  DateTime64(3) DEFAULT now64(3),
                expires_at DateTime64(3),
                is_locked  UInt8     DEFAULT 1
            ) ENGINE = {engine}
            ORDER BY lock_id
            """
        )

    def _try_acquire(self) -> LockInfo | None:
        """Insert a lock row and verify ownership. Returns None on success, LockInfo of holder on failure."""
        now = dt.datetime.now(tz=dt.timezone.utc)
        expires = now + dt.timedelta(seconds=self._ttl)
        self._client.execute(
            f"INSERT INTO {self._db}.{self._LOCK_TABLE} (lock_id, locked_by, locked_at, expires_at, is_locked) VALUES",
            [[self._LOCK_ID, self._locked_by, now, expires, 1]],
            settings=self._settings,
        )
        verified = self._get_active_lock()
        if verified is not None and verified.locked_by == self._locked_by:
            logger.debug("Lock acquired by %s", self._locked_by)
            return None
        return verified

    def acquire(self, retry_count: int = 0, retry_delay: float = 1.0) -> None:
        for attempt in range(retry_count + 1):
            lock_info = self._get_active_lock()
            if lock_info is None:
                holder = self._try_acquire()
                if holder is None:
                    return
                lock_info = holder

            if lock_info is not None and attempt < retry_count:
                logger.debug(
                    "Lock held by %s, retrying in %.1fs (%d/%d)",
                    lock_info.locked_by,
                    retry_delay,
                    attempt + 1,
                    retry_count,
                )
                time.sleep(retry_delay)

        # Last chance: lock may have been released after the loop
        lock_info = self._get_active_lock()
        if lock_info is None:
            holder = self._try_acquire()
            if holder is None:
                return
            lock_info = holder

        if retry_count > 0:
            raise LockTimeoutError(
                locked_by=lock_info.locked_by,
                locked_at=lock_info.locked_at,
                expires_at=lock_info.expires_at,
                retries=retry_count,
            )
        raise LockError(
            locked_by=lock_info.locked_by,
            locked_at=lock_info.locked_at,
            expires_at=lock_info.expires_at,
        )

    def release(self) -> None:
        now = dt.datetime.now(tz=dt.timezone.utc)
        self._client.execute(
            f"INSERT INTO {self._db}.{self._LOCK_TABLE} (lock_id, locked_by, locked_at, expires_at, is_locked) VALUES",
            [[self._LOCK_ID, self._locked_by, now, now, 0]],
            settings=self._settings,
        )
        logger.debug("Lock released by %s", self._locked_by)

    def force_release(self) -> None:
        now = dt.datetime.now(tz=dt.timezone.utc)
        self._client.execute(
            f"INSERT INTO {self._db}.{self._LOCK_TABLE} (lock_id, locked_by, locked_at, expires_at, is_locked) VALUES",
            [[self._LOCK_ID, "force_release", now, now, 0]],
            settings=self._settings,
        )
        logger.info("Lock forcefully released.")

    def is_locked(self) -> bool:
        return self._get_active_lock() is not None

    def get_lock_info(self) -> LockInfo | None:
        return self._get_active_lock()

    def _get_active_lock(self) -> LockInfo | None:
        rows = self._client.execute(
            f"SELECT locked_by, locked_at, expires_at "
            f"FROM {self._db}.{self._LOCK_TABLE} FINAL "
            f"WHERE lock_id = %(lock_id)s AND is_locked = 1 AND expires_at > now64(3)",
            {"lock_id": self._LOCK_ID},
            settings=self._settings,
        )
        if not rows:
            return None
        return LockInfo(locked_by=rows[0][0], locked_at=rows[0][1], expires_at=rows[0][2])

    def __enter__(self) -> MigrationLock:
        self.acquire(retry_count=self._retry_count, retry_delay=self._retry_delay)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        try:
            self.release()
        except Exception:
            logger.exception("Failed to release migration lock")
