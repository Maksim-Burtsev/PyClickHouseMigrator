from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Final

import pytest
from clickhouse_driver import Client

from py_clickhouse_migrator.lock import LOCK_TABLE, LockError, MigrationLock
from py_clickhouse_migrator.migrator import Migrator, create_migrations_dir
from tests.migration_files import create_test_migration
from tests.queries import get_engine, table_exists

CLUSTER_NAME: Final = "test_cluster"
DB: Final = "test"
NODE1_URL: Final = "clickhouse://default@localhost:19001/test"
NODE2_URL: Final = "clickhouse://default@localhost:19002/test"
MIGRATIONS_TABLE: Final = "db_migrations"
USER_TABLE: Final = "test_user_table"
NO_OP_SQL: Final = "SELECT 1"
LOCK_TTL_SECONDS: Final = 300
POD_LOCK_TTL_SECONDS: Final = 30
POD_LOCK_RETRIES: Final = 10
POD_RETRY_DELAY_SECONDS: Final = 1.0
SECOND_POD_START_DELAY_SECONDS: Final = 0.5
POD_TIMEOUT_SECONDS: Final = 15

pytestmark = pytest.mark.cluster


@dataclass
class _PodOutcome:
    """What each of two concurrently started pods observed."""

    first_pod_migrated: bool = False
    unapplied_seen_by_second_pod: list[str] | None = None


@pytest.fixture
def node1() -> Client:
    """Client for the first cluster node, where the migrator runs."""
    return Client.from_url(NODE1_URL)


@pytest.fixture
def node2() -> Client:
    """Client for the second cluster node, used to check that changes replicated."""
    return Client.from_url(NODE2_URL)


@pytest.fixture
def cluster_migrator(node1: Client) -> Iterator[Migrator]:
    """Cluster-mode migrator on node 1; drops the replicated service tables on every node afterwards."""
    create_migrations_dir()
    yield Migrator(database_url=NODE1_URL, cluster=CLUSTER_NAME)
    for service_table in (MIGRATIONS_TABLE, LOCK_TABLE):
        node1.execute(f"DROP TABLE IF EXISTS {service_table} ON CLUSTER {CLUSTER_NAME} SYNC")


def _node2_migrator() -> Migrator:
    return Migrator(database_url=NODE2_URL, cluster=CLUSTER_NAME)


def _cluster_lock(node: Client) -> MigrationLock:
    return MigrationLock(client=node, db=DB, ttl=LOCK_TTL_SECONDS, cluster=CLUSTER_NAME)


def _exists_on(table: str, *nodes: Client) -> list[bool]:
    return [table_exists(node, table) for node in nodes]


def _read_consistently(node: Client, query: str) -> list[tuple[object, ...]]:
    return list(node.execute(query, settings={"select_sequential_consistency": 1}))


def _run_first_pod(outcome: _PodOutcome) -> None:
    migrator = Migrator(database_url=NODE1_URL, cluster=CLUSTER_NAME)
    with MigrationLock(client=migrator.ch_client, db=DB, ttl=POD_LOCK_TTL_SECONDS, cluster=CLUSTER_NAME):
        migrator.up()
    outcome.first_pod_migrated = True


def _run_second_pod(outcome: _PodOutcome) -> None:
    time.sleep(SECOND_POD_START_DELAY_SECONDS)
    migrator = _node2_migrator()
    waiting_lock = MigrationLock(
        client=migrator.ch_client,
        db=DB,
        ttl=POD_LOCK_TTL_SECONDS,
        retry_count=POD_LOCK_RETRIES,
        retry_delay=POD_RETRY_DELAY_SECONDS,
        cluster=CLUSTER_NAME,
    )
    with waiting_lock:
        outcome.unapplied_seen_by_second_pod = migrator.get_unapplied_migration_names()


def _run_pods(outcome: _PodOutcome, *pods: Callable[[_PodOutcome], None]) -> None:
    threads = [threading.Thread(target=pod, args=(outcome,)) for pod in pods]
    for thread in threads:
        thread.start()
    for started_thread in threads:
        started_thread.join(timeout=POD_TIMEOUT_SECONDS)


@pytest.mark.usefixtures("cluster_migrator")
def test_migrations_table_exists_on_both_nodes(node1: Client, node2: Client) -> None:
    """``db_migrations`` is created ON CLUSTER, so every node can check what was applied."""
    assert _exists_on(MIGRATIONS_TABLE, node1, node2) == [True, True]


@pytest.mark.usefixtures("cluster_migrator")
def test_migrations_table_engine_is_replicated(node1: Client) -> None:
    """``db_migrations`` uses a Replicated engine, so rows written on one node reach the others."""
    assert "Replicated" in get_engine(node1, MIGRATIONS_TABLE)


@pytest.mark.usefixtures("cluster_migrator")
def test_lock_table_exists_on_both_nodes(node1: Client, node2: Client) -> None:
    """The lock table is created ON CLUSTER, so workers on any node contend for the same lock."""
    _cluster_lock(node1)

    assert _exists_on(LOCK_TABLE, node1, node2) == [True, True]


@pytest.mark.usefixtures("cluster_migrator")
def test_lock_table_engine_is_replicated(node1: Client) -> None:
    """The lock table uses a Replicated engine, so a lock taken on one node is seen on the others."""
    _cluster_lock(node1)

    assert "Replicated" in get_engine(node1, LOCK_TABLE)


def test_migration_from_node1_visible_on_node2(cluster_migrator: Migrator, node2: Client) -> None:
    """A migration applied through node 1 is recorded on node 2, so no node applies it twice."""
    filename = create_test_migration(name="cluster_test", up=NO_OP_SQL, rollback=NO_OP_SQL)

    cluster_migrator.up()

    assert (filename,) in _read_consistently(node2, "SELECT name FROM db_migrations ORDER BY dt")


def test_baseline_on_node1_visible_on_node2(cluster_migrator: Migrator, node2: Client) -> None:
    """A baseline taken through node 1 is recorded on node 2, so a migrator there has nothing to apply."""
    filename = create_test_migration(name="cluster_baseline", up=NO_OP_SQL, rollback=NO_OP_SQL)

    cluster_migrator.baseline()

    baselined = _read_consistently(node2, "SELECT name, toString(kind) FROM db_migrations ORDER BY dt")
    assert baselined == [(filename, "baseline")]
    assert _node2_migrator().get_unapplied_migration_names() == []


def test_migration_creates_table_on_both_nodes(cluster_migrator: Migrator, node1: Client, node2: Client) -> None:
    """Migration with ON CLUSTER DDL creates a user table on both nodes, and its rollback drops it from both."""
    create_test_migration(
        name="replicated_table",
        up=(
            f"CREATE TABLE IF NOT EXISTS {USER_TABLE} ON CLUSTER {CLUSTER_NAME}"
            " (id Int32, name String)"
            " ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')"
            " ORDER BY id"
        ),
        rollback=f"DROP TABLE IF EXISTS {USER_TABLE} ON CLUSTER {CLUSTER_NAME} SYNC",
    )
    cluster_migrator.up()

    assert _exists_on(USER_TABLE, node1, node2) == [True, True]

    cluster_migrator.rollback()

    assert _exists_on(USER_TABLE, node1, node2) == [False, False]


def test_rollback_on_node1_reflected_on_node2(cluster_migrator: Migrator, node2: Client) -> None:
    """A rollback through node 1 removes the record on node 2, so the migration can be applied again."""
    create_test_migration(name="cluster_rollback", up=NO_OP_SQL, rollback=NO_OP_SQL)
    cluster_migrator.up()
    cluster_migrator.rollback()

    assert _read_consistently(node2, "SELECT count() FROM db_migrations") == [(0,)]


def test_migrator_on_node2_sees_node1_migrations(cluster_migrator: Migrator) -> None:
    """A migrator started on node 2 after node 1 migrated finds nothing left to apply."""
    create_test_migration(name="handoff_test", up=NO_OP_SQL, rollback=NO_OP_SQL)

    cluster_migrator.up()

    assert _node2_migrator().get_unapplied_migration_names() == []


@pytest.mark.usefixtures("cluster_migrator")
def test_lock_on_node1_visible_on_node2(node1: Client, node2: Client) -> None:
    """A lock taken and released on node 1 shows as held and then free on node 2."""
    node1_lock = _cluster_lock(node1)
    node2_lock = _cluster_lock(node2)

    node1_lock.acquire()
    assert node2_lock.is_locked()

    node1_lock.release()
    assert not node2_lock.is_locked()


@pytest.mark.usefixtures("cluster_migrator")
def test_lock_on_node1_blocks_acquire_on_node2(node1: Client, node2: Client) -> None:
    """A worker on node 2 cannot take the lock while a worker on node 1 holds it."""
    node1_lock = _cluster_lock(node1)
    node2_lock = _cluster_lock(node2)

    node1_lock.acquire()

    with pytest.raises(LockError):
        node2_lock.acquire()

    node1_lock.release()


@pytest.mark.usefixtures("cluster_migrator")
def test_concurrent_pods_race_condition() -> None:
    """Two pods start simultaneously. Pod1 migrates, pod2 waits for lock, then sees nothing to apply."""
    create_test_migration(name="race_test", up="SELECT sleep(2)", rollback=NO_OP_SQL)
    outcome = _PodOutcome()

    _run_pods(outcome, _run_first_pod, _run_second_pod)

    assert outcome == _PodOutcome(first_pod_migrated=True, unapplied_seen_by_second_pod=[])
