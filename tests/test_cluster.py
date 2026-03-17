from __future__ import annotations

import os
import shutil
import threading
import time
from collections.abc import Generator
from typing import Any

import pytest
from clickhouse_driver import Client

from py_clickhouse_migrator.lock import LockError, MigrationLock
from py_clickhouse_migrator.migrator import Migrator

from tests.helpers import create_test_migration, get_engine, table_exists

CLUSTER_NAME = "test_cluster"
NODE_1_URL = "clickhouse://default@localhost:19001/test"
NODE_2_URL = "clickhouse://default@localhost:19002/test"

pytestmark = pytest.mark.cluster


@pytest.fixture()
def node1() -> Client:
    return Client.from_url(NODE_1_URL)


@pytest.fixture()
def node2() -> Client:
    return Client.from_url(NODE_2_URL)


@pytest.fixture(autouse=True)
def clean_db_dir() -> Generator[None]:
    yield
    if os.path.exists("./db"):
        shutil.rmtree("./db")


@pytest.fixture()
def cluster_migrator(node1: Client) -> Generator[Migrator]:
    m = Migrator(database_url=NODE_1_URL, cluster=CLUSTER_NAME)
    m.init()
    yield m
    node1.execute(f"DROP TABLE IF EXISTS db_migrations ON CLUSTER {CLUSTER_NAME} SYNC")
    node1.execute(f"DROP TABLE IF EXISTS _migrations_lock ON CLUSTER {CLUSTER_NAME} SYNC")


# --- Group 1: Service tables replicated ---


def test_migrations_table_exists_on_both_nodes(cluster_migrator: Migrator, node1: Client, node2: Client) -> None:
    assert table_exists(node1, "db_migrations")
    assert table_exists(node2, "db_migrations")


def test_migrations_table_engine_is_replicated(cluster_migrator: Migrator, node1: Client) -> None:
    engine = get_engine(node1, "db_migrations")
    assert "Replicated" in engine


def test_lock_table_exists_on_both_nodes(node1: Client, node2: Client, cluster_migrator: Migrator) -> None:
    MigrationLock(client=node1, db="test", cluster=CLUSTER_NAME)
    assert table_exists(node1, "_migrations_lock")
    assert table_exists(node2, "_migrations_lock")


def test_lock_table_engine_is_replicated(node1: Client, cluster_migrator: Migrator) -> None:
    MigrationLock(client=node1, db="test", cluster=CLUSTER_NAME)
    engine = get_engine(node1, "_migrations_lock")
    assert "Replicated" in engine


# --- Group 2: Migration state replication ---


def test_migration_applied_on_node1_visible_on_node2(cluster_migrator: Migrator, node2: Client) -> None:
    filename = create_test_migration(
        name="cluster_test",
        up="SELECT 1",
        rollback="SELECT 1",
        migrator=cluster_migrator,
    )
    cluster_migrator.up()

    rows = node2.execute(
        "SELECT name FROM db_migrations ORDER BY dt",
        settings={"select_sequential_consistency": 1},
    )
    names = [r[0] for r in rows]
    assert filename in names


def test_migration_creates_table_on_both_nodes(cluster_migrator: Migrator, node1: Client, node2: Client) -> None:
    """Migration with ON CLUSTER DDL creates user table visible on both nodes."""
    create_test_migration(
        name="replicated_table",
        up=(
            f"CREATE TABLE IF NOT EXISTS test_user_table ON CLUSTER {CLUSTER_NAME}"
            " (id Int32, name String)"
            " ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')"
            " ORDER BY id"
        ),
        rollback=f"DROP TABLE IF EXISTS test_user_table ON CLUSTER {CLUSTER_NAME} SYNC",
        migrator=cluster_migrator,
    )
    cluster_migrator.up()

    assert table_exists(node1, "test_user_table")
    assert table_exists(node2, "test_user_table")

    cluster_migrator.rollback()

    assert not table_exists(node1, "test_user_table")
    assert not table_exists(node2, "test_user_table")


def test_rollback_on_node1_reflected_on_node2(cluster_migrator: Migrator, node2: Client) -> None:
    create_test_migration(
        name="cluster_rollback",
        up="SELECT 1",
        rollback="SELECT 1",
        migrator=cluster_migrator,
    )
    cluster_migrator.up()
    cluster_migrator.rollback()

    count = node2.execute(
        "SELECT count() FROM db_migrations",
        settings={"select_sequential_consistency": 1},
    )[0][0]
    assert count == 0


# --- Group 3: Cross-node migrator handoff ---


def test_migrator_on_node2_sees_node1_migrations(cluster_migrator: Migrator) -> None:
    create_test_migration(
        name="handoff_test",
        up="SELECT 1",
        rollback="SELECT 1",
        migrator=cluster_migrator,
    )
    cluster_migrator.up()

    m2 = Migrator(database_url=NODE_2_URL, cluster=CLUSTER_NAME)
    unapplied = m2.get_unapplied_migration_names()
    assert unapplied == []


# --- Group 4: Distributed lock ---


@pytest.mark.usefixtures("cluster_migrator")
def test_lock_on_node1_visible_on_node2(node1: Client, node2: Client) -> None:
    lock1 = MigrationLock(client=node1, db="test", ttl=300, cluster=CLUSTER_NAME)
    lock2 = MigrationLock(client=node2, db="test", ttl=300, cluster=CLUSTER_NAME)

    lock1.acquire()
    assert lock2.is_locked()

    lock1.release()
    assert not lock2.is_locked()


@pytest.mark.usefixtures("cluster_migrator")
def test_lock_on_node1_blocks_acquire_on_node2(node1: Client, node2: Client) -> None:
    lock1 = MigrationLock(client=node1, db="test", ttl=300, cluster=CLUSTER_NAME)
    lock2 = MigrationLock(client=node2, db="test", ttl=300, cluster=CLUSTER_NAME)

    lock1.acquire()

    with pytest.raises(LockError):
        lock2.acquire()

    lock1.release()


# --- Group 5: Concurrent pods race condition ---


def test_concurrent_pods_race_condition(cluster_migrator: Migrator) -> None:
    """Two pods start simultaneously. Pod1 migrates, pod2 waits for lock, then sees nothing to apply."""
    create_test_migration(
        name="race_test",
        up="SELECT sleep(2)",
        rollback="SELECT 1",
        migrator=cluster_migrator,
    )

    results: dict[str, Any] = {}

    def pod1() -> None:
        m = Migrator(database_url=NODE_1_URL, cluster=CLUSTER_NAME)
        with MigrationLock(client=m.ch_client, db="test", ttl=30, cluster=CLUSTER_NAME):
            m.up()
        results["pod1"] = "migrated"

    def pod2() -> None:
        time.sleep(0.5)
        m = Migrator(database_url=NODE_2_URL, cluster=CLUSTER_NAME)
        lock = MigrationLock(
            client=m.ch_client, db="test", ttl=30, retry_count=10, retry_delay=1.0, cluster=CLUSTER_NAME
        )
        with lock:
            unapplied = m.get_unapplied_migration_names()
            results["pod2"] = unapplied

    t1 = threading.Thread(target=pod1)
    t2 = threading.Thread(target=pod2)
    t1.start()
    t2.start()
    t1.join(timeout=15)
    t2.join(timeout=15)

    assert results["pod1"] == "migrated"
    assert results["pod2"] == []
