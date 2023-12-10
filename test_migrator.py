import os
import shutil

import pytest
from clickhouse_driver import Client
from clickhouse_driver.errors import ServerException

from py_clickhouse_migrate.migrator import DEFATULT_MIGRATIONS_DIR, ClickHouseServerIsNotHealthyError, Migrator


@pytest.fixture(scope="function")
def migrator_init(migrator: Migrator, ch_client: Client):
    migrator.init()
    yield
    # clean
    shutil.rmtree("./db")
    ch_client.execute("DROP TABLE IF EXISTS db_migrations")


def test_init_base(migrator: Migrator, ch_client: Client):
    with pytest.raises(ServerException):
        ch_client.execute("CHECK TABLE db_migrations")

    assert not os.path.exists(DEFATULT_MIGRATIONS_DIR)
    migrator.init()
    assert ch_client.execute("CHECK TABLE db_migrations")
    assert (
        ch_client.execute("SHOW CREATE TABLE db_migrations")[0][0]
        == "CREATE TABLE test.db_migrations\n(\n    `name` String,\n    `up` String,\n    `rollback` String,\n    `dt`"
        " DateTime64(3) DEFAULT now()\n)\nENGINE = MergeTree\nORDER BY dt\nSETTINGS index_granularity = 8192"
    )
    assert not ch_client.execute("SELECT * FROM db_migrations")

    assert os.path.exists(DEFATULT_MIGRATIONS_DIR)

    # clean
    shutil.rmtree("./db")
    ch_client.execute("DROP TABLE IF EXISTS db_migrations")


def test_init_with_database_creation(test_db: str, ch_client: Client):
    with pytest.raises(ServerException):
        ch_client.execute("CHECK TABLE default.db_migrations")

    assert not os.path.exists(DEFATULT_MIGRATIONS_DIR)
    default_db_url: str = test_db.rsplit("/", 1)[0] + "/default"
    migrator = Migrator(default_db_url)
    migrator.init()
    assert ch_client.execute("CHECK TABLE default.db_migrations")

    # clean
    shutil.rmtree("./db")
    ch_client.execute("DROP TABLE IF EXISTS default.db_migrations")
    ch_client.execute("DROP DATABASE IF EXISTS default")


def test_init_with_invalid_database_url(test_db: str):
    default_db_url: str = test_db.replace("localhost", "some_domain")
    with pytest.raises(ClickHouseServerIsNotHealthyError):
        Migrator(default_db_url)


def test_create_existend_migrations_directory(migrator: Migrator, ch_client: Client):
    os.makedirs(DEFATULT_MIGRATIONS_DIR)
    with open(f"{DEFATULT_MIGRATIONS_DIR}/test_migration.sql", "w"):
        ...
    assert os.path.exists(DEFATULT_MIGRATIONS_DIR)
    with pytest.raises(ServerException):
        ch_client.execute("CHECK TABLE db_migrations")

    migrator.init()
    assert ch_client.execute("CHECK TABLE db_migrations")
    assert os.path.exists(DEFATULT_MIGRATIONS_DIR)
    assert os.path.exists(f"{DEFATULT_MIGRATIONS_DIR}/test_migration.sql")

    # clean
    shutil.rmtree("./db")
    ch_client.execute("DROP TABLE IF EXISTS db_migrations")


def test_create_new_migration(migrator: Migrator, ch_client: Client, migrator_init):
    assert not os.listdir(DEFATULT_MIGRATIONS_DIR)
    assert not ch_client.execute("SELECT count() FROM db_migrations")[0][0]

    migrator.create_new_migration("first_migration")
    assert not ch_client.execute("SELECT count() FROM db_migrations")[0][0]
    migration_filenames: list[str] = os.listdir(DEFATULT_MIGRATIONS_DIR)
    assert len(migration_filenames) == 1

    filename: str = migration_filenames[0]
    assert filename.startswith("0")
    assert "_first_migration.py" in filename


def apply_migration_one_query(migrator: Migrator, ch_client: Client):
    with pytest.raises(ServerException):
        ch_client.execute("CHECK TABLE test_table")

    migrator.apply_migration("CREATE TABLE IF NOT EXISTS test_table (id Integer) Engine=MergeTree() ORDER BY id;")
    assert ch_client.execute("CHECK TABLE test_table")
    assert ch_client.execute("DESCRIBE TABLE test_table")[0][:2] == ("id", "Int32")

    # clean
    ch_client.execute("DROP TABLE IF EXISTS test_table")


# def test_apply_migration_multiquery():
#     pass


# def test_get_migrations_for_apply():
#     pass


# def test_get_migrations_for_apply_with_number():
#     pass


# def test_get_migrations_for_rollback():
#     pass


# def test_get_migrations_for_rollback_with_number():
#     pass


# def test_get_new_migration_filename():
#     pass


# def test_get_new_migration_filename_with_name():
#     pass


# def test_get_applied_migrations_names():
#     pass


# def test_up_one_query():
#     pass


# def test_up_multiquery():
#     pass


# def test_up_multiply_files():
#     pass


# def test_rollback_one_query_migration():
#     pass


# def test_rollback_multiply_migrations():
#     pass


# def test_save_applied_migration():
#     pass


# def test_delete_migration():
#     pass


# def test_save_current_schema():
#     pass
