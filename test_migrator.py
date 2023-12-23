import os
import re
import shutil

import pytest
from clickhouse_driver import Client
from clickhouse_driver.errors import ServerException

from py_clickhouse_migrate.migrator import (
    DEFATULT_MIGRATIONS_DIR,
    ClickHouseServerIsNotHealthyError,
    InvalidMigrationError,
    Migration,
    Migrator,
    MissingDatabaseUrlError,
)

MIGRATION_FILENAME_REGEX = re.compile(r"^\d{8}\d{4}(?:_\w+)*\.py$")

TEST_MIGRATION_TEMPLATE: str = '''
def up() -> str:
    return """{up}"""


def rollback() -> str:
    return """{rollback}"""
'''


def create_test_migration(
    name: str,
    up: str,
    rollback: str,
    migrator: Migrator = Migrator(database_url="clickhouse://default@127.0.0.1:9000/default"),
) -> str:
    filename: str = migrator.get_new_migration_filename(name)
    filepath: str = f"{DEFATULT_MIGRATIONS_DIR}/{filename}"
    with open(filepath, "w") as f:
        f.write(TEST_MIGRATION_TEMPLATE.format(up=up, rollback=rollback))

    return filename


@pytest.fixture(scope="function")
def migrator_init(migrator: Migrator, ch_client: Client):
    migrator.init()

    yield

    # clean
    shutil.rmtree("./db")
    ch_client.execute("DROP TABLE IF EXISTS db_migrations")


@pytest.fixture(scope="function")
def test_table_from_migration(migrator: Migrator, migrator_init: None, ch_client: Client) -> str:
    filename: str = create_test_migration(
        name="test_1",
        up="CREATE TABLE IF NOT EXISTS test_table (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table",
    )
    migrator.up()

    yield filename

    ch_client.execute("DROP TABLE IF EXISTS test_table")


@pytest.fixture(scope="function")
def test_tables_from_migration(migrator: Migrator, migrator_init: None, ch_client: Client) -> list[str]:
    filename_1: str = create_test_migration(
        name="test_1",
        up="CREATE TABLE IF NOT EXISTS test_table_1 (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_2",
    )
    filename_2: str = create_test_migration(
        name="test_2",
        up="CREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_2",
    )
    filename_3: str = create_test_migration(
        name="test_3",
        up="CREATE TABLE IF NOT EXISTS test_table_3 (id String) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_3",
    )
    migrator.up()

    yield [filename_1, filename_2, filename_3]

    ch_client.execute("DROP TABLE IF EXISTS test_table_1")
    ch_client.execute("DROP TABLE IF EXISTS test_table_2")
    ch_client.execute("DROP TABLE IF EXISTS test_table_3")


def test_init_base(migrator: Migrator, ch_client: Client):
    with pytest.raises(ServerException):
        ch_client.execute("CHECK TABLE db_migrations")

    assert not os.path.exists("./db/schema.sql")
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
    assert os.path.exists("./db/schema.sql")

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


def test_create_new_migration(migrator: Migrator, ch_client: Client, migrator_init: None):
    assert not os.listdir(DEFATULT_MIGRATIONS_DIR)
    assert not ch_client.execute("SELECT count() FROM db_migrations")[0][0]

    migrator.create_new_migration("first_migration")
    assert not ch_client.execute("SELECT count() FROM db_migrations")[0][0]
    migration_filenames: list[str] = os.listdir(DEFATULT_MIGRATIONS_DIR)
    assert len(migration_filenames) == 1

    filename: str = migration_filenames[0]
    assert "_first_migration.py" in filename


def test_apply_migration_one_query(migrator: Migrator, ch_client: Client):
    with pytest.raises(ServerException):
        ch_client.execute("CHECK TABLE test_table")

    migrator.apply_migration("CREATE TABLE IF NOT EXISTS test_table (id Integer) Engine=MergeTree() ORDER BY id;")
    assert ch_client.execute("CHECK TABLE test_table")
    assert ch_client.execute("DESCRIBE TABLE test_table")[0][:2] == ("id", "Int32")

    # clean
    ch_client.execute("DROP TABLE IF EXISTS test_table")


def test_apply_migration_multiquery(migrator: Migrator, ch_client: Client):
    with pytest.raises(ServerException):
        ch_client.execute("CHECK TABLE test_table_int_id")

    with pytest.raises(ServerException):
        ch_client.execute("CHECK TABLE test_table_str_id")

    migrator.apply_migration(
        "CREATE TABLE IF NOT EXISTS test_table_int_id (id Integer) Engine=MergeTree() ORDER BY id;"
        "CREATE TABLE IF NOT EXISTS test_table_str_id (id String) Engine=MergeTree() ORDER BY id;"
        "INSERT INTO TABLE test_table_int_id VALUES (1), (2), (3);"
        "INSERT INTO TABLE test_table_str_id VALUES ('17afaed9-ef50-4a2e-a91d-af7cc8344033'),"
        " ('744aa7d7-568b-48f2-80a1-ef0aaf18fc1b'), ('22405e14-e82a-4ab7-a502-05b40bbbd791')"
    )

    assert ch_client.execute("CHECK TABLE test_table_int_id")
    assert ch_client.execute("DESCRIBE TABLE test_table_int_id")[0][:2] == ("id", "Int32")
    assert ch_client.execute("CHECK TABLE test_table_str_id")
    assert ch_client.execute("DESCRIBE TABLE test_table_str_id")[0][:2] == ("id", "String")

    assert ch_client.execute("SELECT id FROM test_table_int_id") == [(1,), (2,), (3,)]
    assert ch_client.execute("SELECT id FROM test_table_str_id") == [
        ("17afaed9-ef50-4a2e-a91d-af7cc8344033",),
        ("22405e14-e82a-4ab7-a502-05b40bbbd791",),
        ("744aa7d7-568b-48f2-80a1-ef0aaf18fc1b",),
    ]

    # clean
    ch_client.execute("DROP TABLE IF EXISTS test_table_int_id")
    ch_client.execute("DROP TABLE IF EXISTS test_table_str_id")


def test_get_migrations_for_apply_empty(migrator: Migrator, migrator_init: None):
    filepath: str = migrator.create_new_migration(name="test")
    assert os.path.exists(filepath)

    assert not migrator.get_migrations_for_apply()


def test_get_all_migrations_for_apply(migrator: Migrator, migrator_init: None):
    migration_1: str = create_test_migration(
        name="test_1",
        up="CREATE TABLE IF NOT EXISTS test_table_1 (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_1",
    )
    migration_2: str = create_test_migration(
        name="test_2",
        up="CREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_2",
    )

    migrations: list[Migration] = migrator.get_migrations_for_apply()
    assert len(migrations) == 2

    assert migrations[0].name == migration_1
    assert migrations[1].name == migration_2

    assert migrations[0].up == "CREATE TABLE IF NOT EXISTS test_table_1 (id Integer) Engine=MergeTree() ORDER BY id;"
    assert migrations[1].up == "CREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;"

    assert migrations[0].rollback == "DROP TABLE IF EXISTS test_table_1"
    assert migrations[1].rollback == "DROP TABLE IF EXISTS test_table_2"


def test_get_few_migrations_for_apply_with_number(migrator: Migrator, migrator_init: None, ch_client: Client):
    migration_1: str = create_test_migration(
        name="test_1",
        up="CREATE TABLE IF NOT EXISTS test_table_1 (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_1",
    )
    migration_2: str = create_test_migration(
        name="test_2",
        up="CREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_2",
    )
    migration_3: str = create_test_migration(
        name="test_3",
        up="CREATE TABLE IF NOT EXISTS test_table_3 (id String) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_3",
    )

    migrations: list[Migration] = migrator.get_migrations_for_apply(number=2)
    assert len(migrations) == 2

    assert migrations[0].name == migration_1
    assert migrations[1].name == migration_2

    assert migrations[0].up == "CREATE TABLE IF NOT EXISTS test_table_1 (id Integer) Engine=MergeTree() ORDER BY id;"
    assert migrations[1].up == "CREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;"

    assert migrations[0].rollback == "DROP TABLE IF EXISTS test_table_1"
    assert migrations[1].rollback == "DROP TABLE IF EXISTS test_table_2"

    assert os.path.exists(f"{DEFATULT_MIGRATIONS_DIR}/{migration_3}")
    assert not ch_client.execute(f"SELECT * FROM db_migrations WHERE name='{migration_3}'")

    assert len(migrator.get_migrations_for_apply()) == 3  # get migrations for apply without number


def test_get_migrations_for_rollback(migrator: Migrator, test_tables_from_migration: list[str], ch_client: Client):
    assert ch_client.execute("SELECT count() from db_migrations")[0][0] == 3

    migrations: list[Migration] = migrator.get_migrations_for_rollback(number=2)

    assert len(migrations) == 2
    assert migrations[0].name == test_tables_from_migration[2]
    assert migrations[0].up == "CREATE TABLE IF NOT EXISTS test_table_3 (id String) Engine=MergeTree() ORDER BY id;"
    assert migrations[0].rollback == "DROP TABLE IF EXISTS test_table_3"

    assert migrations[1].name == test_tables_from_migration[1]
    assert migrations[1].up == "CREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;"
    assert migrations[1].rollback == "DROP TABLE IF EXISTS test_table_2"

    assert (
        ch_client.execute(f"SELECT count() FROM db_migrations WHERE name='{test_tables_from_migration[0]}'")[0][0] == 1
    )
    all_migrations_for_rollback: list[Migration] = migrator.get_migrations_for_rollback()
    assert len(all_migrations_for_rollback) == 1  # by default 1
    assert all_migrations_for_rollback[0].name == test_tables_from_migration[2]
    assert (
        all_migrations_for_rollback[0].up
        == "CREATE TABLE IF NOT EXISTS test_table_3 (id String) Engine=MergeTree() ORDER BY id;"
    )
    assert all_migrations_for_rollback[0].rollback == "DROP TABLE IF EXISTS test_table_3"


def test_get_new_migration_filename(migrator: Migrator):
    filename: str = migrator.get_new_migration_filename()
    assert MIGRATION_FILENAME_REGEX.match(filename)


def test_get_new_migration_filename_with_name(migrator: Migrator):
    filename: str = migrator.get_new_migration_filename("test_migration")
    assert "_test_migration.py" in filename
    assert MIGRATION_FILENAME_REGEX.match(filename)


def test_get_applied_migrations_names(migrator: Migrator, test_tables_from_migration: list[str], ch_client: Client):
    migration_names: list[str] = migrator.get_applied_migrations_names()
    assert len(migration_names) == 3

    db_migration_names: list[str] = [row[0] for row in ch_client.execute("SELECT name FROM db_migrations ORDER BY dt")]
    assert len(db_migration_names) == 3

    assert migration_names == db_migration_names


def test_up_one_query(migrator: Migrator, migrator_init: None, ch_client: Client):
    with pytest.raises(ServerException):
        assert not ch_client.execute("CHECK TABLE test_table")

    filename: str = create_test_migration(
        name="test",
        up="CREATE TABLE IF NOT EXISTS test_table (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table",
    )
    assert os.path.exists(f"{DEFATULT_MIGRATIONS_DIR}/{filename}")
    assert ch_client.execute(f"SELECT count() FROM db_migrations WHERE name='{filename}'")[0][0] == 0

    migrator.up()
    assert ch_client.execute(f"SELECT count() FROM db_migrations WHERE name='{filename}'")[0][0] == 1
    assert ch_client.execute("CHECK TABLE test_table")
    assert ch_client.execute("DESCRIBE TABLE test_table")[0][:2] == ("id", "Int32")

    # clean
    ch_client.execute("DROP TABLE IF EXISTS test_table")


def test_up_multiquery(migrator: Migrator, migrator_init, ch_client: Client):
    with pytest.raises(ServerException):
        assert not ch_client.execute("CHECK TABLE test_table_1")

    with pytest.raises(ServerException):
        assert not ch_client.execute("CHECK TABLE test_table_2")

    filename: str = create_test_migration(
        name="test_multiquery",
        up="CREATE TABLE IF NOT EXISTS test_table_1 (id Integer) Engine=MergeTree() ORDER BY id;"
        "CREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_1;DROP TABLE IF EXISTS test_table_2",
    )
    assert os.path.exists(f"{DEFATULT_MIGRATIONS_DIR}/{filename}")
    assert ch_client.execute(f"SELECT count() FROM db_migrations WHERE name='{filename}'")[0][0] == 0

    migrator.up()
    assert ch_client.execute(f"SELECT count() FROM db_migrations WHERE name='{filename}'")[0][0] == 1
    assert ch_client.execute("CHECK TABLE test_table_1")
    assert ch_client.execute("CHECK TABLE test_table_2")
    assert ch_client.execute("DESCRIBE TABLE test_table_1")[0][:2] == ("id", "Int32")
    assert ch_client.execute("DESCRIBE TABLE test_table_2")[0][:2] == ("id", "String")

    # clean
    ch_client.execute("DROP TABLE IF EXISTS test_table_1")
    ch_client.execute("DROP TABLE IF EXISTS test_table_2")


def test_up_multiply_files(migrator: Migrator, migrator_init, ch_client: Client):
    with pytest.raises(ServerException):
        assert not ch_client.execute("CHECK TABLE test_table_1")

    with pytest.raises(ServerException):
        assert not ch_client.execute("CHECK TABLE test_table_2")

    filename_1: str = create_test_migration(
        name="test_1",
        up="CREATE TABLE IF NOT EXISTS test_table_1 (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_2",
    )
    filename_2: str = create_test_migration(
        name="test_2",
        up="CREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_2",
    )

    assert os.path.exists(f"{DEFATULT_MIGRATIONS_DIR}/{filename_1}")
    assert os.path.exists(f"{DEFATULT_MIGRATIONS_DIR}/{filename_2}")
    assert ch_client.execute("SELECT count() FROM db_migrations")[0][0] == 0

    migrator.up()
    assert ch_client.execute("SELECT count() FROM db_migrations")[0][0] == 2
    assert ch_client.execute("CHECK TABLE test_table_1")
    assert ch_client.execute("CHECK TABLE test_table_2")
    assert ch_client.execute("DESCRIBE TABLE test_table_1")[0][:2] == ("id", "Int32")
    assert ch_client.execute("DESCRIBE TABLE test_table_2")[0][:2] == ("id", "String")

    assert sorted(migrator.get_applied_migrations_names()) == sorted([filename_1, filename_2])

    # clean
    ch_client.execute("DROP TABLE IF EXISTS test_table_1")
    ch_client.execute("DROP TABLE IF EXISTS test_table_2")


def test_rollback_one_query_migration(migrator: Migrator, test_tables_from_migration, ch_client):
    assert ch_client.execute("SELECT count() FROM db_migrations")[0][0] == 3
    assert ch_client.execute("CHECK TABLE test_table_3")
    assert sorted(migrator.get_applied_migrations_names()) == sorted(test_tables_from_migration)

    migrator.rollback()

    assert ch_client.execute("SELECT count() FROM db_migrations")[0][0] == 2
    assert sorted(migrator.get_applied_migrations_names()) == [
        test_tables_from_migration[0],
        test_tables_from_migration[1],
    ]
    with pytest.raises(ServerException):
        assert ch_client.execute("CHECK TABLE test_table_3")


def test_rollback_multiply_migrations(migrator: Migrator, test_tables_from_migration, ch_client):
    assert ch_client.execute("SELECT count() FROM db_migrations")[0][0] == 3
    assert ch_client.execute("CHECK TABLE test_table_3")
    assert ch_client.execute("CHECK TABLE test_table_2")
    assert sorted(migrator.get_applied_migrations_names()) == sorted(test_tables_from_migration)

    migrator.rollback(number=2)

    assert ch_client.execute("SELECT count() FROM db_migrations")[0][0] == 1
    assert sorted(migrator.get_applied_migrations_names()) == [
        test_tables_from_migration[0],
    ]
    with pytest.raises(ServerException):
        assert ch_client.execute("CHECK TABLE test_table_2")
    with pytest.raises(ServerException):
        assert ch_client.execute("CHECK TABLE test_table_3")


def test_rollback_multiquery_migration(migrator: Migrator, test_table_from_migration, ch_client):
    assert ch_client.execute("CHECK TABLE test_table")
    filename: str = create_test_migration(
        name="test_multiquery",
        up="CREATE TABLE IF NOT EXISTS test_table_1 (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_1; INSERT INTO test_table(id) VALUES (1),(2),(3);",
    )
    migrator.up()
    assert os.path.exists(f"{DEFATULT_MIGRATIONS_DIR}/{filename}")
    assert ch_client.execute("CHECK TABLE test_table_1")
    assert ch_client.execute("SELECT count() FROM test_table")[0][0] == 0
    assert ch_client.execute(f"SELECT count() FROM db_migrations WHERE name='{filename}'")[0][0] == 1
    assert ch_client.execute("SELECT count() FROM db_migrations")[0][0] == 2

    migrator.rollback()

    assert ch_client.execute("SELECT count() FROM db_migrations")[0][0] == 1
    assert ch_client.execute(f"SELECT count() FROM db_migrations WHERE name='{filename}'")[0][0] == 0
    with pytest.raises(ServerException):
        assert ch_client.execute("CHECK TABLE test_table_1")

    # check inserted from rollback values
    assert [row[0] for row in ch_client.execute("SELECT id FROM test_table")] == [1, 2, 3]


def test_save_applied_migration(migrator: Migrator, ch_client: Client, migrator_init: None):
    assert not ch_client.execute("SELECT * FROM db_migrations")

    migrator.save_applied_migration(
        name="test",
        up="CREATE TABLE IF NOT EXISTS test_table (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table;",
    )

    assert ch_client.execute("SELECT count() FROM db_migrations")[0][0] == 1
    row = ch_client.execute("SELECT name, up, rollback FROM db_migrations LIMIT 1")[0]

    assert row[0] == "test"
    assert row[1] == "CREATE TABLE IF NOT EXISTS test_table (id Integer) Engine=MergeTree() ORDER BY id;"
    assert row[2] == "DROP TABLE IF EXISTS test_table;"

    # clean
    ch_client.execute("DELETE FROM db_migrations WHERE name='test'")


def test_delete_migration(migrator: Migrator, ch_client: Client, migrator_init: None):
    assert not ch_client.execute("SELECT * FROM db_migrations")
    ch_client.execute(
        "INSERT INTO db_migrations (name, up, rollback) VALUES "
        "('test.sql',"
        " 'CREATE TABLE IF NOT EXISTS test_table (id Integer) Engine=MergeTree() ORDER BY id;',"
        " 'DROP TABLE IF EXISTS test_table')"
    )
    assert ch_client.execute("SELECT count() FROM db_migrations WHERE name='test.sql'")[0][0] == 1

    migrator.delete_migration("test.sql")

    assert not ch_client.execute("SELECT * FROM db_migrations")


def test_save_current_schema(migrator: Migrator, ch_client: Client):
    assert not os.path.exists("./db/schema.sql")

    migrator.init()

    assert os.path.exists("./db/schema.sql")
    with open("./db/schema.sql", "r") as f:
        assert (
            f.read()
            == """---- Database schema ----

CREATE TABLE IF NOT EXISTS test.db_migrations
(
    `name` String,
    `up` String,
    `rollback` String,
    `dt` DateTime64(3) DEFAULT now()
)
ENGINE = MergeTree
ORDER BY dt
SETTINGS index_granularity = 8192;"""
        )

    # clean
    shutil.rmtree("./db")
    ch_client.execute("DROP TABLE IF EXISTS db_migrations")


def test_apply_invalid_migration(migrator: Migrator, ch_client: Client):
    with pytest.raises(ServerException):
        ch_client.execute("CHECK TABLE test_table")

    with pytest.raises(InvalidMigrationError):
        migrator.apply_migration("ALTER TABLE test_table ADD COLUMN IF NOT EXISTS new_column Integer;")


def test_missing_database_url_error():
    with pytest.raises(MissingDatabaseUrlError):
        _ = Migrator()
