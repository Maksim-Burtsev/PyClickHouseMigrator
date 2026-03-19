from __future__ import annotations

import logging
import os
import shutil
from unittest.mock import patch

import click
import pytest
from clickhouse_driver import Client

from py_clickhouse_migrator.migrator import (
    DEFAULT_MIGRATIONS_DIR,
    ClickHouseServerIsNotHealthyError,
    InvalidMigrationError,
    Migration,
    MigrationDirectoryNotFoundError,
    Migrator,
    MissingDatabaseUrlError,
)


from tests.helpers import MIGRATION_FILENAME_REGEX, create_test_migration, table_exists


def test_db_migrations_table_creation(ch_client: Client, test_db: str) -> None:
    ch_client.execute("DROP TABLE IF EXISTS db_migrations")
    assert not table_exists(ch_client, "db_migrations")

    Migrator(database_url=test_db)

    assert table_exists(ch_client, "db_migrations")
    expected_schema = (
        "CREATE TABLE test.db_migrations\n"
        "(\n"
        "    `name` String,\n"
        "    `up` String,\n"
        "    `rollback` String,\n"
        "    `dt` DateTime64(3) DEFAULT now(),\n"
        "    `checksum` String DEFAULT ''\n"
        ")\n"
        "ENGINE = MergeTree\n"
        "ORDER BY dt\n"
        "SETTINGS index_granularity = 8192"
    )
    assert ch_client.execute("SHOW CREATE TABLE db_migrations")[0][0] == expected_schema
    assert not ch_client.execute("SELECT * FROM db_migrations")

    # clean
    ch_client.execute("DROP TABLE IF EXISTS db_migrations")


def test_init_base(migrator: Migrator, ch_client: Client) -> None:
    assert not os.path.exists(DEFAULT_MIGRATIONS_DIR)

    migrator.init()

    assert os.path.exists(DEFAULT_MIGRATIONS_DIR)

    # clean
    ch_client.execute("DROP TABLE IF EXISTS db_migrations")


def test_init_with_database_creation(test_db: str, ch_client: Client) -> None:
    ch_client.execute("DROP TABLE IF EXISTS default.db_migrations")
    assert not table_exists(ch_client, "db_migrations") or True  # might exist from other tests

    default_db_url: str = test_db.rsplit("/", 1)[0] + "/default"  # switch db from test to default
    migrator = Migrator(default_db_url)
    migrator.init()

    result = ch_client.execute(
        "SELECT count() FROM system.tables WHERE database = 'default' AND name = 'db_migrations'"
    )
    assert result[0][0] > 0

    # clean
    ch_client.execute("DROP TABLE IF EXISTS default.db_migrations")


def test_init_with_invalid_database_url(test_db: str) -> None:
    default_db_url: str = test_db.replace("localhost", "some_domain")
    with pytest.raises(ClickHouseServerIsNotHealthyError):
        Migrator(default_db_url)


def test_create_existend_migrations_directory(migrator: Migrator, ch_client: Client) -> None:
    os.makedirs(DEFAULT_MIGRATIONS_DIR, exist_ok=True)
    with open(f"{DEFAULT_MIGRATIONS_DIR}/test_migration.sql", "w"):
        ...
    assert os.path.exists(DEFAULT_MIGRATIONS_DIR)

    migrator.init()

    assert os.path.exists(DEFAULT_MIGRATIONS_DIR)
    assert os.path.exists(f"{DEFAULT_MIGRATIONS_DIR}/test_migration.sql")

    # clean
    ch_client.execute("DROP TABLE IF EXISTS db_migrations")


def test_create_new_migration(migrator: Migrator, ch_client: Client, migrator_init: None) -> None:
    assert not os.listdir(DEFAULT_MIGRATIONS_DIR)
    assert not ch_client.execute("SELECT count() FROM db_migrations")[0][0]

    migrator.create_new_migration("first_migration")
    assert not ch_client.execute("SELECT count() FROM db_migrations")[0][0]
    migration_filenames: list[str] = os.listdir(DEFAULT_MIGRATIONS_DIR)
    assert len(migration_filenames) == 1

    filename: str = migration_filenames[0]
    assert "_first_migration.py" in filename


def test_create_new_migration_without_init(test_db: str) -> None:
    migrator = Migrator(test_db)
    shutil.rmtree("./db", ignore_errors=True)
    with pytest.raises(MigrationDirectoryNotFoundError):
        migrator.create_new_migration(name="test")
    # clean
    migrator.ch_client.execute("DROP TABLE IF EXISTS db_migrations")


def test_apply_migration_one_query(migrator: Migrator, ch_client: Client) -> None:
    ch_client.execute("DROP TABLE IF EXISTS test_table")
    assert not table_exists(ch_client, "test_table")

    migrator.apply_migration("CREATE TABLE IF NOT EXISTS test_table (id Integer) Engine=MergeTree() ORDER BY id;")
    assert table_exists(ch_client, "test_table")
    assert ch_client.execute("DESCRIBE TABLE test_table")[0][:2] == ("id", "Int32")

    # clean
    ch_client.execute("DROP TABLE IF EXISTS test_table")


def test_apply_migration_multiquery(migrator: Migrator, ch_client: Client) -> None:
    assert not table_exists(ch_client, "test_table_int_id")
    assert not table_exists(ch_client, "test_table_str_id")

    migrator.apply_migration(
        "CREATE TABLE IF NOT EXISTS test_table_int_id (id Integer) Engine=MergeTree() ORDER BY id;"
        "CREATE TABLE IF NOT EXISTS test_table_str_id (id String) Engine=MergeTree() ORDER BY id;"
        "INSERT INTO TABLE test_table_int_id VALUES (1), (2), (3);"
        "INSERT INTO TABLE test_table_str_id VALUES ('17afaed9-ef50-4a2e-a91d-af7cc8344033'),"
        " ('744aa7d7-568b-48f2-80a1-ef0aaf18fc1b'), ('22405e14-e82a-4ab7-a502-05b40bbbd791')"
    )

    assert table_exists(ch_client, "test_table_int_id")
    assert ch_client.execute("DESCRIBE TABLE test_table_int_id")[0][:2] == ("id", "Int32")
    assert table_exists(ch_client, "test_table_str_id")
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


def test_get_migrations_for_apply_is_empty(migrator: Migrator, migrator_init: None) -> None:
    filepath: str = migrator.create_new_migration(name="test")
    assert os.path.exists(filepath)

    assert not migrator.get_migrations_for_apply()


def test_get_all_migrations_for_apply(migrator: Migrator, migrator_init: None) -> None:
    migration_1: str = create_test_migration(
        name="test_1",
        up="CREATE TABLE IF NOT EXISTS test_table_1 (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_1",
        migrator=migrator,
    )
    migration_2: str = create_test_migration(
        name="test_2",
        up="CREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_2",
        migrator=migrator,
    )

    migrations: list[Migration] = migrator.get_migrations_for_apply()
    assert len(migrations) == 2

    assert migrations[0].name == migration_1
    assert migrations[1].name == migration_2

    assert migrations[0].up == "CREATE TABLE IF NOT EXISTS test_table_1 (id Integer) Engine=MergeTree() ORDER BY id;"
    assert migrations[1].up == "CREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;"

    assert migrations[0].rollback == "DROP TABLE IF EXISTS test_table_1"
    assert migrations[1].rollback == "DROP TABLE IF EXISTS test_table_2"


def test_get_few_migrations_for_apply_with_number(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    migration_1: str = create_test_migration(
        name="test_1",
        up="CREATE TABLE IF NOT EXISTS test_table_1 (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_1",
        migrator=migrator,
    )
    migration_2: str = create_test_migration(
        name="test_2",
        up="CREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_2",
        migrator=migrator,
    )
    migration_3: str = create_test_migration(
        name="test_3",
        up="CREATE TABLE IF NOT EXISTS test_table_3 (id String) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_3",
        migrator=migrator,
    )

    migrations: list[Migration] = migrator.get_migrations_for_apply(number=2)
    assert len(migrations) == 2

    assert migrations[0].name == migration_1
    assert migrations[1].name == migration_2

    assert migrations[0].up == "CREATE TABLE IF NOT EXISTS test_table_1 (id Integer) Engine=MergeTree() ORDER BY id;"
    assert migrations[1].up == "CREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;"

    assert migrations[0].rollback == "DROP TABLE IF EXISTS test_table_1"
    assert migrations[1].rollback == "DROP TABLE IF EXISTS test_table_2"

    assert os.path.exists(f"{DEFAULT_MIGRATIONS_DIR}/{migration_3}")
    assert not ch_client.execute(f"SELECT * FROM db_migrations WHERE name='{migration_3}'")

    assert len(migrator.get_migrations_for_apply()) == 3  # get migrations for apply without number


def test_get_migrations_for_rollback(
    migrator: Migrator, test_tables_from_migration: list[str], ch_client: Client
) -> None:
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


def test_get_new_migration_filename(migrator: Migrator) -> None:
    filename: str = migrator.get_new_migration_filename()
    assert MIGRATION_FILENAME_REGEX.match(filename)


def test_get_new_migration_filename_with_name(migrator: Migrator) -> None:
    filename: str = migrator.get_new_migration_filename("test_migration")
    assert "_test_migration.py" in filename
    assert MIGRATION_FILENAME_REGEX.match(filename)


def test_get_applied_migrations_names(
    migrator: Migrator, test_tables_from_migration: list[str], ch_client: Client
) -> None:
    migration_names: list[str] = migrator.get_applied_migrations_names()
    assert len(migration_names) == 3

    db_migration_names: list[str] = [row[0] for row in ch_client.execute("SELECT name FROM db_migrations ORDER BY dt")]
    assert len(db_migration_names) == 3

    assert migration_names == db_migration_names


def test_up_one_query(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    ch_client.execute("DROP TABLE IF EXISTS test_table")
    assert not table_exists(ch_client, "test_table")

    filename: str = create_test_migration(
        name="test",
        up="CREATE TABLE IF NOT EXISTS test_table (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table",
        migrator=migrator,
    )
    assert os.path.exists(f"{DEFAULT_MIGRATIONS_DIR}/{filename}")
    assert ch_client.execute(f"SELECT count() FROM db_migrations WHERE name='{filename}'")[0][0] == 0

    migrator.up()
    assert ch_client.execute(f"SELECT count() FROM db_migrations WHERE name='{filename}'")[0][0] == 1
    assert table_exists(ch_client, "test_table")
    assert ch_client.execute("DESCRIBE TABLE test_table")[0][:2] == ("id", "Int32")

    # clean
    ch_client.execute("DROP TABLE IF EXISTS test_table")


def test_up_multiquery(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    assert not table_exists(ch_client, "test_table_1")
    assert not table_exists(ch_client, "test_table_2")

    filename: str = create_test_migration(
        name="test_multiquery",
        up="CREATE TABLE IF NOT EXISTS test_table_1 (id Integer) Engine=MergeTree() ORDER BY id;"
        "CREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_1;DROP TABLE IF EXISTS test_table_2",
        migrator=migrator,
    )
    assert os.path.exists(f"{DEFAULT_MIGRATIONS_DIR}/{filename}")
    assert ch_client.execute(f"SELECT count() FROM db_migrations WHERE name='{filename}'")[0][0] == 0

    migrator.up()
    assert ch_client.execute(f"SELECT count() FROM db_migrations WHERE name='{filename}'")[0][0] == 1
    assert table_exists(ch_client, "test_table_1")
    assert table_exists(ch_client, "test_table_2")
    assert ch_client.execute("DESCRIBE TABLE test_table_1")[0][:2] == ("id", "Int32")
    assert ch_client.execute("DESCRIBE TABLE test_table_2")[0][:2] == ("id", "String")

    # clean
    ch_client.execute("DROP TABLE IF EXISTS test_table_1")
    ch_client.execute("DROP TABLE IF EXISTS test_table_2")


def test_up_multiquery_with_line_breakes(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    assert not table_exists(ch_client, "test_table_1")
    assert not table_exists(ch_client, "test_table_2")

    filename: str = create_test_migration(
        name="test_multiquery",
        up="CREATE TABLE IF NOT EXISTS test_table_1 (id Integer) Engine=MergeTree() ORDER BY id;\n\n"
        "CREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;   \n\n",
        rollback="DROP TABLE IF EXISTS test_table_1;DROP TABLE IF EXISTS test_table_2 \n\n\n",
        migrator=migrator,
    )
    assert ch_client.execute(f"SELECT count() FROM db_migrations WHERE name='{filename}'")[0][0] == 0

    migrator.up()
    assert ch_client.execute(f"SELECT count() FROM db_migrations WHERE name='{filename}'")[0][0] == 1
    assert table_exists(ch_client, "test_table_1")
    assert table_exists(ch_client, "test_table_2")
    assert ch_client.execute("DESCRIBE TABLE test_table_1")[0][:2] == ("id", "Int32")
    assert ch_client.execute("DESCRIBE TABLE test_table_2")[0][:2] == ("id", "String")

    # clean
    ch_client.execute("DROP TABLE IF EXISTS test_table_1")
    ch_client.execute("DROP TABLE IF EXISTS test_table_2")


def test_up_multiply_files(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    assert not table_exists(ch_client, "test_table_1")
    assert not table_exists(ch_client, "test_table_2")

    filename_1: str = create_test_migration(
        name="test_1",
        up="CREATE TABLE IF NOT EXISTS test_table_1 (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_2",
        migrator=migrator,
    )
    filename_2: str = create_test_migration(
        name="test_2",
        up="CREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_2",
        migrator=migrator,
    )

    assert os.path.exists(f"{DEFAULT_MIGRATIONS_DIR}/{filename_1}")
    assert os.path.exists(f"{DEFAULT_MIGRATIONS_DIR}/{filename_2}")
    assert ch_client.execute("SELECT count() FROM db_migrations")[0][0] == 0

    migrator.up()
    assert ch_client.execute("SELECT count() FROM db_migrations")[0][0] == 2
    assert table_exists(ch_client, "test_table_1")
    assert table_exists(ch_client, "test_table_2")
    assert ch_client.execute("DESCRIBE TABLE test_table_1")[0][:2] == ("id", "Int32")
    assert ch_client.execute("DESCRIBE TABLE test_table_2")[0][:2] == ("id", "String")

    assert sorted(migrator.get_applied_migrations_names()) == sorted([filename_1, filename_2])

    # clean
    ch_client.execute("DROP TABLE IF EXISTS test_table_1")
    ch_client.execute("DROP TABLE IF EXISTS test_table_2")


def test_rollback_one_query_migration(
    migrator: Migrator, test_tables_from_migration: list[str], ch_client: Client
) -> None:
    assert ch_client.execute("SELECT count() FROM db_migrations")[0][0] == 3
    assert table_exists(ch_client, "test_table_3")
    assert sorted(migrator.get_applied_migrations_names()) == sorted(test_tables_from_migration)

    migrator.rollback()

    assert ch_client.execute("SELECT count() FROM db_migrations")[0][0] == 2
    assert sorted(migrator.get_applied_migrations_names()) == [
        test_tables_from_migration[0],
        test_tables_from_migration[1],
    ]
    assert not table_exists(ch_client, "test_table_3")


def test_rollback_multiply_migrations(
    migrator: Migrator, test_tables_from_migration: list[str], ch_client: Client
) -> None:
    assert ch_client.execute("SELECT count() FROM db_migrations")[0][0] == 3
    assert table_exists(ch_client, "test_table_3")
    assert table_exists(ch_client, "test_table_2")
    assert sorted(migrator.get_applied_migrations_names()) == sorted(test_tables_from_migration)

    migrator.rollback(number=2)

    assert ch_client.execute("SELECT count() FROM db_migrations")[0][0] == 1
    assert sorted(migrator.get_applied_migrations_names()) == [
        test_tables_from_migration[0],
    ]
    assert not table_exists(ch_client, "test_table_2")
    assert not table_exists(ch_client, "test_table_3")


def test_rollback_multiquery_migration(migrator: Migrator, test_table_from_migration: str, ch_client: Client) -> None:
    assert table_exists(ch_client, "test_table")
    filename: str = create_test_migration(
        name="test_multiquery",
        up="CREATE TABLE IF NOT EXISTS test_table_1 (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_1; INSERT INTO test_table(id) VALUES (1),(2),(3);",
        migrator=migrator,
    )
    migrator.up()
    assert os.path.exists(f"{DEFAULT_MIGRATIONS_DIR}/{filename}")
    assert table_exists(ch_client, "test_table_1")
    assert ch_client.execute("SELECT count() FROM test_table")[0][0] == 0
    assert ch_client.execute(f"SELECT count() FROM db_migrations WHERE name='{filename}'")[0][0] == 1
    assert ch_client.execute("SELECT count() FROM db_migrations")[0][0] == 2

    migrator.rollback()

    assert ch_client.execute("SELECT count() FROM db_migrations")[0][0] == 1
    assert ch_client.execute(f"SELECT count() FROM db_migrations WHERE name='{filename}'")[0][0] == 0
    assert not table_exists(ch_client, "test_table_1")

    # check inserted from rollback values
    assert [row[0] for row in ch_client.execute("SELECT id FROM test_table")] == [1, 2, 3]


def test_save_applied_migration(migrator: Migrator, ch_client: Client, migrator_init: None) -> None:
    assert not ch_client.execute("SELECT * FROM db_migrations")

    migrator.save_applied_migration(
        name="test",
        up="CREATE TABLE IF NOT EXISTS test_table (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table;",
        checksum="abc123",
    )

    assert ch_client.execute("SELECT count() FROM db_migrations")[0][0] == 1
    row = ch_client.execute("SELECT name, up, rollback, checksum FROM db_migrations LIMIT 1")[0]

    assert row[0] == "test"
    assert row[1] == "CREATE TABLE IF NOT EXISTS test_table (id Integer) Engine=MergeTree() ORDER BY id;"
    assert row[2] == "DROP TABLE IF EXISTS test_table;"
    assert row[3] == "abc123"

    # clean
    ch_client.execute("DELETE FROM db_migrations WHERE name='test'")


def test_delete_migration(migrator: Migrator, ch_client: Client, migrator_init: None) -> None:
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


def test_apply_invalid_migration(migrator: Migrator, ch_client: Client) -> None:
    assert not table_exists(ch_client, "test_table")

    with pytest.raises(InvalidMigrationError):
        migrator.apply_migration("ALTER TABLE test_table ADD COLUMN IF NOT EXISTS new_column Integer;")


def test_missing_database_url_error() -> None:
    with pytest.raises(MissingDatabaseUrlError):
        _ = Migrator()


def test_up_dry_run_does_not_apply(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """dry_run=True should not create tables or save migrations to db_migrations."""
    ch_client.execute("DROP TABLE IF EXISTS test_table")
    assert not table_exists(ch_client, "test_table")

    create_test_migration(
        name="test_dry",
        up="CREATE TABLE IF NOT EXISTS test_table (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table",
        migrator=migrator,
    )

    migrator.up(dry_run=True)

    assert not table_exists(ch_client, "test_table")
    assert ch_client.execute("SELECT count() FROM db_migrations")[0][0] == 0

    # migration should still be unapplied
    assert len(migrator.get_unapplied_migration_names()) == 1


def test_up_dry_run_with_number(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """dry_run with number should show only N migrations without applying."""
    create_test_migration(
        name="test_1",
        up="CREATE TABLE IF NOT EXISTS test_table_1 (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_1",
        migrator=migrator,
    )
    create_test_migration(
        name="test_2",
        up="CREATE TABLE IF NOT EXISTS test_table_2 (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_2",
        migrator=migrator,
    )

    migrator.up(n=1, dry_run=True)

    assert not table_exists(ch_client, "test_table_1")
    assert not table_exists(ch_client, "test_table_2")
    assert ch_client.execute("SELECT count() FROM db_migrations")[0][0] == 0
    assert len(migrator.get_unapplied_migration_names()) == 2


def test_rollback_dry_run_does_not_rollback(
    migrator: Migrator, test_table_from_migration: str, ch_client: Client
) -> None:
    """dry_run=True should not drop tables or delete from db_migrations."""
    assert table_exists(ch_client, "test_table")
    assert ch_client.execute("SELECT count() FROM db_migrations")[0][0] == 1

    migrator.rollback(dry_run=True)

    assert table_exists(ch_client, "test_table")
    assert ch_client.execute("SELECT count() FROM db_migrations")[0][0] == 1


def test_rollback_dry_run_multiple(
    migrator: Migrator, test_tables_from_migration: list[str], ch_client: Client
) -> None:
    """dry_run rollback of multiple migrations should leave everything intact."""
    assert ch_client.execute("SELECT count() FROM db_migrations")[0][0] == 3
    assert table_exists(ch_client, "test_table_1")
    assert table_exists(ch_client, "test_table_2")
    assert table_exists(ch_client, "test_table_3")

    migrator.rollback(number=2, dry_run=True)

    assert ch_client.execute("SELECT count() FROM db_migrations")[0][0] == 3
    assert table_exists(ch_client, "test_table_1")
    assert table_exists(ch_client, "test_table_2")
    assert table_exists(ch_client, "test_table_3")


def test_new_migration_filename_format(migrator: Migrator) -> None:
    """Filename should be 14 digits (YYYYMMDDHHmmSS) without microseconds."""
    filename: str = migrator.get_new_migration_filename("test")
    assert MIGRATION_FILENAME_REGEX.match(filename)
    # 14 digits before _
    stem = filename.split("_", maxsplit=1)[0]
    assert len(stem) == 14
    assert stem.isdigit()


def test_new_migration_filename_with_name(migrator: Migrator) -> None:
    filename: str = migrator.get_new_migration_filename("create_users")
    assert "_create_users.py" in filename
    assert MIGRATION_FILENAME_REGEX.match(filename)


def test_new_migration_filename_without_name_logs_warning(migrator: Migrator, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="py_clickhouse_migrator"):
        filename = migrator.get_new_migration_filename()
    assert "Migration name is recommended" in caplog.text
    assert MIGRATION_FILENAME_REGEX.match(filename)


def test_show_migrations_default_limits_applied(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """With >5 applied migrations, show only last 5 + '... and N more'."""
    for i in range(7):
        create_test_migration(
            name=f"table_{i}",
            up=f"CREATE TABLE IF NOT EXISTS t_{i} (id Int32) Engine=MergeTree() ORDER BY id;",
            rollback=f"DROP TABLE IF EXISTS t_{i}",
            migrator=migrator,
        )
    migrator.up()

    output, warning = migrator.show_migrations()
    plain = click.unstyle(output)

    assert plain.count("[X]") == 5
    assert "... and 2 more applied" in plain
    assert "(HEAD)" in plain
    assert "Applied: 7 | Pending: 0" in plain
    assert warning == ""

    # clean
    for i in range(7):
        ch_client.execute(f"DROP TABLE IF EXISTS t_{i}")


def test_show_migrations_all_flag(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """show_all=True should show every applied migration."""
    for i in range(7):
        create_test_migration(
            name=f"table_{i}",
            up=f"CREATE TABLE IF NOT EXISTS t_{i} (id Int32) Engine=MergeTree() ORDER BY id;",
            rollback=f"DROP TABLE IF EXISTS t_{i}",
            migrator=migrator,
        )
    migrator.up()

    output, warning = migrator.show_migrations(show_all=True)
    plain = click.unstyle(output)

    assert plain.count("[X]") == 7
    assert "... and" not in plain
    assert "(HEAD)" in plain
    assert "Applied: 7" in plain
    assert warning == ""

    # clean
    for i in range(7):
        ch_client.execute(f"DROP TABLE IF EXISTS t_{i}")


@patch.object(Migrator, "check_migrations_table")
def test_migrator_cluster_param_from_init(_mock: object, test_db: str) -> None:
    migrator = Migrator(database_url=test_db, cluster="my_cluster")
    assert migrator.cluster == "my_cluster"


def test_migrator_cluster_param_empty_by_default(ch_client: Client, test_db: str) -> None:
    ch_client.execute("DROP TABLE IF EXISTS db_migrations")
    migrator = Migrator(database_url=test_db)
    assert migrator.cluster == ""
    ch_client.execute("DROP TABLE IF EXISTS db_migrations")
