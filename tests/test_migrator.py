from __future__ import annotations

import logging
import os
import shutil
from unittest.mock import MagicMock, patch

import click
import pytest
from clickhouse_driver import Client

from py_clickhouse_migrator.errors import (
    BaselineError,
    ClickHouseServerIsNotHealthyError,
    DatabaseNotFoundError,
    InvalidMigrationError,
    InvalidStatementError,
    MigrationDirectoryNotFoundError,
    MissingDatabaseUrlError,
)
from py_clickhouse_migrator.migrator import (
    DEFAULT_MIGRATIONS_DIR,
    Migration,
    Migrator,
    create_migration_file,
    create_migrations_dir,
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
        "    `kind` Enum8('migration' = 1, 'baseline' = 2) DEFAULT 'migration',\n"
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


def test_init_base(ch_client: Client) -> None:
    assert not os.path.exists(DEFAULT_MIGRATIONS_DIR)

    create_migrations_dir()

    assert os.path.exists(DEFAULT_MIGRATIONS_DIR)


def test_nonexistent_database_raises_error() -> None:
    """Migrator should fail with clear error if database doesn't exist."""
    bad_url = "clickhouse://default@localhost:19000/this_db_does_not_exist"
    with pytest.raises(DatabaseNotFoundError, match="does not exist"):
        Migrator(database_url=bad_url)


def test_init_with_invalid_database_url(test_db: str) -> None:
    default_db_url: str = test_db.replace("localhost", "some_domain")
    with pytest.raises(ClickHouseServerIsNotHealthyError):
        Migrator(default_db_url)


def test_create_existend_migrations_directory() -> None:
    os.makedirs(DEFAULT_MIGRATIONS_DIR, exist_ok=True)
    with open(f"{DEFAULT_MIGRATIONS_DIR}/test_migration.sql", "w"):
        ...
    assert os.path.exists(DEFAULT_MIGRATIONS_DIR)

    create_migrations_dir()

    assert os.path.exists(DEFAULT_MIGRATIONS_DIR)
    assert os.path.exists(f"{DEFAULT_MIGRATIONS_DIR}/test_migration.sql")


def test_create_new_migration(migrator_init: None) -> None:
    assert not os.listdir(DEFAULT_MIGRATIONS_DIR)

    create_migration_file(name="first_migration")
    migration_filenames: list[str] = os.listdir(DEFAULT_MIGRATIONS_DIR)
    assert len(migration_filenames) == 1

    filename: str = migration_filenames[0]
    assert "_first_migration.sql" in filename


def test_create_new_migration_without_init() -> None:
    shutil.rmtree("./db", ignore_errors=True)
    with pytest.raises(MigrationDirectoryNotFoundError):
        create_migration_file(name="test")


def test_apply_migration_one_query(migrator: Migrator, ch_client: Client) -> None:
    ch_client.execute("DROP TABLE IF EXISTS test_table")
    assert not table_exists(ch_client, "test_table")

    migrator.apply_migration(["CREATE TABLE IF NOT EXISTS test_table (id Integer) Engine=MergeTree() ORDER BY id;"])
    assert table_exists(ch_client, "test_table")
    assert ch_client.execute("DESCRIBE TABLE test_table")[0][:2] == ("id", "Int32")

    # clean
    ch_client.execute("DROP TABLE IF EXISTS test_table")


def test_apply_migration_multiquery(migrator: Migrator, ch_client: Client) -> None:
    assert not table_exists(ch_client, "test_table_int_id")
    assert not table_exists(ch_client, "test_table_str_id")

    migrator.apply_migration(
        [
            "CREATE TABLE IF NOT EXISTS test_table_int_id (id Integer) Engine=MergeTree() ORDER BY id;",
            "CREATE TABLE IF NOT EXISTS test_table_str_id (id String) Engine=MergeTree() ORDER BY id;",
            "INSERT INTO TABLE test_table_int_id VALUES (1), (2), (3);",
            "INSERT INTO TABLE test_table_str_id VALUES ('17afaed9-ef50-4a2e-a91d-af7cc8344033'),"
            " ('744aa7d7-568b-48f2-80a1-ef0aaf18fc1b'), ('22405e14-e82a-4ab7-a502-05b40bbbd791')",
        ]
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


def test_up_rejects_empty_template(migrator: Migrator, migrator_init: None) -> None:
    filepath: str = create_migration_file(name="test")
    assert os.path.exists(filepath)

    with pytest.raises(
        InvalidMigrationError,
        match=r"Migration .+: Must contain at least one non-empty '-- @stmt' block",
    ):
        migrator.up()


def test_get_all_migrations_for_apply(migrator: Migrator, migrator_init: None) -> None:
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

    expected_up_1 = "-- @stmt\nCREATE TABLE IF NOT EXISTS test_table_1 (id Integer) Engine=MergeTree() ORDER BY id;"
    expected_up_2 = "-- @stmt\nCREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;"
    assert migrations[0].up == expected_up_1
    assert migrations[1].up == expected_up_2
    assert migrations[0].up_statements == [
        "CREATE TABLE IF NOT EXISTS test_table_1 (id Integer) Engine=MergeTree() ORDER BY id;"
    ]
    assert migrations[1].up_statements == [
        "CREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;"
    ]

    assert migrations[0].rollback == "-- @stmt\nDROP TABLE IF EXISTS test_table_1"
    assert migrations[1].rollback == "-- @stmt\nDROP TABLE IF EXISTS test_table_2"
    assert migrations[0].rollback_statements == ["DROP TABLE IF EXISTS test_table_1"]
    assert migrations[1].rollback_statements == ["DROP TABLE IF EXISTS test_table_2"]


def test_get_few_migrations_for_apply_with_number(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
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

    expected_up_1 = "-- @stmt\nCREATE TABLE IF NOT EXISTS test_table_1 (id Integer) Engine=MergeTree() ORDER BY id;"
    expected_up_2 = "-- @stmt\nCREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;"
    assert migrations[0].up == expected_up_1
    assert migrations[1].up == expected_up_2
    assert migrations[0].up_statements == [
        "CREATE TABLE IF NOT EXISTS test_table_1 (id Integer) Engine=MergeTree() ORDER BY id;"
    ]
    assert migrations[1].up_statements == [
        "CREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;"
    ]

    assert migrations[0].rollback == "-- @stmt\nDROP TABLE IF EXISTS test_table_1"
    assert migrations[1].rollback == "-- @stmt\nDROP TABLE IF EXISTS test_table_2"
    assert migrations[0].rollback_statements == ["DROP TABLE IF EXISTS test_table_1"]
    assert migrations[1].rollback_statements == ["DROP TABLE IF EXISTS test_table_2"]

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
    expected_up_3 = "-- @stmt\nCREATE TABLE IF NOT EXISTS test_table_3 (id String) Engine=MergeTree() ORDER BY id;"
    expected_up_2 = "-- @stmt\nCREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;"
    assert migrations[0].up == expected_up_3
    assert migrations[0].rollback == "-- @stmt\nDROP TABLE IF EXISTS test_table_3"
    assert migrations[0].up_statements == [
        "CREATE TABLE IF NOT EXISTS test_table_3 (id String) Engine=MergeTree() ORDER BY id;"
    ]
    assert migrations[0].rollback_statements == ["DROP TABLE IF EXISTS test_table_3"]

    assert migrations[1].name == test_tables_from_migration[1]
    assert migrations[1].up == expected_up_2
    assert migrations[1].rollback == "-- @stmt\nDROP TABLE IF EXISTS test_table_2"
    assert migrations[1].up_statements == [
        "CREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;"
    ]
    assert migrations[1].rollback_statements == ["DROP TABLE IF EXISTS test_table_2"]

    assert (
        ch_client.execute(f"SELECT count() FROM db_migrations WHERE name='{test_tables_from_migration[0]}'")[0][0] == 1
    )
    all_migrations_for_rollback: list[Migration] = migrator.get_migrations_for_rollback()
    assert len(all_migrations_for_rollback) == 1  # by default 1
    assert all_migrations_for_rollback[0].name == test_tables_from_migration[2]
    assert all_migrations_for_rollback[0].up == expected_up_3
    assert all_migrations_for_rollback[0].rollback == "-- @stmt\nDROP TABLE IF EXISTS test_table_3"


def test_baseline_records_sorted_rows_without_parsing_files(
    migrator: Migrator, migrator_init: None, ch_client: Client
) -> None:
    filenames = [
        "20990101000002_second.sql",
        "20990101000001_first.sql",
    ]
    for filename in filenames:
        with open(f"{DEFAULT_MIGRATIONS_DIR}/{filename}", "w", encoding="utf-8") as f:
            f.write("this is not a parsed migration file")

    with patch("py_clickhouse_migrator.migrator.load_migration_sections") as mock_load:
        migrator.baseline()

    mock_load.assert_not_called()
    rows = ch_client.execute(
        "SELECT name, toString(kind), up, rollback, checksum FROM db_migrations ORDER BY dt",
    )
    assert rows == [
        ("20990101000001_first.sql", "baseline", "", "", ""),
        ("20990101000002_second.sql", "baseline", "", "", ""),
    ]


def test_baseline_without_sql_files_is_noop(
    migrator: Migrator,
    migrator_init: None,
    ch_client: Client,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.INFO, logger="py_clickhouse_migrator"):
        migrator.baseline()

    assert "No SQL migration files found to baseline." in caplog.text
    assert ch_client.execute("SELECT count() FROM db_migrations")[0][0] == 0


def test_baseline_requires_empty_ledger(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    filename = create_test_migration(
        name="baseline_guard",
        up="CREATE TABLE IF NOT EXISTS baseline_guard (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS baseline_guard",
    )
    migrator.up()

    with pytest.raises(BaselineError, match="Baseline requires an empty db_migrations ledger."):
        migrator.baseline()

    rows = ch_client.execute("SELECT name, toString(kind) FROM db_migrations ORDER BY dt")
    assert rows == [(filename, "migration")]

    ch_client.execute("DROP TABLE IF EXISTS baseline_guard")


def test_up_after_baseline_applies_only_new_migrations_and_rollback_ignores_baselines(
    migrator: Migrator, migrator_init: None, ch_client: Client
) -> None:
    baselined_filename = create_test_migration(
        name="old_schema",
        up="CREATE TABLE IF NOT EXISTS old_schema (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS old_schema",
    )

    migrator.baseline()

    assert not table_exists(ch_client, "old_schema")

    new_filename = create_test_migration(
        name="new_schema",
        up="CREATE TABLE IF NOT EXISTS new_schema (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS new_schema",
    )

    migrator.up()

    rows = dict(ch_client.execute("SELECT name, toString(kind) FROM db_migrations"))
    assert rows == {
        baselined_filename: "baseline",
        new_filename: "migration",
    }
    assert table_exists(ch_client, "new_schema")

    rollback_candidates = migrator.get_migrations_for_rollback(number=10)
    assert [migration.name for migration in rollback_candidates] == [new_filename]

    migrator.rollback(number=10)

    assert not table_exists(ch_client, "new_schema")
    assert ch_client.execute("SELECT name, toString(kind) FROM db_migrations ORDER BY dt") == [
        (baselined_filename, "baseline"),
    ]


def test_create_migration_file_default_name(migrator_init: None) -> None:
    filepath = create_migration_file()
    filename = os.path.basename(filepath)
    assert MIGRATION_FILENAME_REGEX.match(filename)


def test_create_migration_file_with_name(migrator_init: None) -> None:
    filepath = create_migration_file(name="test_migration")
    filename = os.path.basename(filepath)
    assert "_test_migration.sql" in filename
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
        up=[
            "CREATE TABLE IF NOT EXISTS test_table_1 (id Integer) Engine=MergeTree() ORDER BY id;",
            "CREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;",
        ],
        rollback=[
            "DROP TABLE IF EXISTS test_table_1",
            "DROP TABLE IF EXISTS test_table_2",
        ],
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
        up=[
            "CREATE TABLE IF NOT EXISTS test_table_1 (id Integer) Engine=MergeTree() ORDER BY id;\n\n",
            "CREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;   \n\n",
        ],
        rollback=[
            "DROP TABLE IF EXISTS test_table_1",
            "DROP TABLE IF EXISTS test_table_2 \n\n\n",
        ],
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
    )
    filename_2: str = create_test_migration(
        name="test_2",
        up="CREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_2",
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
        rollback=[
            "DROP TABLE IF EXISTS test_table_1",
            "INSERT INTO test_table(id) VALUES (1),(2),(3);",
        ],
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
        migrator.apply_migration(["ALTER TABLE test_table ADD COLUMN IF NOT EXISTS new_column Integer;"])


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
    )
    create_test_migration(
        name="test_2",
        up="CREATE TABLE IF NOT EXISTS test_table_2 (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_table_2",
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


def test_up_validation_failure_does_not_execute_queries(
    migrator: Migrator, migrator_init: None, ch_client: Client
) -> None:
    filename = create_test_migration(
        name="validation_fail_up",
        up="CREATE TABLE IF NOT EXISTS validation_fail_up (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS validation_fail_up",
    )

    with (
        patch.object(migrator, "validate_statements", side_effect=InvalidStatementError("bad statement")),
        patch.object(migrator, "apply_migration") as mock_apply,
        patch.object(migrator, "save_applied_migration") as mock_save,
    ):
        with pytest.raises(InvalidMigrationError, match=rf"Validation failed for migration {filename}"):
            migrator.up()

    mock_apply.assert_not_called()
    mock_save.assert_not_called()
    assert not table_exists(ch_client, "validation_fail_up")
    assert ch_client.execute(f"SELECT count() FROM db_migrations WHERE name='{filename}'")[0][0] == 0


def test_rollback_validation_failure_does_not_execute_queries(
    migrator: Migrator, migrator_init: None, ch_client: Client
) -> None:
    filename = create_test_migration(
        name="validation_fail_rollback",
        up="CREATE TABLE IF NOT EXISTS validation_fail_rollback (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS validation_fail_rollback",
    )
    migrator.up()

    with (
        patch.object(migrator, "validate_statements", side_effect=InvalidStatementError("bad statement")),
        patch.object(migrator, "apply_migration") as mock_apply,
        patch.object(migrator, "delete_migration") as mock_delete,
    ):
        with pytest.raises(InvalidMigrationError, match=rf"Validation failed for migration {filename}"):
            migrator.rollback()

    mock_apply.assert_not_called()
    mock_delete.assert_not_called()
    assert table_exists(ch_client, "validation_fail_rollback")
    assert ch_client.execute(f"SELECT count() FROM db_migrations WHERE name='{filename}'")[0][0] == 1


def test_new_migration_filename_format(migrator_init: None) -> None:
    """Filename should be 14 digits (YYYYMMDDHHmmSS) without microseconds."""
    filepath = create_migration_file(name="test")
    filename = os.path.basename(filepath)
    assert MIGRATION_FILENAME_REGEX.match(filename)
    # 14 digits before _
    stem = filename.split("_", maxsplit=1)[0]
    assert len(stem) == 14
    assert stem.isdigit()


def test_new_migration_filename_with_name(migrator_init: None) -> None:
    filepath = create_migration_file(name="create_users")
    filename = os.path.basename(filepath)
    assert "_create_users.sql" in filename
    assert MIGRATION_FILENAME_REGEX.match(filename)


def test_new_migration_filename_without_name_logs_warning(
    migrator_init: None, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING, logger="py_clickhouse_migrator"):
        filepath = create_migration_file()
    assert "Migration name is recommended" in caplog.text
    filename = os.path.basename(filepath)
    assert MIGRATION_FILENAME_REGEX.match(filename)


def test_show_migrations_default_limits_applied(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """With >5 applied migrations, show only last 5 + '... and N more'."""
    for i in range(7):
        create_test_migration(
            name=f"table_{i}",
            up=f"CREATE TABLE IF NOT EXISTS t_{i} (id Int32) Engine=MergeTree() ORDER BY id;",
            rollback=f"DROP TABLE IF EXISTS t_{i}",
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


def test_get_db_name_with_query_params() -> None:
    """get_db_name() should strip query parameters from the URL."""
    with (
        patch("py_clickhouse_migrator.migrator.Client.from_url", return_value=MagicMock()),
        patch.object(Migrator, "check_migrations_table"),
    ):
        migrator = Migrator(database_url="clickhouse://default@localhost:9000/mydb?secure=1&timeout=30")
    assert migrator.get_db_name() == "mydb"


def test_show_migrations_no_applied(migrator: Migrator, migrator_init: None) -> None:
    """show_migrations with zero applied migrations should show 'none'."""
    output, warning = migrator.show_migrations()
    plain = click.unstyle(output)
    assert "none" in plain
    assert "Applied: 0" in plain
    assert warning == ""


def test_show_migrations_with_pending(migrator: Migrator, migrator_init: None) -> None:
    """show_migrations should list pending migrations."""
    create_test_migration(
        name="test_pending",
        up="CREATE TABLE IF NOT EXISTS test_pending (id Int32) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_pending",
    )

    result = migrator.show_migrations()
    plain = click.unstyle(result.output)
    lines = plain.splitlines()

    pending_items = [line for line in lines if "[ ]" in line]
    assert len(pending_items) == 1
    assert "test_pending" in pending_items[0]

    assert any(line.strip() == "Applied: 0 | Pending: 1" for line in lines)
    assert result.warning == ""


def test_get_migrations_for_apply_invalid_sql_file(migrator: Migrator, migrator_init: None) -> None:
    """A malformed SQL migration file should raise InvalidMigrationError."""
    filename = "20990101000000_bad.sql"
    filepath = f"{DEFAULT_MIGRATIONS_DIR}/{filename}"
    os.makedirs(DEFAULT_MIGRATIONS_DIR, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("-- migrator:up\nSELECT 1;\n")

    with pytest.raises(InvalidMigrationError, match="Must contain exactly one"):
        migrator.get_migrations_for_apply()


def test_get_unapplied_migration_names_skips_non_sql_files(migrator: Migrator, migrator_init: None) -> None:
    create_test_migration(name="real", up="SELECT 1", rollback="")
    with open(f"{DEFAULT_MIGRATIONS_DIR}/notes.txt", "w", encoding="utf-8") as f:
        f.write("ignore me")
    with open(f"{DEFAULT_MIGRATIONS_DIR}/legacy.py", "w", encoding="utf-8") as f:
        f.write("print('legacy')")

    unapplied = migrator.get_unapplied_migration_names()

    assert len(unapplied) == 1
    assert unapplied[0].endswith(".sql")


@patch.object(Migrator, "check_migrations_table")
def test_migrator_cluster_param_from_init(_mock: object, test_db: str) -> None:
    migrator = Migrator(database_url=test_db, cluster="my_cluster")
    assert migrator.cluster == "my_cluster"


def test_send_receive_timeout_passed_to_client(ch_client: Client, test_db: str) -> None:
    ch_client.execute("DROP TABLE IF EXISTS db_migrations")
    migrator = Migrator(database_url=test_db, send_receive_timeout=900)
    assert migrator.ch_client.connection.send_receive_timeout == 900
    ch_client.execute("DROP TABLE IF EXISTS db_migrations")


def test_send_receive_timeout_default(ch_client: Client, test_db: str) -> None:
    ch_client.execute("DROP TABLE IF EXISTS db_migrations")
    migrator = Migrator(database_url=test_db)
    assert migrator.ch_client.connection.send_receive_timeout == 600
    ch_client.execute("DROP TABLE IF EXISTS db_migrations")


def test_migrator_cluster_param_empty_by_default(ch_client: Client, test_db: str) -> None:
    ch_client.execute("DROP TABLE IF EXISTS db_migrations")
    migrator = Migrator(database_url=test_db)
    assert migrator.cluster == ""
    ch_client.execute("DROP TABLE IF EXISTS db_migrations")
