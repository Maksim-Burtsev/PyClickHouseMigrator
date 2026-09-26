import shutil
from collections.abc import Iterator, Sequence
from pathlib import Path
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
from py_clickhouse_migrator.migration import Migration, MigrationKind
from py_clickhouse_migrator.migrator import (
    DEFAULT_MIGRATIONS_DIR,
    Migrator,
    create_migration_file,
    create_migrations_dir,
)
from tests.migration_files import MIGRATION_FILENAME_REGEX, Statements, create_test_migration
from tests.queries import count_migration_rows, drop_tables, table_exists

MigrationSpec = tuple[str, Statements, Statements]
MigrationView = tuple[str, str, str, list[str], list[str]]
DatabaseState = dict[str, list[str]]

MIGRATIONS_ROOT = Path("db")
MIGRATIONS_DIR = Path(DEFAULT_MIGRATIONS_DIR)
MIGRATIONS_TABLE = "db_migrations"
OFFLINE_DATABASE_URL = "clickhouse://default@localhost:9000/test"
MIGRATOR_LOGGER = "py_clickhouse_migrator"

TIMESTAMP_DIGITS = 14
CUSTOM_TIMEOUT_SECONDS = 900
DEFAULT_TIMEOUT_SECONDS = 600
SHOWN_APPLIED_BY_DEFAULT = 5

EXPECTED_MIGRATIONS_TABLE_SCHEMA = (
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

TEST_TABLE = "test_table"
FIRST_TABLE = "test_table_1"
SECOND_TABLE = "test_table_2"
THIRD_TABLE = "test_table_3"
FIXTURE_TABLES = (FIRST_TABLE, SECOND_TABLE, THIRD_TABLE)
INT_ID_TABLE = "test_table_int_id"
STRING_ID_TABLE = "test_table_str_id"
NUMBERED_TABLES = tuple(f"t_{index}" for index in range(7))

CREATE_TEST_TABLE = "CREATE TABLE IF NOT EXISTS test_table (id Integer) Engine=MergeTree() ORDER BY id;"
DROP_TEST_TABLE = "DROP TABLE IF EXISTS test_table"
CREATE_FIRST_TABLE = "CREATE TABLE IF NOT EXISTS test_table_1 (id Integer) Engine=MergeTree() ORDER BY id;"
CREATE_SECOND_TABLE = "CREATE TABLE IF NOT EXISTS test_table_2 (id String) Engine=MergeTree() ORDER BY id;"
CREATE_THIRD_TABLE = "CREATE TABLE IF NOT EXISTS test_table_3 (id String) Engine=MergeTree() ORDER BY id;"
DROP_FIRST_TABLE = "DROP TABLE IF EXISTS test_table_1"
DROP_SECOND_TABLE = "DROP TABLE IF EXISTS test_table_2"
DROP_THIRD_TABLE = "DROP TABLE IF EXISTS test_table_3"
TABLE_MIGRATIONS: tuple[MigrationSpec, ...] = (
    ("test_1", CREATE_FIRST_TABLE, DROP_FIRST_TABLE),
    ("test_2", CREATE_SECOND_TABLE, DROP_SECOND_TABLE),
    ("test_3", CREATE_THIRD_TABLE, DROP_THIRD_TABLE),
)

INT_ID = ("id", "Int32")
STRING_ID = ("id", "String")

SELECT_RECORDED_NAMES = "SELECT name FROM db_migrations ORDER BY dt"
SELECT_RECORDED_KINDS = "SELECT name, toString(kind) FROM db_migrations ORDER BY dt"
SELECT_FIRST_COLUMNS = (
    "SELECT table, name, type FROM system.columns"
    " WHERE database = currentDatabase() AND table IN %(tables)s AND position = 1"
)
SELECT_TEST_TABLE_IDS = "SELECT id FROM test_table ORDER BY id"
INSERT_RECORDED_MIGRATION = "INSERT INTO db_migrations (name, up, rollback) VALUES"


def _recorded_names(ch_client: Client) -> list[str]:
    return [row[0] for row in ch_client.execute(SELECT_RECORDED_NAMES)]


def _existing_tables(ch_client: Client, *tables: str) -> list[str]:
    return [table for table in tables if table_exists(ch_client, table)]


def _database_state(ch_client: Client, *tables: str) -> DatabaseState:
    return {"recorded": sorted(_recorded_names(ch_client)), "tables": _existing_tables(ch_client, *tables)}


def _expected_state(recorded: Sequence[str], tables: Sequence[str]) -> DatabaseState:
    return {"recorded": sorted(recorded), "tables": list(tables)}


def _first_columns(ch_client: Client, *tables: str) -> dict[str, tuple[str, str]]:
    rows = ch_client.execute(SELECT_FIRST_COLUMNS, {"tables": tables})
    return {table: (column, column_type) for table, column, column_type in rows}


def _migration_files() -> list[str]:
    return sorted(path.name for path in MIGRATIONS_DIR.iterdir())


def _create_migration(spec: MigrationSpec) -> str:
    name, up, rollback = spec
    return create_test_migration(name=name, up=up, rollback=rollback)


def _create_migrations(specs: Sequence[MigrationSpec]) -> list[str]:
    return [_create_migration(spec) for spec in specs]


def _view(migration: Migration) -> MigrationView:
    return (
        migration.name,
        migration.up,
        migration.rollback,
        migration.up_statements,
        migration.rollback_statements,
    )


def _views(migrations: list[Migration]) -> list[MigrationView]:
    return [_view(migration) for migration in migrations]


def _expected_view(filename: str, up: str, rollback: str) -> MigrationView:
    return (filename, f"-- @stmt\n{up}", f"-- @stmt\n{rollback}", [up], [rollback])


def _offline_migrator(database_url: str, migrations_dir: str = DEFAULT_MIGRATIONS_DIR) -> Migrator:
    with (
        patch("py_clickhouse_migrator.migrator.Client.from_url", return_value=MagicMock()),
        patch.object(Migrator, "health_check"),
        patch.object(Migrator, "check_migrations_table"),
    ):
        return Migrator(database_url=database_url, migrations_dir=migrations_dir)


@pytest.fixture
def no_migrations_table(ch_client: Client) -> Iterator[None]:
    """Start without ``db_migrations`` so the Migrator creates it, and drop it again after the test."""
    drop_tables(ch_client, MIGRATIONS_TABLE)
    yield
    drop_tables(ch_client, MIGRATIONS_TABLE)


@pytest.fixture
def numbered_migrations(migrator: Migrator, migrator_init: None, ch_client: Client) -> Iterator[None]:
    """Apply seven one-table migrations, more than ``show`` lists by default."""
    for index, table in enumerate(NUMBERED_TABLES):
        create_test_migration(
            name=f"table_{index}",
            up=f"CREATE TABLE IF NOT EXISTS {table} (id Int32) Engine=MergeTree() ORDER BY id;",
            rollback=f"DROP TABLE IF EXISTS {table}",
        )
    migrator.up()
    yield
    drop_tables(ch_client, *NUMBERED_TABLES)


def test_db_migrations_table_creation(ch_client: Client, test_db: str, no_migrations_table: None) -> None:
    """Creating a Migrator creates an empty ``db_migrations`` with the schema existing databases already have."""
    assert not table_exists(ch_client, MIGRATIONS_TABLE)

    Migrator(database_url=test_db)

    assert table_exists(ch_client, MIGRATIONS_TABLE)
    assert ch_client.execute("SHOW CREATE TABLE db_migrations")[0][0] == EXPECTED_MIGRATIONS_TABLE_SCHEMA
    assert count_migration_rows(ch_client) == 0


def test_init_base() -> None:
    """``init`` creates the default migrations directory when it is missing."""
    assert not MIGRATIONS_DIR.exists()

    create_migrations_dir()

    assert MIGRATIONS_DIR.exists()


def test_nonexistent_database_raises_error() -> None:
    """Migrator should fail with clear error if database doesn't exist."""
    bad_url = "clickhouse://default@localhost:19000/this_db_does_not_exist"
    with pytest.raises(DatabaseNotFoundError, match="does not exist"):
        Migrator(database_url=bad_url)


def test_init_with_invalid_database_url(test_db: str) -> None:
    """An unreachable ClickHouse host is reported as an unhealthy server, not as a raw driver error."""
    unreachable_url = test_db.replace("localhost", "some_domain")
    with pytest.raises(ClickHouseServerIsNotHealthyError):
        Migrator(unreachable_url)


def test_init_keeps_existing_migration_files() -> None:
    """Running ``init`` again must not wipe migration files that are already in the directory."""
    MIGRATIONS_DIR.mkdir(parents=True, exist_ok=True)
    existing_migration = MIGRATIONS_DIR / "test_migration.sql"
    existing_migration.touch()
    assert MIGRATIONS_DIR.exists()

    create_migrations_dir()

    assert MIGRATIONS_DIR.exists()
    assert existing_migration.exists()


def test_create_new_migration(migrator_init: None) -> None:
    """``new`` adds exactly one file whose name ends with the requested migration name."""
    assert not _migration_files()

    create_migration_file(name="first_migration")
    migration_filenames = _migration_files()

    assert len(migration_filenames) == 1
    assert "_first_migration.sql" in migration_filenames[0]


def test_create_new_migration_without_init() -> None:
    """``new`` without a migrations directory fails with a hint to run ``init`` instead of crashing."""
    shutil.rmtree(MIGRATIONS_ROOT, ignore_errors=True)
    with pytest.raises(MigrationDirectoryNotFoundError):
        create_migration_file(name="test")


def test_migration_is_baseline_property() -> None:
    """A migration of kind ``baseline`` reports itself as a baseline so rollback can skip it."""
    migration = Migration(name="baseline.sql", up="", rollback="", kind=MigrationKind.BASELINE)

    assert migration.is_baseline is True


def test_apply_migration_one_query(migrator: Migrator, ch_client: Client) -> None:
    """A single statement is executed against ClickHouse as written."""
    drop_tables(ch_client, TEST_TABLE)
    assert not table_exists(ch_client, TEST_TABLE)

    migrator.apply_migration([CREATE_TEST_TABLE])

    assert _first_columns(ch_client, TEST_TABLE) == {TEST_TABLE: INT_ID}

    drop_tables(ch_client, TEST_TABLE)


def test_apply_migration_multiquery(migrator: Migrator, ch_client: Client) -> None:
    """Several statements run in order, so later inserts see the tables that earlier statements created."""
    assert not _existing_tables(ch_client, INT_ID_TABLE, STRING_ID_TABLE)

    migrator.apply_migration([
        "CREATE TABLE IF NOT EXISTS test_table_int_id (id Integer) Engine=MergeTree() ORDER BY id;",
        "CREATE TABLE IF NOT EXISTS test_table_str_id (id String) Engine=MergeTree() ORDER BY id;",
        "INSERT INTO TABLE test_table_int_id VALUES (1), (2), (3);",
        (
            "INSERT INTO TABLE test_table_str_id VALUES ('17afaed9-ef50-4a2e-a91d-af7cc8344033'),"
            " ('744aa7d7-568b-48f2-80a1-ef0aaf18fc1b'), ('22405e14-e82a-4ab7-a502-05b40bbbd791')"
        ),
    ])

    assert _first_columns(ch_client, INT_ID_TABLE, STRING_ID_TABLE) == {
        INT_ID_TABLE: INT_ID,
        STRING_ID_TABLE: STRING_ID,
    }
    assert ch_client.execute("SELECT id FROM test_table_int_id") == [(1,), (2,), (3,)]
    assert ch_client.execute("SELECT id FROM test_table_str_id") == [
        ("17afaed9-ef50-4a2e-a91d-af7cc8344033",),
        ("22405e14-e82a-4ab7-a502-05b40bbbd791",),
        ("744aa7d7-568b-48f2-80a1-ef0aaf18fc1b",),
    ]

    drop_tables(ch_client, INT_ID_TABLE, STRING_ID_TABLE)


def test_up_rejects_empty_template(migrator: Migrator, migrator_init: None) -> None:
    """An untouched ``new`` template has no SQL, and ``up`` must refuse it rather than record a no-op."""
    filepath = create_migration_file(name="test")
    assert Path(filepath).exists()

    with pytest.raises(
        InvalidMigrationError,
        match=r"Migration .+: Must contain at least one non-empty '-- @stmt' block",
    ):
        migrator.up()


def test_get_all_migrations_for_apply(migrator: Migrator, migrator_init: None) -> None:
    """Every pending file loads in file order with its raw sections and parsed statement blocks."""
    first, second = _create_migrations(TABLE_MIGRATIONS[:2])

    assert _views(migrator.get_migrations_for_apply()) == [
        _expected_view(first, CREATE_FIRST_TABLE, DROP_FIRST_TABLE),
        _expected_view(second, CREATE_SECOND_TABLE, DROP_SECOND_TABLE),
    ]


def test_get_few_migrations_for_apply_with_number(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """``number`` limits loading to the oldest pending files and leaves the rest pending."""
    first, second, third = _create_migrations(TABLE_MIGRATIONS)

    assert _views(migrator.get_migrations_for_apply(number=2)) == [
        _expected_view(first, CREATE_FIRST_TABLE, DROP_FIRST_TABLE),
        _expected_view(second, CREATE_SECOND_TABLE, DROP_SECOND_TABLE),
    ]
    assert (MIGRATIONS_DIR / third).exists()
    assert count_migration_rows(ch_client, third) == 0
    assert len(migrator.get_migrations_for_apply()) == len(TABLE_MIGRATIONS)


def test_get_migrations_for_rollback(
    migrator: Migrator,
    test_tables_from_migration: list[str],
    ch_client: Client,
) -> None:
    """Rollback candidates are read from ``db_migrations`` newest first, and reading them deletes nothing."""
    first, second, third = test_tables_from_migration
    newest = _expected_view(third, CREATE_THIRD_TABLE, DROP_THIRD_TABLE)
    assert count_migration_rows(ch_client) == len(test_tables_from_migration)

    assert _views(migrator.get_migrations_for_rollback(number=2)) == [
        newest,
        _expected_view(second, CREATE_SECOND_TABLE, DROP_SECOND_TABLE),
    ]
    assert count_migration_rows(ch_client, first) == 1
    assert _views(migrator.get_migrations_for_rollback()) == [newest]


def test_baseline_records_files_without_parsing(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """Baseline records every file name in sorted order without reading its SQL, so any legacy file works."""
    for filename in ("20990101000002_second.sql", "20990101000001_first.sql"):
        (MIGRATIONS_DIR / filename).write_text("this is not a parsed migration file", encoding="utf-8")

    with patch("py_clickhouse_migrator.migration.load_migration_sections") as load_sections:
        baselined = migrator.baseline()
        load_sections.assert_not_called()

    assert baselined == ["20990101000001_first.sql", "20990101000002_second.sql"]
    assert ch_client.execute("SELECT name, toString(kind), up, rollback, checksum FROM db_migrations ORDER BY dt") == [
        ("20990101000001_first.sql", "baseline", "", "", ""),
        ("20990101000002_second.sql", "baseline", "", "", ""),
    ]


def test_baseline_without_files_records_nothing(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """Baseline of an empty migrations directory succeeds and writes no rows."""
    baselined = migrator.baseline()

    assert baselined == []
    assert count_migration_rows(ch_client) == 0


def test_baseline_requires_migrations_dir() -> None:
    """A missing migrations directory is reported with a hint to run ``init``, not as a raw OS error."""
    migrator = _offline_migrator(OFFLINE_DATABASE_URL, migrations_dir="./missing_migrations")

    with (
        patch.object(migrator, "get_applied_migrations_names", return_value=[]),
        pytest.raises(MigrationDirectoryNotFoundError, match="Run 'migrator init' first"),
    ):
        migrator.baseline()


def test_baseline_requires_empty_db_migrations(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """Baseline refuses to run once migrations are applied, and leaves the existing rows untouched."""
    filename = create_test_migration(
        name="baseline_guard",
        up="CREATE TABLE IF NOT EXISTS baseline_guard (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS baseline_guard",
    )
    migrator.up()

    with pytest.raises(BaselineError, match=r"Baseline requires an empty db_migrations table\."):
        migrator.baseline()

    assert ch_client.execute(SELECT_RECORDED_KINDS) == [(filename, "migration")]

    drop_tables(ch_client, "baseline_guard")


def _baseline_old_then_apply_new(migrator: Migrator) -> tuple[str, str]:
    baselined = create_test_migration(
        name="old_schema",
        up="CREATE TABLE IF NOT EXISTS old_schema (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS old_schema",
    )
    migrator.baseline()
    applied = create_test_migration(
        name="new_schema",
        up="CREATE TABLE IF NOT EXISTS new_schema (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS new_schema",
    )
    migrator.up()
    return baselined, applied


def test_up_after_baseline_applies_only_new_files(
    migrator: Migrator,
    migrator_init: None,
    ch_client: Client,
) -> None:
    """After baseline, ``up`` runs only files added later and never executes the baselined ones."""
    baselined, applied = _baseline_old_then_apply_new(migrator)

    assert dict(ch_client.execute("SELECT name, toString(kind) FROM db_migrations")) == {
        baselined: "baseline",
        applied: "migration",
    }
    assert _existing_tables(ch_client, "old_schema", "new_schema") == ["new_schema"]

    drop_tables(ch_client, "new_schema")


def test_rollback_skips_baselined_migrations(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """Rollback reverts only migrations it applied; baseline rows have no down SQL and must stay."""
    baselined, applied = _baseline_old_then_apply_new(migrator)

    assert [migration.name for migration in migrator.get_migrations_for_rollback(number=10)] == [applied]

    migrator.rollback(number=10)

    assert not table_exists(ch_client, "new_schema")
    assert ch_client.execute(SELECT_RECORDED_KINDS) == [(baselined, "baseline")]


def test_create_migration_file_default_name(migrator_init: None) -> None:
    """A migration created without a name still gets a valid timestamped file name."""
    filename = Path(create_migration_file()).name

    assert MIGRATION_FILENAME_REGEX.match(filename)


def test_create_migration_file_with_name(migrator_init: None) -> None:
    """The requested name becomes the suffix of a valid migration file name."""
    filename = Path(create_migration_file(name="test_migration")).name

    assert "_test_migration.sql" in filename
    assert MIGRATION_FILENAME_REGEX.match(filename)


def test_get_applied_migrations_names(
    migrator: Migrator,
    test_tables_from_migration: list[str],
    ch_client: Client,
) -> None:
    """Applied names are exactly the rows of ``db_migrations``, oldest first."""
    migration_names = migrator.get_applied_migrations_names()
    recorded_names = _recorded_names(ch_client)

    assert len(migration_names) == len(test_tables_from_migration)
    assert len(recorded_names) == len(test_tables_from_migration)
    assert migration_names == recorded_names


@pytest.mark.parametrize(
    ("migrations", "expected_columns"),
    [
        pytest.param(
            (("test", CREATE_TEST_TABLE, DROP_TEST_TABLE),),
            {TEST_TABLE: INT_ID},
            id="one_query",
        ),
        pytest.param(
            (("test_multiquery", (CREATE_FIRST_TABLE, CREATE_SECOND_TABLE), (DROP_FIRST_TABLE, DROP_SECOND_TABLE)),),
            {FIRST_TABLE: INT_ID, SECOND_TABLE: STRING_ID},
            id="multiquery",
        ),
        pytest.param(
            (
                (
                    "test_multiquery",
                    (f"{CREATE_FIRST_TABLE}\n\n", f"{CREATE_SECOND_TABLE}   \n\n"),
                    (DROP_FIRST_TABLE, f"{DROP_SECOND_TABLE} \n\n\n"),
                ),
            ),
            {FIRST_TABLE: INT_ID, SECOND_TABLE: STRING_ID},
            id="multiquery_with_line_breaks",
        ),
        pytest.param(
            TABLE_MIGRATIONS[:2],
            {FIRST_TABLE: INT_ID, SECOND_TABLE: STRING_ID},
            id="multiple_files",
        ),
    ],
)
def test_up_applies_pending_migrations(
    migrator: Migrator,
    migrator_init: None,
    ch_client: Client,
    migrations: tuple[MigrationSpec, ...],
    expected_columns: dict[str, tuple[str, str]],
) -> None:
    """``up`` runs every statement block of every pending file and records each file exactly once."""
    tables = tuple(expected_columns)
    assert _database_state(ch_client, *tables) == _expected_state(recorded=[], tables=[])

    filenames = sorted(_create_migrations(migrations))
    assert _migration_files() == filenames

    migrator.up()

    assert sorted(_recorded_names(ch_client)) == sorted(migrator.get_applied_migrations_names()) == filenames
    assert _first_columns(ch_client, *tables) == expected_columns

    drop_tables(ch_client, *tables)


@pytest.mark.parametrize("rolled_back", [1, 2])
def test_rollback_reverts_newest_migrations(
    migrator: Migrator,
    test_tables_from_migration: list[str],
    ch_client: Client,
    rolled_back: int,
) -> None:
    """Rollback reverts and unrecords only the ``rolled_back`` newest migrations and keeps the older ones."""
    kept = len(test_tables_from_migration) - rolled_back
    assert sorted(migrator.get_applied_migrations_names()) == sorted(test_tables_from_migration)
    assert _database_state(ch_client, *FIXTURE_TABLES) == _expected_state(test_tables_from_migration, FIXTURE_TABLES)

    migrator.rollback(number=rolled_back)

    assert sorted(migrator.get_applied_migrations_names()) == sorted(test_tables_from_migration)[:kept]
    assert _database_state(ch_client, *FIXTURE_TABLES) == _expected_state(
        test_tables_from_migration[:kept],
        FIXTURE_TABLES[:kept],
    )


def test_rollback_multiquery_migration(migrator: Migrator, test_table_from_migration: str, ch_client: Client) -> None:
    """Rollback runs every statement block of the down section in order, then unrecords the migration."""
    filename = create_test_migration(
        name="test_multiquery",
        up=CREATE_FIRST_TABLE,
        rollback=[DROP_FIRST_TABLE, "INSERT INTO test_table(id) VALUES (1),(2),(3);"],
    )
    migrator.up()
    assert (MIGRATIONS_DIR / filename).exists()
    assert _database_state(ch_client, TEST_TABLE, FIRST_TABLE) == _expected_state(
        [test_table_from_migration, filename],
        [TEST_TABLE, FIRST_TABLE],
    )
    assert not ch_client.execute(SELECT_TEST_TABLE_IDS)

    migrator.rollback()

    assert _database_state(ch_client, TEST_TABLE, FIRST_TABLE) == _expected_state(
        [test_table_from_migration],
        [TEST_TABLE],
    )
    assert ch_client.execute(SELECT_TEST_TABLE_IDS) == [(1,), (2,), (3,)]


def test_save_applied_migration(migrator: Migrator, ch_client: Client, migrator_init: None) -> None:
    """An applied migration is stored with its name, both SQL sections, and its checksum."""
    assert count_migration_rows(ch_client) == 0

    migrator.save_applied_migration(
        name="test",
        up=CREATE_TEST_TABLE,
        rollback="DROP TABLE IF EXISTS test_table;",
        checksum="abc123",
    )

    assert ch_client.execute("SELECT name, up, rollback, checksum FROM db_migrations") == [
        ("test", CREATE_TEST_TABLE, "DROP TABLE IF EXISTS test_table;", "abc123"),
    ]

    ch_client.execute("DELETE FROM db_migrations WHERE name='test'")


def test_delete_migration(migrator: Migrator, ch_client: Client, migrator_init: None) -> None:
    """Deleting a migration removes its row, and the deletion is visible to the next read."""
    assert not _recorded_names(ch_client)
    ch_client.execute(INSERT_RECORDED_MIGRATION, [("test.sql", CREATE_TEST_TABLE, DROP_TEST_TABLE)])
    assert _recorded_names(ch_client) == ["test.sql"]

    migrator.delete_migration("test.sql")

    assert not _recorded_names(ch_client)


def test_apply_invalid_migration(migrator: Migrator, ch_client: Client) -> None:
    """A statement that ClickHouse rejects surfaces as ``InvalidMigrationError``."""
    assert not table_exists(ch_client, TEST_TABLE)

    with pytest.raises(InvalidMigrationError):
        migrator.apply_migration(["ALTER TABLE test_table ADD COLUMN IF NOT EXISTS new_column Integer;"])


def test_missing_database_url_error() -> None:
    """A Migrator without a database URL fails fast with an error that names the missing setting."""
    with pytest.raises(MissingDatabaseUrlError):
        Migrator()


def test_up_dry_run_does_not_apply(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """dry_run=True should not create tables or save migrations to db_migrations."""
    drop_tables(ch_client, TEST_TABLE)
    assert not table_exists(ch_client, TEST_TABLE)
    create_test_migration(name="test_dry", up=CREATE_TEST_TABLE, rollback=DROP_TEST_TABLE)

    migrator.up(dry_run=True)

    assert not table_exists(ch_client, TEST_TABLE)
    assert count_migration_rows(ch_client) == 0
    assert len(migrator.get_unapplied_migration_names()) == 1


def test_up_dry_run_with_number(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """dry_run with number should show only N migrations without applying."""
    _create_migrations(TABLE_MIGRATIONS[:2])

    migrator.up(n=1, dry_run=True)

    assert not _existing_tables(ch_client, FIRST_TABLE, SECOND_TABLE)
    assert count_migration_rows(ch_client) == 0
    assert len(migrator.get_unapplied_migration_names()) == 2


def test_up_dry_run_separates_migrations(
    migrator: Migrator,
    migrator_init: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Dry-run output puts a blank line between one migration's SQL and the next migration's header."""
    first_up = "CREATE TABLE IF NOT EXISTS dry_run_a (id Integer) Engine=MergeTree() ORDER BY id;"
    first_filename = create_test_migration(name="dry_run_a", up=first_up, rollback="DROP TABLE IF EXISTS dry_run_a")
    second_filename = create_test_migration(
        name="dry_run_b",
        up="CREATE TABLE IF NOT EXISTS dry_run_b (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS dry_run_b",
    )

    migrator.up(dry_run=True)

    printed = click.unstyle(capsys.readouterr().out)
    assert f"{first_up}\n\n-- {second_filename} (up)" in printed
    assert f"-- {first_filename} (up)" in printed


def test_rollback_dry_run_does_not_rollback(
    migrator: Migrator,
    test_table_from_migration: str,
    ch_client: Client,
) -> None:
    """dry_run=True should not drop tables or delete from db_migrations."""
    applied_state = _expected_state([test_table_from_migration], [TEST_TABLE])
    assert _database_state(ch_client, TEST_TABLE) == applied_state

    migrator.rollback(dry_run=True)

    assert _database_state(ch_client, TEST_TABLE) == applied_state


def test_rollback_dry_run_multiple(
    migrator: Migrator,
    test_tables_from_migration: list[str],
    ch_client: Client,
) -> None:
    """dry_run rollback of multiple migrations should leave everything intact."""
    applied_state = _expected_state(test_tables_from_migration, FIXTURE_TABLES)
    assert _database_state(ch_client, *FIXTURE_TABLES) == applied_state

    migrator.rollback(number=2, dry_run=True)

    assert _database_state(ch_client, *FIXTURE_TABLES) == applied_state


def test_validation_wraps_clickhouse_error(migrator: Migrator) -> None:
    """A statement rejected by ``EXPLAIN AST`` raises ``InvalidStatementError`` carrying the server's message."""
    with pytest.raises(InvalidStatementError, match="ClickHouse error"):
        migrator.validate_statements(["SELECT FROM system.tables"])


def test_up_validation_failure_runs_nothing(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """If any pending statement fails validation, ``up`` must not execute or record anything."""
    filename = create_test_migration(
        name="validation_fail_up",
        up="CREATE TABLE IF NOT EXISTS validation_fail_up (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS validation_fail_up",
    )
    apply_mock = MagicMock()
    save_mock = MagicMock()

    with (
        patch.object(migrator, "validate_statements", side_effect=InvalidStatementError("bad statement")),
        patch.object(migrator, "apply_migration", apply_mock),
        patch.object(migrator, "save_applied_migration", save_mock),
        pytest.raises(InvalidMigrationError, match=rf"Validation failed for migration {filename}"),
    ):
        migrator.up()

    apply_mock.assert_not_called()
    save_mock.assert_not_called()
    assert _database_state(ch_client, "validation_fail_up") == _expected_state(recorded=[], tables=[])


def test_rollback_validation_failure_runs_nothing(
    migrator: Migrator,
    migrator_init: None,
    ch_client: Client,
) -> None:
    """If any rollback statement fails validation, rollback must not execute anything or unrecord the migration."""
    filename = create_test_migration(
        name="validation_fail_rollback",
        up="CREATE TABLE IF NOT EXISTS validation_fail_rollback (id Integer) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS validation_fail_rollback",
    )
    migrator.up()
    apply_mock = MagicMock()
    delete_mock = MagicMock()

    with (
        patch.object(migrator, "validate_statements", side_effect=InvalidStatementError("bad statement")),
        patch.object(migrator, "apply_migration", apply_mock),
        patch.object(migrator, "delete_migration", delete_mock),
        pytest.raises(InvalidMigrationError, match=rf"Validation failed for migration {filename}"),
    ):
        migrator.rollback()

    apply_mock.assert_not_called()
    delete_mock.assert_not_called()
    assert _database_state(ch_client, "validation_fail_rollback") == _expected_state(
        [filename],
        ["validation_fail_rollback"],
    )

    drop_tables(ch_client, "validation_fail_rollback")


def test_new_migration_filename_format(migrator_init: None) -> None:
    """Filename should be 14 digits (YYYYMMDDHHmmSS) without microseconds."""
    filename = Path(create_migration_file(name="test")).name
    timestamp = filename.split("_", maxsplit=1)[0]

    assert MIGRATION_FILENAME_REGEX.match(filename)
    assert len(timestamp) == TIMESTAMP_DIGITS
    assert timestamp.isdigit()


def test_new_migration_filename_with_name(migrator_init: None) -> None:
    """A descriptive name is kept verbatim as the file name suffix."""
    filename = Path(create_migration_file(name="create_users")).name

    assert "_create_users.sql" in filename
    assert MIGRATION_FILENAME_REGEX.match(filename)


def test_new_migration_without_name_warns(migrator_init: None, caplog: pytest.LogCaptureFixture) -> None:
    """Creating an unnamed migration still works but warns that a name is recommended."""
    with caplog.at_level("WARNING", logger=MIGRATOR_LOGGER):
        filepath = create_migration_file()

    assert "Migration name is recommended" in caplog.text
    assert MIGRATION_FILENAME_REGEX.match(Path(filepath).name)


def test_show_migrations_default_limits_applied(migrator: Migrator, numbered_migrations: None) -> None:
    """With >5 applied migrations, show only last 5 + '... and N more'."""
    output, warning = migrator.show_migrations()
    plain = click.unstyle(output)

    assert plain.count("[X]") == SHOWN_APPLIED_BY_DEFAULT
    assert "... and 2 more applied" in plain
    assert "(HEAD)" in plain
    assert "Applied: 7 | Pending: 0" in plain
    assert not warning


def test_show_migrations_all_flag(migrator: Migrator, numbered_migrations: None) -> None:
    """show_all=True should show every applied migration."""
    output, warning = migrator.show_migrations(show_all=True)
    plain = click.unstyle(output)

    assert plain.count("[X]") == len(NUMBERED_TABLES)
    assert "... and" not in plain
    assert "(HEAD)" in plain
    assert "Applied: 7" in plain
    assert not warning


def test_get_db_name_with_query_params() -> None:
    """get_db_name() should strip query parameters from the URL."""
    migrator = _offline_migrator("clickhouse://default@localhost:9000/mydb?secure=1&timeout=30")

    assert migrator.get_db_name() == "mydb"


def test_show_migrations_no_applied(migrator: Migrator, migrator_init: None) -> None:
    """show_migrations with zero applied migrations should show 'none'."""
    output, warning = migrator.show_migrations()
    plain = click.unstyle(output)

    assert "none" in plain
    assert "Applied: 0" in plain
    assert not warning


def test_show_migrations_with_pending(migrator: Migrator, migrator_init: None) -> None:
    """show_migrations should list pending migrations."""
    create_test_migration(
        name="test_pending",
        up="CREATE TABLE IF NOT EXISTS test_pending (id Int32) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_pending",
    )

    status = migrator.show_migrations()
    lines = click.unstyle(status.output).splitlines()
    pending_items = [line for line in lines if "[ ]" in line]

    assert len(pending_items) == 1
    assert "test_pending" in pending_items[0]
    assert any(line.strip() == "Applied: 0 | Pending: 1" for line in lines)
    assert not status.warning


def test_malformed_migration_file_is_rejected(migrator: Migrator, migrator_init: None) -> None:
    """A malformed SQL migration file should raise InvalidMigrationError."""
    MIGRATIONS_DIR.mkdir(parents=True, exist_ok=True)
    (MIGRATIONS_DIR / "20990101000000_bad.sql").write_text("-- migrator:up\nSELECT 1;\n", encoding="utf-8")

    with pytest.raises(InvalidMigrationError, match="Must contain exactly one"):
        migrator.get_migrations_for_apply()


def test_unapplied_names_skip_non_sql_files(migrator: Migrator, migrator_init: None) -> None:
    """Only ``.sql`` files count as migrations; notes and legacy scripts in the directory are ignored."""
    create_test_migration(name="real", up="SELECT 1", rollback="")
    (MIGRATIONS_DIR / "notes.txt").write_text("ignore me", encoding="utf-8")
    (MIGRATIONS_DIR / "legacy.py").write_text("print('legacy')", encoding="utf-8")

    unapplied = migrator.get_unapplied_migration_names()

    assert len(unapplied) == 1
    assert unapplied[0].endswith(".sql")


def test_migrator_cluster_param_from_init(test_db: str) -> None:
    """The cluster passed to the constructor is kept for ``ON CLUSTER`` DDL."""
    with patch.object(Migrator, "check_migrations_table"):
        migrator = Migrator(database_url=test_db, cluster="my_cluster")

    assert migrator.cluster == "my_cluster"


def test_send_receive_timeout_passed_to_client(test_db: str, no_migrations_table: None) -> None:
    """A custom send/receive timeout reaches the ClickHouse connection, so long DDL is not cut off."""
    migrator = Migrator(database_url=test_db, send_receive_timeout=CUSTOM_TIMEOUT_SECONDS)

    assert migrator.ch_client.connection.send_receive_timeout == CUSTOM_TIMEOUT_SECONDS


def test_send_receive_timeout_default(test_db: str, no_migrations_table: None) -> None:
    """Without an explicit timeout the connection uses the documented 600-second default."""
    migrator = Migrator(database_url=test_db)

    assert migrator.ch_client.connection.send_receive_timeout == DEFAULT_TIMEOUT_SECONDS


def test_migrator_cluster_param_empty_by_default(test_db: str, no_migrations_table: None) -> None:
    """Without a cluster the Migrator runs in single-node mode."""
    migrator = Migrator(database_url=test_db)

    assert not migrator.cluster
