"""Unit tests for CLI commands. All ClickHouse interactions are mocked."""

import datetime as dt
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock, patch

import click
import pytest
from click.testing import CliRunner, Result

from py_clickhouse_migrator.cli import main
from py_clickhouse_migrator.errors import (
    BaselineError,
    ChecksumMismatchError,
    ClickHouseServerIsNotHealthyError,
    DatabaseNotFoundError,
    InvalidMigrationError,
    MigrationDirectoryNotFoundError,
    MissingDatabaseUrlError,
)
from py_clickhouse_migrator.lock import DEFAULT_LOCK_TTL, LockError, LockInfo
from py_clickhouse_migrator.migration import Migration
from py_clickhouse_migrator.migrator import (
    DEFAULT_MIGRATIONS_DIR,
    ChecksumMismatch,
    Migrator,
    ShowMigrationsResult,
)

FAKE_URL = "clickhouse://default@localhost:9000/test"
FAKE_DB = "test"
CLI_OPTIONS_MODULE = "py_clickhouse_migrator.cli.options"
EXIT_OK = 0
EXIT_ERROR = 1
STMT_MARKER = "-- @stmt"
STMT_BLOCKS_IN_TEMPLATE = 2
LOCKED_AT = dt.datetime.fromisoformat("2026-01-15T10:30:00+00:00")
EXPIRES_AT = dt.datetime.fromisoformat("2026-01-15T10:35:00+00:00")


def _run(*cli_args: str, color: bool = False) -> Result:
    return CliRunner().invoke(main, list(cli_args), color=color)


def _run_ok(*cli_args: str, color: bool = False) -> Result:
    cli_result = _run(*cli_args, color=color)
    assert cli_result.exit_code == EXIT_OK, cli_result.output
    return cli_result


def _run_failing(*cli_args: str) -> Result:
    cli_result = _run(*cli_args)
    assert cli_result.exit_code == EXIT_ERROR, cli_result.output
    return cli_result


def _assert_lock_was_held(lock_cls: MagicMock) -> None:
    lock_cls.assert_called_once()
    lock_cls.return_value.__enter__.assert_called_once()


@pytest.fixture
def migrator_cls() -> Iterator[MagicMock]:
    """Replace ``Migrator`` where the CLI builds it, so no command connects to ClickHouse."""
    with patch(f"{CLI_OPTIONS_MODULE}.Migrator") as patched_cls:
        yield patched_cls


@pytest.fixture
def mock_migrator(migrator_cls: MagicMock) -> MagicMock:
    """The migrator instance that every CLI command receives."""
    migrator_instance = MagicMock()
    migrator_cls.return_value = migrator_instance
    return migrator_instance


@pytest.fixture
def lock_cls(mock_migrator: MagicMock) -> Iterator[MagicMock]:
    """Replace ``MigrationLock`` where the CLI builds it; the lock it returns works as a context manager."""
    mock_migrator.get_db_name.return_value = FAKE_DB
    mock_migrator.ch_client = MagicMock()
    held_lock = MagicMock()
    held_lock.__enter__ = MagicMock(return_value=held_lock)
    held_lock.__exit__ = MagicMock(return_value=False)
    with patch(f"{CLI_OPTIONS_MODULE}.MigrationLock", return_value=held_lock) as patched_cls:
        yield patched_cls


@pytest.fixture
def create_users_migration() -> Migration:
    """A pending migration whose SQL must appear in dry-run output."""
    return Migration(
        name="001_create_users.sql",
        up="CREATE TABLE users (id Int32) ENGINE MergeTree() ORDER BY id",
        rollback="DROP TABLE users",
    )


def test_version_flag() -> None:
    """``--version`` names the distribution so users can report which release they run."""
    cli_result = _run_ok("--version")
    assert "py-clickhouse-migrator" in cli_result.output


def test_verbose_flag(mock_migrator: MagicMock) -> None:
    """``-v`` is accepted as a global option before the command."""
    mock_migrator.show_migrations.return_value = ShowMigrationsResult("ok", "")
    _run_ok("--url", FAKE_URL, "-v", "show")


def test_quiet_flag(mock_migrator: MagicMock) -> None:
    """``-q`` is accepted as a global option before the command."""
    mock_migrator.show_migrations.return_value = ShowMigrationsResult("ok", "")
    _run_ok("--url", FAKE_URL, "-q", "show")


def test_cli_init(tmp_path: Path) -> None:
    """``init`` creates the migrations directory given by ``--path``."""
    migrations_dir = tmp_path / "migrations"
    _run_ok("--path", str(migrations_dir), "init")
    assert migrations_dir.is_dir()


def test_cli_init_default_path() -> None:
    """Without ``--path``, ``init`` creates the default ``./db/migrations`` directory."""
    _run_ok("init")
    assert Path(DEFAULT_MIGRATIONS_DIR).is_dir()


@pytest.mark.parametrize(
    ("name_args", "expected_suffix"),
    [
        (("add_users",), "_add_users.sql"),
        ((), ".sql"),
    ],
    ids=["with_name", "without_name"],
)
def test_cli_new(tmp_path: Path, name_args: tuple[str, ...], expected_suffix: str) -> None:
    """``new`` writes exactly one SQL file whose template has an up and a down statement block."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    _run_ok("--path", str(migrations_dir), "new", *name_args)
    created_files = list(migrations_dir.iterdir())
    assert len(created_files) == 1
    assert created_files[0].name.endswith(expected_suffix)
    migration_text = created_files[0].read_text(encoding="utf-8")
    assert migration_text.count(STMT_MARKER) == STMT_BLOCKS_IN_TEMPLATE


def test_cli_new_default_path() -> None:
    """Without ``--path``, ``new`` writes into the default ``./db/migrations`` directory."""
    default_dir = Path(DEFAULT_MIGRATIONS_DIR)
    default_dir.mkdir(parents=True, exist_ok=True)
    _run_ok("new", "test_migration")
    created_names = [created_file.name for created_file in default_dir.iterdir()]
    assert any(filename.endswith("_test_migration.sql") for filename in created_names)


def test_cli_new_missing_dir(tmp_path: Path) -> None:
    """``new`` fails with a clear message instead of creating a missing migrations directory."""
    cli_result = _run_failing("--path", str(tmp_path / "nonexistent"), "new", "test")
    assert "not found" in cli_result.stderr


def test_up_dry_run_output_visible_with_quiet(create_users_migration: Migration) -> None:
    """--quiet must not suppress dry-run output (click.echo, not logger)."""
    with (
        patch.object(Migrator, "__init__", return_value=None),
        patch.object(Migrator, "check_integrity"),
        patch.object(Migrator, "get_migrations_for_apply", return_value=[create_users_migration]),
        patch.object(Migrator, "validate_migrations"),
    ):
        cli_result = _run_ok("--url", FAKE_URL, "--quiet", "up", "--dry-run")

    assert "(up)" in cli_result.output
    assert "CREATE TABLE" in cli_result.output


def test_cli_up_dry_run(mock_migrator: MagicMock) -> None:
    """``up --dry-run`` asks the migrator for a dry run with validation on and dirty state refused."""
    _run_ok("--url", FAKE_URL, "up", "--dry-run")
    mock_migrator.up.assert_called_once_with(n=None, dry_run=True, allow_dirty=False, validate=True)


def test_cli_up_dry_run_allow_dirty(mock_migrator: MagicMock) -> None:
    """``--allow-dirty`` reaches the migrator in a dry run."""
    _run_ok("--url", FAKE_URL, "up", "--dry-run", "--allow-dirty")
    mock_migrator.up.assert_called_once_with(n=None, dry_run=True, allow_dirty=True, validate=True)


def test_cli_up_dry_run_no_validate(mock_migrator: MagicMock) -> None:
    """``--no-validate`` turns off preflight validation in a dry run."""
    _run_ok("--url", FAKE_URL, "up", "--dry-run", "--no-validate")
    mock_migrator.up.assert_called_once_with(n=None, dry_run=True, allow_dirty=False, validate=False)


def test_cli_up_no_lock(mock_migrator: MagicMock, lock_cls: MagicMock) -> None:
    """``up --no-lock`` applies migrations without creating the migration lock."""
    _run_ok("--url", FAKE_URL, "up", "--no-lock")
    lock_cls.assert_not_called()
    mock_migrator.up.assert_called_once_with(n=None, allow_dirty=False, validate=True)


def test_cli_up_with_lock(mock_migrator: MagicMock, lock_cls: MagicMock) -> None:
    """``up`` holds the migration lock while it applies migrations."""
    _run_ok("--url", FAKE_URL, "up")
    _assert_lock_was_held(lock_cls)
    mock_migrator.up.assert_called_once_with(n=None, allow_dirty=False, validate=True)


def test_cli_up_no_pending_still_uses_lock(mock_migrator: MagicMock, lock_cls: MagicMock) -> None:
    """``up`` takes the lock before it knows whether anything is pending."""
    _run_ok("--url", FAKE_URL, "up")
    _assert_lock_was_held(lock_cls)
    mock_migrator.up.assert_called_once_with(n=None, allow_dirty=False, validate=True)


def test_up_fails_on_mismatch_without_pending(mock_migrator: MagicMock, lock_cls: MagicMock) -> None:
    """A checksum mismatch fails ``up`` under the lock even when no migration is pending."""
    mock_migrator.up.side_effect = ChecksumMismatchError("Checksum mismatch: 001.sql")
    cli_result = _run_failing("--url", FAKE_URL, "up")
    assert "Checksum mismatch: 001.sql" in cli_result.stderr
    _assert_lock_was_held(lock_cls)
    mock_migrator.up.assert_called_once_with(n=None, allow_dirty=False, validate=True)


def test_cli_up_with_number(mock_migrator: MagicMock) -> None:
    """The positional number limits how many migrations ``up`` applies."""
    _run_ok("--url", FAKE_URL, "up", "--no-lock", "3")
    mock_migrator.up.assert_called_once_with(n=3, allow_dirty=False, validate=True)


def test_rollback_dry_run_visible_with_quiet(create_users_migration: Migration) -> None:
    """--quiet must not suppress rollback dry-run output, which goes through click.echo."""
    with (
        patch.object(Migrator, "__init__", return_value=None),
        patch.object(Migrator, "get_migrations_for_rollback", return_value=[create_users_migration]),
        patch.object(Migrator, "validate_migrations"),
    ):
        cli_result = _run_ok("--url", FAKE_URL, "--quiet", "rollback", "--dry-run")

    assert "(rollback)" in cli_result.output
    assert "DROP TABLE" in cli_result.output


def test_cli_rollback_dry_run(mock_migrator: MagicMock) -> None:
    """``rollback --dry-run`` asks the migrator for a validated dry run of the last migration."""
    _run_ok("--url", FAKE_URL, "rollback", "--dry-run")
    mock_migrator.rollback.assert_called_once_with(number=1, dry_run=True, validate=True)


def test_cli_rollback_no_lock(mock_migrator: MagicMock, lock_cls: MagicMock) -> None:
    """``rollback --no-lock`` rolls back without creating the migration lock."""
    _run_ok("--url", FAKE_URL, "rollback", "--no-lock")
    lock_cls.assert_not_called()
    mock_migrator.rollback.assert_called_once_with(number=1, validate=True)


def test_cli_rollback_with_lock(mock_migrator: MagicMock, lock_cls: MagicMock) -> None:
    """``rollback`` holds the migration lock while it rolls back."""
    _run_ok("--url", FAKE_URL, "rollback")
    _assert_lock_was_held(lock_cls)
    mock_migrator.rollback.assert_called_once_with(number=1, validate=True)


def test_cli_rollback_with_number(mock_migrator: MagicMock) -> None:
    """The positional number sets how many migrations ``rollback`` reverts."""
    _run_ok("--url", FAKE_URL, "rollback", "--no-lock", "5")
    mock_migrator.rollback.assert_called_once_with(number=5, validate=True)


def test_cli_rollback_no_validate(mock_migrator: MagicMock) -> None:
    """``--no-validate`` turns off preflight validation for ``rollback``."""
    _run_ok("--url", FAKE_URL, "rollback", "--no-lock", "--no-validate")
    mock_migrator.rollback.assert_called_once_with(number=1, validate=False)


def test_cli_show(mock_migrator: MagicMock) -> None:
    """``show`` prints the migrator's report and hides fully applied history by default."""
    mock_migrator.show_migrations.return_value = ShowMigrationsResult("Applied: 0", "")
    cli_result = _run_ok("--url", FAKE_URL, "show")
    assert "Applied: 0" in cli_result.output
    mock_migrator.show_migrations.assert_called_once_with(show_all=False)


def test_cli_show_all(mock_migrator: MagicMock) -> None:
    """``show --all`` asks the migrator for every migration."""
    mock_migrator.show_migrations.return_value = ShowMigrationsResult("Applied: 5", "")
    _run_ok("--url", FAKE_URL, "show", "--all")
    mock_migrator.show_migrations.assert_called_once_with(show_all=True)


def test_cli_show_warning_to_stderr(mock_migrator: MagicMock) -> None:
    """``show`` keeps warnings on stderr so the report on stdout stays parseable."""
    mock_migrator.show_migrations.return_value = ShowMigrationsResult("output", "WARNING: 1 issue")
    cli_result = _run_ok("--url", FAKE_URL, "show")
    assert "output" in cli_result.output
    assert "WARNING: 1 issue" in cli_result.stderr


def test_cli_baseline_no_lock(mock_migrator: MagicMock, lock_cls: MagicMock) -> None:
    """``baseline --no-lock`` records every file without the lock and lists them as baselined."""
    mock_migrator.baseline.return_value = ["001.sql", "002.sql"]
    cli_result = _run_ok("--url", FAKE_URL, "baseline", "--no-lock")
    lock_cls.assert_not_called()
    mock_migrator.baseline.assert_called_once_with()
    assert "Baselined 2 migration(s)." in cli_result.output
    assert "[B]" in cli_result.output
    assert "001.sql" in cli_result.output
    assert "002.sql" in cli_result.output


def test_cli_baseline_with_lock(mock_migrator: MagicMock, lock_cls: MagicMock) -> None:
    """``baseline`` holds the migration lock while it records files."""
    mock_migrator.baseline.return_value = ["001.sql"]
    cli_result = _run_ok("--url", FAKE_URL, "baseline")
    _assert_lock_was_held(lock_cls)
    mock_migrator.baseline.assert_called_once_with()
    assert "Baselined 1 migration(s)." in cli_result.output
    assert "001.sql" in cli_result.output


def test_baseline_no_files_visible_with_quiet(mock_migrator: MagicMock) -> None:
    """--quiet must not hide the result of a baseline that found no files."""
    mock_migrator.baseline.return_value = []
    cli_result = _run_ok("--url", FAKE_URL, "--quiet", "baseline", "--no-lock")
    assert "No SQL migration files found to baseline." in cli_result.output


def test_cli_baseline_color_output(mock_migrator: MagicMock) -> None:
    """``baseline`` highlights its summary and the ``[B]`` markers on a color terminal."""
    mock_migrator.baseline.return_value = ["001.sql"]
    cli_result = _run_ok("--url", FAKE_URL, "baseline", "--no-lock", color=True)
    assert click.style("Baselined 1 migration(s).", fg="green", bold=True) in cli_result.output
    assert click.style("[B]", fg="cyan") in cli_result.output


def test_baseline_error_clean_output(mock_migrator: MagicMock) -> None:
    """A ``BaselineError`` prints as one ``Error:`` line with exit code 1, not a traceback."""
    mock_migrator.baseline.side_effect = BaselineError("Baseline requires an empty db_migrations table.")
    cli_result = _run_failing("--url", FAKE_URL, "baseline", "--no-lock")
    assert "Error: " in cli_result.stderr
    assert "Baseline requires an empty db_migrations table." in cli_result.stderr
    assert "Traceback" not in cli_result.output


def test_cli_repair_nothing(mock_migrator: MagicMock) -> None:
    """``repair`` says so when every stored checksum already matches its file."""
    mock_migrator.validate_checksums.return_value = []
    cli_result = _run_ok("--url", FAKE_URL, "repair")
    assert "Nothing to repair" in cli_result.output


def test_cli_repair_with_mismatch(mock_migrator: MagicMock) -> None:
    """``repair`` lists modified migrations and reports how many checksums it updated."""
    mock_migrator.validate_checksums.return_value = [
        ChecksumMismatch("001.sql", "aaa111bbb222ccc", "ddd444eee555fff"),
    ]
    mock_migrator.repair.return_value = ["001.sql"]
    cli_result = _run_ok("--url", FAKE_URL, "repair")
    assert "Modified migrations:" in cli_result.output
    assert "Repaired 1 migration(s)" in cli_result.output


def test_cli_repair_missing_file(mock_migrator: MagicMock) -> None:
    """``repair`` skips an applied migration whose file is gone and says why."""
    mock_migrator.validate_checksums.return_value = [
        ChecksumMismatch("001.sql", "aaa111bbb222ccc", ""),
    ]
    mock_migrator.repair.return_value = []
    cli_result = _run_ok("--url", FAKE_URL, "repair")
    assert "file missing (skipped)" in cli_result.output


def test_cli_force_unlock(mock_migrator: MagicMock, lock_cls: MagicMock) -> None:
    """``force-unlock`` force-releases the lock of the migrator's database, built with default lock settings."""
    cli_result = _run_ok("--url", FAKE_URL, "force-unlock")
    assert "Lock forcefully released" in cli_result.output
    lock_cls.assert_called_once_with(
        client=mock_migrator.ch_client,
        db=FAKE_DB,
        ttl=DEFAULT_LOCK_TTL,
        retry_count=0,
        cluster="",
    )
    lock_cls.return_value.release.assert_called_once_with(force=True)


def test_cli_lock_info_no_lock(lock_cls: MagicMock) -> None:
    """``lock-info`` says so when nobody holds the lock."""
    lock_cls.return_value.get_lock_info.return_value = None
    cli_result = _run_ok("--url", FAKE_URL, "lock-info")
    assert "No active lock" in cli_result.output


def test_cli_lock_info_active_lock(lock_cls: MagicMock) -> None:
    """``lock-info`` shows the holder and the lock times as ``YYYY-MM-DD HH:MM:SS``."""
    lock_cls.return_value.get_lock_info.return_value = LockInfo(
        locked_by="host:123",
        locked_at=LOCKED_AT,
        expires_at=EXPIRES_AT,
    )
    cli_result = _run_ok("--url", FAKE_URL, "lock-info")
    assert "Locked by: host:123" in cli_result.output
    assert "2026-01-15 10:30:00" in cli_result.output
    assert "2026-01-15 10:35:00" in cli_result.output


@pytest.mark.parametrize(
    ("exception", "expected_text"),
    [
        (LockError("other", LOCKED_AT, EXPIRES_AT), "other"),
        (ChecksumMismatchError("Checksum mismatch"), "Checksum mismatch"),
        (InvalidMigrationError("bad query"), "bad query"),
    ],
)
def test_handled_exception_clean_output(mock_migrator: MagicMock, exception: Exception, expected_text: str) -> None:
    """Expected errors from ``up`` print as one ``Error:`` line with exit code 1, not a traceback."""
    mock_migrator.up.side_effect = exception
    cli_result = _run_failing("--url", FAKE_URL, "up", "--no-lock")
    assert "Error: " in cli_result.stderr
    assert expected_text in cli_result.stderr
    assert "Traceback" not in cli_result.output


@pytest.mark.parametrize(
    ("exception", "expected_text"),
    [
        (ClickHouseServerIsNotHealthyError("not healthy"), "not healthy"),
        (MissingDatabaseUrlError("url was not provided"), "url was not provided"),
        (DatabaseNotFoundError("does not exist"), "does not exist"),
        (MigrationDirectoryNotFoundError("dir not found"), "dir not found"),
    ],
)
def test_migrator_init_exception_clean_output(
    migrator_cls: MagicMock,
    exception: Exception,
    expected_text: str,
) -> None:
    """Connection and setup errors while building the migrator print as a clean ``Error:`` line."""
    migrator_cls.side_effect = exception
    cli_result = _run_failing("--url", FAKE_URL, "show")
    assert "Error: " in cli_result.stderr
    assert expected_text in cli_result.stderr


def test_unexpected_error_not_handled(mock_migrator: MagicMock) -> None:
    """Unexpected errors keep their traceback so bugs are not hidden behind a one-line message."""
    mock_migrator.up.side_effect = RuntimeError("unexpected")
    cli_result = _run("--url", FAKE_URL, "up", "--no-lock")
    assert cli_result.exit_code != EXIT_OK
    assert isinstance(cli_result.exception, RuntimeError)


def test_cluster_option_passed_to_migrator(migrator_cls: MagicMock, mock_migrator: MagicMock) -> None:
    """``--cluster`` reaches the migrator so service tables are created ``ON CLUSTER``."""
    mock_migrator.show_migrations.return_value = ShowMigrationsResult("ok", "")
    _run("--url", FAKE_URL, "--cluster", "my_cluster", "show")
    migrator_cls.assert_called_once()
    assert migrator_cls.call_args.kwargs["cluster"] == "my_cluster"


def test_connect_retries_passed_to_migrator(migrator_cls: MagicMock, mock_migrator: MagicMock) -> None:
    """``--connect-retries`` and ``--connect-retries-interval`` reach the migrator's connection retry loop."""
    mock_migrator.show_migrations.return_value = ShowMigrationsResult("ok", "")
    _run("--url", FAKE_URL, "--connect-retries", "5", "--connect-retries-interval", "2", "show")
    migrator_kwargs = migrator_cls.call_args.kwargs
    assert migrator_kwargs["connect_retries"] == 5
    assert migrator_kwargs["connect_retries_interval"] == 2
