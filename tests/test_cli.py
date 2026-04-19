"""Unit tests for CLI commands. All ClickHouse interactions are mocked."""

from __future__ import annotations

import datetime as dt
import os
from collections.abc import Generator
from unittest.mock import MagicMock, patch

import click
import pytest
from click.testing import CliRunner

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
from py_clickhouse_migrator.lock import LockError, LockInfo
from py_clickhouse_migrator.migrator import (
    DEFAULT_MIGRATIONS_DIR,
    ChecksumMismatch,
    Migration,
    Migrator,
    ShowMigrationsResult,
)

FAKE_URL = "clickhouse://default@localhost:9000/test"


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture()
def mock_migrator() -> Generator[MagicMock]:
    with patch("py_clickhouse_migrator.cli.Migrator") as mock_cls:
        instance = MagicMock()
        mock_cls.return_value = instance
        yield instance


# --- version ---


def test_version_flag(runner: CliRunner) -> None:
    result = runner.invoke(main, ["--version"])
    assert result.exit_code == 0
    assert "py-clickhouse-migrator" in result.output


# --- logging flags ---


def test_verbose_flag(runner: CliRunner, mock_migrator: MagicMock) -> None:
    mock_migrator.show_migrations.return_value = ShowMigrationsResult("ok", "")
    result = runner.invoke(main, ["--url", FAKE_URL, "-v", "show"])
    assert result.exit_code == 0


def test_quiet_flag(runner: CliRunner, mock_migrator: MagicMock) -> None:
    mock_migrator.show_migrations.return_value = ShowMigrationsResult("ok", "")
    result = runner.invoke(main, ["--url", FAKE_URL, "-q", "show"])
    assert result.exit_code == 0


# --- init ---


def test_cli_init(runner: CliRunner, tmp_path: pytest.TempPathFactory) -> None:
    path = str(tmp_path / "migrations")
    result = runner.invoke(main, ["--path", path, "init"])
    assert result.exit_code == 0
    assert os.path.isdir(path)


def test_cli_init_default_path(runner: CliRunner) -> None:
    result = runner.invoke(main, ["init"])
    assert result.exit_code == 0
    assert os.path.isdir(DEFAULT_MIGRATIONS_DIR)


# --- new ---


def test_cli_new(runner: CliRunner, tmp_path: pytest.TempPathFactory) -> None:
    path = str(tmp_path / "migrations")
    os.makedirs(path)
    result = runner.invoke(main, ["--path", path, "new", "add_users"])
    assert result.exit_code == 0
    files = os.listdir(path)
    assert len(files) == 1
    assert files[0].endswith("_add_users.sql")
    content = (tmp_path / "migrations" / files[0]).read_text(encoding="utf-8")
    assert content.count("-- @stmt") == 2


def test_cli_new_without_name(runner: CliRunner, tmp_path: pytest.TempPathFactory) -> None:
    path = str(tmp_path / "migrations")
    os.makedirs(path)
    result = runner.invoke(main, ["--path", path, "new"])
    assert result.exit_code == 0
    files = os.listdir(path)
    assert len(files) == 1
    assert files[0].endswith(".sql")
    content = (tmp_path / "migrations" / files[0]).read_text(encoding="utf-8")
    assert content.count("-- @stmt") == 2


def test_cli_new_default_path(runner: CliRunner) -> None:
    os.makedirs(DEFAULT_MIGRATIONS_DIR, exist_ok=True)
    result = runner.invoke(main, ["new", "test_migration"])
    assert result.exit_code == 0
    files = os.listdir(DEFAULT_MIGRATIONS_DIR)
    assert any(f.endswith("_test_migration.sql") for f in files)


def test_cli_new_missing_dir(runner: CliRunner, tmp_path: pytest.TempPathFactory) -> None:
    path = str(tmp_path / "nonexistent")
    result = runner.invoke(main, ["--path", path, "new", "test"])
    assert result.exit_code == 1
    assert "not found" in result.stderr


# --- up ---


def test_up_dry_run_output_visible_with_quiet(runner: CliRunner) -> None:
    """--quiet must not suppress dry-run output (click.echo, not logger)."""
    migrations = [
        Migration(
            name="001_create_users.sql",
            up="CREATE TABLE users (id Int32) ENGINE MergeTree() ORDER BY id",
            rollback="DROP TABLE users",
        ),
    ]
    with (
        patch.object(Migrator, "__init__", return_value=None),
        patch.object(Migrator, "check_integrity"),
        patch.object(Migrator, "get_migrations_for_apply", return_value=migrations),
        patch.object(Migrator, "validate_migrations"),
    ):
        result = runner.invoke(main, ["--url", FAKE_URL, "--quiet", "up", "--dry-run"])

    assert result.exit_code == 0
    assert "(up)" in result.output
    assert "CREATE TABLE" in result.output


def test_cli_up_dry_run(runner: CliRunner, mock_migrator: MagicMock) -> None:
    result = runner.invoke(main, ["--url", FAKE_URL, "up", "--dry-run"])
    assert result.exit_code == 0
    mock_migrator.up.assert_called_once_with(n=None, dry_run=True, allow_dirty=False, validate=True)


def test_cli_up_dry_run_allow_dirty(runner: CliRunner, mock_migrator: MagicMock) -> None:
    result = runner.invoke(main, ["--url", FAKE_URL, "up", "--dry-run", "--allow-dirty"])
    assert result.exit_code == 0
    mock_migrator.up.assert_called_once_with(n=None, dry_run=True, allow_dirty=True, validate=True)


def test_cli_up_dry_run_no_validate(runner: CliRunner, mock_migrator: MagicMock) -> None:
    result = runner.invoke(main, ["--url", FAKE_URL, "up", "--dry-run", "--no-validate"])
    assert result.exit_code == 0
    mock_migrator.up.assert_called_once_with(n=None, dry_run=True, allow_dirty=False, validate=False)


def test_cli_up_no_lock(runner: CliRunner, mock_migrator: MagicMock) -> None:
    with patch("py_clickhouse_migrator.cli.MigrationLock") as mock_lock_cls:
        result = runner.invoke(main, ["--url", FAKE_URL, "up", "--no-lock"])
    assert result.exit_code == 0
    mock_lock_cls.assert_not_called()
    mock_migrator.up.assert_called_once_with(n=None, allow_dirty=False, validate=True)


def test_cli_up_with_lock_and_pending(runner: CliRunner, mock_migrator: MagicMock) -> None:
    mock_migrator.get_unapplied_migration_names.return_value = ["001.sql"]
    mock_migrator.get_db_name.return_value = "test"
    mock_migrator.ch_client = MagicMock()

    with patch("py_clickhouse_migrator.cli.MigrationLock") as mock_lock_cls:
        mock_lock_instance = MagicMock()
        mock_lock_cls.return_value = mock_lock_instance
        mock_lock_instance.__enter__ = MagicMock(return_value=mock_lock_instance)
        mock_lock_instance.__exit__ = MagicMock(return_value=False)

        result = runner.invoke(main, ["--url", FAKE_URL, "up"])

    assert result.exit_code == 0
    mock_lock_cls.assert_called_once()
    mock_lock_instance.__enter__.assert_called_once()
    mock_migrator.up.assert_called_once_with(n=None, allow_dirty=False, validate=True)


def test_cli_up_no_pending_skips_lock(runner: CliRunner, mock_migrator: MagicMock) -> None:
    mock_migrator.get_unapplied_migration_names.return_value = []
    with patch("py_clickhouse_migrator.cli.MigrationLock") as mock_lock_cls:
        result = runner.invoke(main, ["--url", FAKE_URL, "up"])
    assert result.exit_code == 0
    mock_lock_cls.assert_not_called()
    mock_migrator.up.assert_not_called()


def test_cli_up_with_number(runner: CliRunner, mock_migrator: MagicMock) -> None:
    result = runner.invoke(main, ["--url", FAKE_URL, "up", "--no-lock", "3"])
    assert result.exit_code == 0
    mock_migrator.up.assert_called_once_with(n=3, allow_dirty=False, validate=True)


# --- rollback ---


def test_rollback_dry_run_output_visible_with_quiet(runner: CliRunner) -> None:
    migrations = [
        Migration(
            name="001_create_users.sql",
            up="CREATE TABLE users (id Int32) ENGINE MergeTree() ORDER BY id",
            rollback="DROP TABLE users",
        ),
    ]
    with (
        patch.object(Migrator, "__init__", return_value=None),
        patch.object(Migrator, "get_migrations_for_rollback", return_value=migrations),
        patch.object(Migrator, "validate_migrations"),
    ):
        result = runner.invoke(main, ["--url", FAKE_URL, "--quiet", "rollback", "--dry-run"])

    assert result.exit_code == 0
    assert "(rollback)" in result.output
    assert "DROP TABLE" in result.output


def test_cli_rollback_dry_run(runner: CliRunner, mock_migrator: MagicMock) -> None:
    result = runner.invoke(main, ["--url", FAKE_URL, "rollback", "--dry-run"])
    assert result.exit_code == 0
    mock_migrator.rollback.assert_called_once_with(number=1, dry_run=True, validate=True)


def test_cli_rollback_no_lock(runner: CliRunner, mock_migrator: MagicMock) -> None:
    with patch("py_clickhouse_migrator.cli.MigrationLock") as mock_lock_cls:
        result = runner.invoke(main, ["--url", FAKE_URL, "rollback", "--no-lock"])
    assert result.exit_code == 0
    mock_lock_cls.assert_not_called()
    mock_migrator.rollback.assert_called_once_with(number=1, validate=True)


def test_cli_rollback_with_lock(runner: CliRunner, mock_migrator: MagicMock) -> None:
    mock_migrator.get_db_name.return_value = "test"
    mock_migrator.ch_client = MagicMock()

    with patch("py_clickhouse_migrator.cli.MigrationLock") as mock_lock_cls:
        mock_lock_instance = MagicMock()
        mock_lock_cls.return_value = mock_lock_instance
        mock_lock_instance.__enter__ = MagicMock(return_value=mock_lock_instance)
        mock_lock_instance.__exit__ = MagicMock(return_value=False)

        result = runner.invoke(main, ["--url", FAKE_URL, "rollback"])

    assert result.exit_code == 0
    mock_lock_cls.assert_called_once()
    mock_lock_instance.__enter__.assert_called_once()
    mock_migrator.rollback.assert_called_once_with(number=1, validate=True)


def test_cli_rollback_with_number(runner: CliRunner, mock_migrator: MagicMock) -> None:
    result = runner.invoke(main, ["--url", FAKE_URL, "rollback", "--no-lock", "5"])
    assert result.exit_code == 0
    mock_migrator.rollback.assert_called_once_with(number=5, validate=True)


def test_cli_rollback_no_validate(runner: CliRunner, mock_migrator: MagicMock) -> None:
    result = runner.invoke(main, ["--url", FAKE_URL, "rollback", "--no-lock", "--no-validate"])
    assert result.exit_code == 0
    mock_migrator.rollback.assert_called_once_with(number=1, validate=False)


# --- show ---


def test_cli_show(runner: CliRunner, mock_migrator: MagicMock) -> None:
    mock_migrator.show_migrations.return_value = ShowMigrationsResult("Applied: 0", "")
    result = runner.invoke(main, ["--url", FAKE_URL, "show"])
    assert result.exit_code == 0
    assert "Applied: 0" in result.output
    mock_migrator.show_migrations.assert_called_once_with(show_all=False)


def test_cli_show_all(runner: CliRunner, mock_migrator: MagicMock) -> None:
    mock_migrator.show_migrations.return_value = ShowMigrationsResult("Applied: 5", "")
    result = runner.invoke(main, ["--url", FAKE_URL, "show", "--all"])
    assert result.exit_code == 0
    mock_migrator.show_migrations.assert_called_once_with(show_all=True)


def test_cli_show_warning_to_stderr(runner: CliRunner, mock_migrator: MagicMock) -> None:
    mock_migrator.show_migrations.return_value = ShowMigrationsResult("output", "WARNING: 1 issue")
    result = runner.invoke(main, ["--url", FAKE_URL, "show"])
    assert result.exit_code == 0
    assert "output" in result.output
    assert "WARNING: 1 issue" in result.stderr


# --- baseline ---


def test_cli_baseline_no_lock(runner: CliRunner, mock_migrator: MagicMock) -> None:
    mock_migrator.baseline.return_value = ["001.sql", "002.sql"]

    with patch("py_clickhouse_migrator.cli.MigrationLock") as mock_lock_cls:
        result = runner.invoke(main, ["--url", FAKE_URL, "baseline", "--no-lock"])

    assert result.exit_code == 0
    mock_lock_cls.assert_not_called()
    mock_migrator.baseline.assert_called_once_with()
    assert "Baselined 2 migration(s)." in result.output
    assert "[B]" in result.output
    assert "001.sql" in result.output
    assert "002.sql" in result.output


def test_cli_baseline_with_lock(runner: CliRunner, mock_migrator: MagicMock) -> None:
    mock_migrator.get_db_name.return_value = "test"
    mock_migrator.ch_client = MagicMock()
    mock_migrator.baseline.return_value = ["001.sql"]

    with patch("py_clickhouse_migrator.cli.MigrationLock") as mock_lock_cls:
        mock_lock_instance = MagicMock()
        mock_lock_cls.return_value = mock_lock_instance
        mock_lock_instance.__enter__ = MagicMock(return_value=mock_lock_instance)
        mock_lock_instance.__exit__ = MagicMock(return_value=False)

        result = runner.invoke(main, ["--url", FAKE_URL, "baseline"])

    assert result.exit_code == 0
    mock_lock_cls.assert_called_once()
    mock_lock_instance.__enter__.assert_called_once()
    mock_migrator.baseline.assert_called_once_with()
    assert "Baselined 1 migration(s)." in result.output
    assert "001.sql" in result.output


def test_cli_baseline_no_files_output_visible_with_quiet(runner: CliRunner, mock_migrator: MagicMock) -> None:
    mock_migrator.baseline.return_value = []

    result = runner.invoke(main, ["--url", FAKE_URL, "--quiet", "baseline", "--no-lock"])

    assert result.exit_code == 0
    assert "No SQL migration files found to baseline." in result.output


def test_cli_baseline_color_output(runner: CliRunner, mock_migrator: MagicMock) -> None:
    mock_migrator.baseline.return_value = ["001.sql"]

    result = runner.invoke(main, ["--url", FAKE_URL, "baseline", "--no-lock"], color=True)

    assert result.exit_code == 0
    assert click.style("Baselined 1 migration(s).", fg="green", bold=True) in result.output
    assert click.style("[B]", fg="cyan") in result.output


def test_cli_baseline_handled_exception_clean_output(runner: CliRunner) -> None:
    with patch("py_clickhouse_migrator.cli.Migrator") as mock_cls:
        mock_cls.return_value.baseline.side_effect = BaselineError("Baseline requires an empty db_migrations table.")
        result = runner.invoke(main, ["--url", FAKE_URL, "baseline", "--no-lock"])

    assert result.exit_code == 1
    assert "Error: " in result.stderr
    assert "Baseline requires an empty db_migrations table." in result.stderr
    assert "Traceback" not in result.output


# --- repair ---


def test_cli_repair_nothing(runner: CliRunner, mock_migrator: MagicMock) -> None:
    mock_migrator.validate_checksums.return_value = []
    result = runner.invoke(main, ["--url", FAKE_URL, "repair"])
    assert result.exit_code == 0
    assert "Nothing to repair" in result.output


def test_cli_repair_with_mismatch(runner: CliRunner, mock_migrator: MagicMock) -> None:
    mock_migrator.validate_checksums.return_value = [
        ChecksumMismatch("001.sql", "aaa111bbb222ccc", "ddd444eee555fff"),
    ]
    mock_migrator.repair.return_value = ["001.sql"]
    result = runner.invoke(main, ["--url", FAKE_URL, "repair"])
    assert result.exit_code == 0
    assert "Modified migrations:" in result.output
    assert "Repaired 1 migration(s)" in result.output


def test_cli_repair_missing_file(runner: CliRunner, mock_migrator: MagicMock) -> None:
    mock_migrator.validate_checksums.return_value = [
        ChecksumMismatch("001.sql", "aaa111bbb222ccc", ""),
    ]
    mock_migrator.repair.return_value = []
    result = runner.invoke(main, ["--url", FAKE_URL, "repair"])
    assert result.exit_code == 0
    assert "file missing (skipped)" in result.output


# --- force-unlock ---


def test_cli_force_unlock(runner: CliRunner, mock_migrator: MagicMock) -> None:
    mock_migrator.get_db_name.return_value = "test"
    mock_migrator.ch_client = MagicMock()

    with patch("py_clickhouse_migrator.cli.MigrationLock") as mock_lock_cls:
        mock_lock = MagicMock()
        mock_lock_cls.return_value = mock_lock
        result = runner.invoke(main, ["--url", FAKE_URL, "force-unlock"])

    assert result.exit_code == 0
    assert "Lock forcefully released" in result.output
    mock_lock_cls.assert_called_once_with(client=mock_migrator.ch_client, db="test", cluster="")
    mock_lock.release.assert_called_once_with(force=True)


# --- lock-info ---


def test_cli_lock_info_no_lock(runner: CliRunner, mock_migrator: MagicMock) -> None:
    mock_migrator.get_db_name.return_value = "test"
    mock_migrator.ch_client = MagicMock()

    with patch("py_clickhouse_migrator.cli.MigrationLock") as mock_lock_cls:
        mock_lock_cls.return_value.get_lock_info.return_value = None
        result = runner.invoke(main, ["--url", FAKE_URL, "lock-info"])

    assert result.exit_code == 0
    assert "No active lock" in result.output


def test_cli_lock_info_active_lock(runner: CliRunner, mock_migrator: MagicMock) -> None:
    mock_migrator.get_db_name.return_value = "test"
    mock_migrator.ch_client = MagicMock()

    with patch("py_clickhouse_migrator.cli.MigrationLock") as mock_lock_cls:
        mock_lock_cls.return_value.get_lock_info.return_value = LockInfo(
            locked_by="host:123",
            locked_at=dt.datetime(2026, 1, 15, 10, 30, 0),
            expires_at=dt.datetime(2026, 1, 15, 10, 35, 0),
        )
        result = runner.invoke(main, ["--url", FAKE_URL, "lock-info"])

    assert result.exit_code == 0
    assert "Locked by: host:123" in result.output
    assert "2026-01-15 10:30:00" in result.output
    assert "2026-01-15 10:35:00" in result.output


# --- SafeGroup error handling ---


@pytest.mark.parametrize(
    ("exception", "expected_text"),
    [
        (
            LockError("other", dt.datetime(2024, 1, 1), dt.datetime(2024, 1, 2)),
            "other",
        ),
        (
            ChecksumMismatchError("Checksum mismatch"),
            "Checksum mismatch",
        ),
        (
            InvalidMigrationError("bad query"),
            "bad query",
        ),
    ],
)
def test_handled_exception_clean_output(runner: CliRunner, exception: Exception, expected_text: str) -> None:
    with patch("py_clickhouse_migrator.cli.Migrator") as mock_cls:
        mock_cls.return_value.up.side_effect = exception
        result = runner.invoke(main, ["--url", FAKE_URL, "up", "--no-lock"])

    assert result.exit_code == 1
    assert "Error: " in result.stderr
    assert expected_text in result.stderr
    assert "Traceback" not in result.output


@pytest.mark.parametrize(
    ("exception", "expected_text"),
    [
        (
            ClickHouseServerIsNotHealthyError("not healthy"),
            "not healthy",
        ),
        (
            MissingDatabaseUrlError("url was not provided"),
            "url was not provided",
        ),
        (
            DatabaseNotFoundError("does not exist"),
            "does not exist",
        ),
        (
            MigrationDirectoryNotFoundError("dir not found"),
            "dir not found",
        ),
    ],
)
def test_migrator_init_exception_clean_output(runner: CliRunner, exception: Exception, expected_text: str) -> None:
    with patch("py_clickhouse_migrator.cli.Migrator") as mock_cls:
        mock_cls.side_effect = exception
        result = runner.invoke(main, ["--url", FAKE_URL, "show"])

    assert result.exit_code == 1
    assert "Error: " in result.stderr
    assert expected_text in result.stderr


def test_unexpected_error_not_handled(runner: CliRunner) -> None:
    with patch("py_clickhouse_migrator.cli.Migrator") as mock_cls:
        mock_cls.return_value.up.side_effect = RuntimeError("unexpected")
        result = runner.invoke(main, ["--url", FAKE_URL, "up", "--no-lock"])

    assert result.exit_code != 0
    assert isinstance(result.exception, RuntimeError)


# --- CLI option pass-through ---


def test_cluster_option_passed_to_migrator(runner: CliRunner) -> None:
    with patch("py_clickhouse_migrator.cli.Migrator") as mock_cls:
        mock_cls.return_value.show_migrations.return_value = ShowMigrationsResult("ok", "")
        runner.invoke(main, ["--url", FAKE_URL, "--cluster", "my_cluster", "show"])

    mock_cls.assert_called_once()
    call_kwargs = mock_cls.call_args
    assert call_kwargs.kwargs["cluster"] == "my_cluster"


def test_connect_retries_passed_to_migrator(runner: CliRunner) -> None:
    with patch("py_clickhouse_migrator.cli.Migrator") as mock_cls:
        mock_cls.return_value.show_migrations.return_value = ShowMigrationsResult("ok", "")
        runner.invoke(main, ["--url", FAKE_URL, "--connect-retries", "5", "--connect-retries-interval", "2", "show"])

    call_kwargs = mock_cls.call_args[1]
    assert call_kwargs["connect_retries"] == 5
    assert call_kwargs["connect_retries_interval"] == 2
