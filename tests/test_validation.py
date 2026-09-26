from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from py_clickhouse_migrator.cli import main
from py_clickhouse_migrator.migrator import Migrator, create_migration_file

FAKE_URL = "clickhouse://default@localhost:9000/default"
USAGE_ERROR_EXIT_CODE = 2
TEMPLATE_STATEMENT_BLOCKS = 2


def _make_migrator(cluster: str) -> Migrator:
    with (
        patch("py_clickhouse_migrator.migrator.Client.from_url", return_value=MagicMock()),
        patch.object(Migrator, "check_migrations_table"),
    ):
        return Migrator(database_url=FAKE_URL, cluster=cluster)


def _cli_exit_code(args: list[str]) -> int:
    return CliRunner().invoke(main, ["--url", FAKE_URL, *args]).exit_code


def _make_migrations_dir(tmp_path: Path) -> str:
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    return str(migrations_dir)


@pytest.mark.parametrize(
    ("cmd", "args"),
    [
        ("up", ["0"]),
        ("up", ["-1"]),
        ("rollback", ["0"]),
        ("rollback", ["-1"]),
    ],
)
def test_cli_rejects_non_positive_number(cmd: str, args: list[str]) -> None:
    """``up`` and ``rollback`` reject a migration count below one as a usage error."""
    assert _cli_exit_code([cmd, *args]) == USAGE_ERROR_EXIT_CODE


@pytest.mark.parametrize(
    ("option", "option_value"),
    [
        ("--lock-ttl", "0"),
        ("--lock-ttl", "-1"),
        ("--lock-retry", "-1"),
    ],
)
def test_cli_rejects_invalid_lock_params(option: str, option_value: str) -> None:
    """``up`` rejects a lock TTL below one and a negative lock retry count as a usage error."""
    assert _cli_exit_code(["up", option, option_value]) == USAGE_ERROR_EXIT_CODE


@pytest.mark.parametrize(
    ("option", "option_value"),
    [
        ("--connect-retries", "-1"),
        ("--connect-retries-interval", "-1"),
    ],
)
def test_cli_rejects_negative_connect_params(option: str, option_value: str) -> None:
    """Negative connection retry settings are rejected as a usage error."""
    assert _cli_exit_code([option, option_value, "show"]) == USAGE_ERROR_EXIT_CODE


@pytest.mark.parametrize(
    "cluster",
    [
        "DROP TABLE",
        "my-cluster",
        "cluster; --",
        "123abc",
    ],
)
def test_rejects_invalid_cluster_name(cluster: str) -> None:
    """Cluster names that are not SQL identifiers are refused before they reach any query."""
    with pytest.raises(ValueError, match="Invalid cluster name"):
        _make_migrator(cluster=cluster)


@pytest.mark.parametrize(
    "cluster",
    [
        "my_cluster",
        "production",
        "_cluster1",
    ],
)
def test_accepts_valid_cluster_name(cluster: str) -> None:
    """Cluster names that are SQL identifiers are stored unchanged."""
    migrator = _make_migrator(cluster=cluster)
    assert migrator.cluster == cluster


def test_rejects_invalid_migration_name() -> None:
    """Migration names with path separators are refused, so no file is written outside the directory."""
    with pytest.raises(ValueError, match="Invalid migration name"):
        create_migration_file(name="../../etc")


def test_accepts_valid_migration_name(tmp_path: Path) -> None:
    """A valid name ends up in the file name of the created migration."""
    filepath = create_migration_file(migrations_dir=_make_migrations_dir(tmp_path), name="add_users_table")
    assert "add_users_table" in filepath
    assert Path(filepath).exists()


def test_new_migration_template_contains_markers(tmp_path: Path) -> None:
    """A new migration is a ``.sql`` file with both section markers and one ``-- @stmt`` block per section."""
    filepath = create_migration_file(migrations_dir=_make_migrations_dir(tmp_path), name="create_users")

    template = Path(filepath).read_text(encoding="utf-8")

    assert filepath.endswith(".sql")
    assert "-- migrator:up" in template
    assert "-- migrator:down" in template
    assert template.count("-- @stmt") == TEMPLATE_STATEMENT_BLOCKS
