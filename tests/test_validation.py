from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from py_clickhouse_migrator.cli import main
from py_clickhouse_migrator.migrator import Migrator, create_migration_file

FAKE_URL = "clickhouse://default@localhost:9000/default"


def _make_migrator(**kwargs: object) -> Migrator:
    with (
        patch("py_clickhouse_migrator.migrator.Client.from_url", return_value=MagicMock()),
        patch.object(Migrator, "check_migrations_table"),
    ):
        return Migrator(database_url=FAKE_URL, **kwargs)  # type: ignore[arg-type]


# --- CLI IntRange validation ---


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
    runner = CliRunner()
    result = runner.invoke(main, ["--url", FAKE_URL, cmd] + args)
    assert result.exit_code != 0


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("--lock-ttl", "0"),
        ("--lock-ttl", "-1"),
        ("--lock-retry", "-1"),
    ],
)
def test_cli_rejects_invalid_lock_params(option: str, value: str) -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["--url", FAKE_URL, "up", option, value])
    assert result.exit_code != 0


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("--connect-retries", "-1"),
        ("--connect-retries-interval", "-1"),
    ],
)
def test_cli_rejects_negative_connect_params(option: str, value: str) -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["--url", FAKE_URL, option, value, "show"])
    assert result.exit_code != 0


# --- cluster name validation ---


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
    m = _make_migrator(cluster=cluster)
    assert m.cluster == cluster


# --- migration name validation ---


def test_rejects_invalid_migration_name() -> None:
    with pytest.raises(ValueError, match="Invalid migration name"):
        create_migration_file(name="../../etc")


def test_accepts_valid_migration_name(tmp_path: pytest.TempPathFactory) -> None:
    path = str(tmp_path / "migrations")
    os.makedirs(path)
    filepath = create_migration_file(migrations_dir=path, name="add_users_table")
    assert "add_users_table" in filepath
    assert os.path.exists(filepath)
