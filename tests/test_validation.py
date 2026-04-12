from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from py_clickhouse_migrator.cli import main
from py_clickhouse_migrator.errors import InvalidMigrationError
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


# --- SQL migration format ---


def test_new_sql_migration_template_contains_markers(tmp_path: pytest.TempPathFactory) -> None:
    path = str(tmp_path / "migrations")
    os.makedirs(path)

    filepath = create_migration_file(migrations_dir=path, name="create_users")

    with open(filepath, encoding="utf-8") as f:
        content = f.read()

    assert filepath.endswith(".sql")
    assert "-- migrator:up" in content
    assert "-- migrator:down" in content


def test_sql_migration_is_parsed_for_apply(tmp_path: pytest.TempPathFactory) -> None:
    path = str(tmp_path / "migrations")
    os.makedirs(path)
    filepath = os.path.join(path, "20260412120000_create_users.sql")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(
            "-- migrator:up\n"
            "CREATE TABLE users (id UInt64) ENGINE = MergeTree() ORDER BY id;\n\n"
            "-- migrator:down\n"
            "DROP TABLE IF EXISTS users;\n"
        )

    migrator = _make_migrator(migrations_dir=path)
    migrator.ch_client.execute.return_value = []

    migrations = migrator.get_migrations_for_apply()

    assert len(migrations) == 1
    assert migrations[0].name == "20260412120000_create_users.sql"
    assert migrations[0].up == "CREATE TABLE users (id UInt64) ENGINE = MergeTree() ORDER BY id;"
    assert migrations[0].rollback == "DROP TABLE IF EXISTS users;"


def test_sql_migration_allows_empty_rollback(tmp_path: pytest.TempPathFactory) -> None:
    path = str(tmp_path / "migrations")
    os.makedirs(path)
    filepath = os.path.join(path, "20260412120000_create_users.sql")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(
            "-- migrator:up\nCREATE TABLE users (id UInt64) ENGINE = MergeTree() ORDER BY id;\n\n-- migrator:down\n"
        )

    migrator = _make_migrator(migrations_dir=path)
    migrator.ch_client.execute.return_value = []

    migrations = migrator.get_migrations_for_apply()

    assert len(migrations) == 1
    assert migrations[0].rollback == ""


def test_get_unapplied_migration_names_skips_non_sql_files(tmp_path: pytest.TempPathFactory) -> None:
    path = str(tmp_path / "migrations")
    os.makedirs(path)
    with open(os.path.join(path, "20260412120000_create_users.sql"), "w", encoding="utf-8") as f:
        f.write("-- migrator:up\nSELECT 1;\n\n-- migrator:down\n")
    with open(os.path.join(path, "legacy.py"), "w", encoding="utf-8") as f:
        f.write("print('legacy')")
    with open(os.path.join(path, "notes.txt"), "w", encoding="utf-8") as f:
        f.write("ignore me")

    migrator = _make_migrator(migrations_dir=path)
    migrator.ch_client.execute.return_value = []

    assert migrator.get_unapplied_migration_names() == ["20260412120000_create_users.sql"]


def test_invalid_sql_migration_missing_down_marker(tmp_path: pytest.TempPathFactory) -> None:
    path = str(tmp_path / "migrations")
    os.makedirs(path)
    filepath = os.path.join(path, "20260412120000_create_users.sql")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("-- migrator:up\nSELECT 1;\n")

    migrator = _make_migrator(migrations_dir=path)
    migrator.ch_client.execute.return_value = []

    with pytest.raises(InvalidMigrationError, match="must contain exactly one"):
        migrator.get_migrations_for_apply()


def test_invalid_sql_migration_down_before_up(tmp_path: pytest.TempPathFactory) -> None:
    path = str(tmp_path / "migrations")
    os.makedirs(path)
    filepath = os.path.join(path, "20260412120000_create_users.sql")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("-- migrator:down\nDROP TABLE users;\n\n-- migrator:up\nSELECT 1;\n")

    migrator = _make_migrator(migrations_dir=path)
    migrator.ch_client.execute.return_value = []

    with pytest.raises(InvalidMigrationError, match="must declare '-- migrator:up' before '-- migrator:down'"):
        migrator.get_migrations_for_apply()
