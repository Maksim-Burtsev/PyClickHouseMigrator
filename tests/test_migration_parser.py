from __future__ import annotations

from pathlib import Path

import pytest

from py_clickhouse_migrator.errors import MigrationParseError
from py_clickhouse_migrator.migration_parser import (
    _find_marker_indexes,
    _find_section_indexes,
    _read_migration_file,
    _trim_section,
    parse_migration_file,
)


def test_trim_section_removes_blank_edges_and_preserves_inner_blank_lines() -> None:
    lines = [
        "",
        "   ",
        "CREATE TABLE users (id UInt64)",
        "",
        "ORDER BY id",
        "   ",
        "",
    ]

    result = _trim_section(lines)

    assert result == "CREATE TABLE users (id UInt64)\n\nORDER BY id"


def test_trim_section_returns_empty_string_for_only_blank_lines() -> None:
    assert _trim_section(["", "   ", "\t"]) == ""


def test_read_migration_file_reads_utf8(tmp_path: Path) -> None:
    filepath = tmp_path / "migration.sql"
    filepath.write_text("-- migrator:up\nSELECT 1;\n-- migrator:down\n", encoding="utf-8")

    content = _read_migration_file(str(filepath))

    assert "SELECT 1;" in content


def test_read_migration_file_raises_for_missing_file(tmp_path: Path) -> None:
    filepath = tmp_path / "missing.sql"

    with pytest.raises(MigrationParseError, match="Cannot load migration"):
        _read_migration_file(str(filepath))


def test_find_marker_indexes_matches_stripped_marker_lines() -> None:
    lines = [
        "  -- migrator:up  ",
        "SELECT 1;",
        "-- migrator:down",
        "-- not-a-marker:up",
    ]

    up_indexes = _find_marker_indexes(lines, "-- migrator:up")
    down_indexes = _find_marker_indexes(lines, "-- migrator:down")

    assert up_indexes == [0]
    assert down_indexes == [2]


def test_find_section_indexes_returns_up_and_down_positions() -> None:
    lines = [
        "-- migrator:up",
        "SELECT 1;",
        "-- migrator:down",
        "DROP TABLE users;",
    ]

    up_index, down_index = _find_section_indexes(lines, "migration.sql")

    assert (up_index, down_index) == (0, 2)


@pytest.mark.parametrize(
    "lines",
    [
        [],
        ["-- migrator:up", "SELECT 1;"],
        ["-- migrator:down", "DROP TABLE users;"],
        ["-- migrator:up", "-- migrator:up", "-- migrator:down"],
        ["-- migrator:up", "-- migrator:down", "-- migrator:down"],
    ],
)
def test_find_section_indexes_requires_exactly_one_up_and_one_down(lines: list[str]) -> None:
    with pytest.raises(MigrationParseError, match="must contain exactly one"):
        _find_section_indexes(lines, "migration.sql")


def test_find_section_indexes_requires_up_before_down() -> None:
    lines = [
        "-- migrator:down",
        "DROP TABLE users;",
        "-- migrator:up",
        "SELECT 1;",
    ]

    with pytest.raises(MigrationParseError, match="must declare '-- migrator:up' before '-- migrator:down'"):
        _find_section_indexes(lines, "migration.sql")


def test_parse_migration_file_parses_sections_and_trims_blank_edges(tmp_path: Path) -> None:
    filepath = tmp_path / "20260412120000_create_users.sql"
    filepath.write_text(
        "\n"
        "  -- migrator:up  \n"
        "\n"
        "CREATE TABLE users (id UInt64)\n"
        "\n"
        "ORDER BY id;\n"
        "\n"
        "-- migrator:down\n"
        "\n"
        "DROP TABLE IF EXISTS users;\n"
        "\n",
        encoding="utf-8",
    )

    up_sql, rollback_sql = parse_migration_file(str(filepath))

    assert up_sql == "CREATE TABLE users (id UInt64)\n\nORDER BY id;"
    assert rollback_sql == "DROP TABLE IF EXISTS users;"


def test_parse_migration_file_allows_empty_sections(tmp_path: Path) -> None:
    filepath = tmp_path / "20260412120000_empty.sql"
    filepath.write_text("-- migrator:up\n\n-- migrator:down\n", encoding="utf-8")

    up_sql, rollback_sql = parse_migration_file(str(filepath))

    assert up_sql == ""
    assert rollback_sql == ""


def test_parse_migration_file_raises_for_duplicate_up_marker(tmp_path: Path) -> None:
    filepath = tmp_path / "20260412120000_bad.sql"
    filepath.write_text(
        "-- migrator:up\nSELECT 1;\n-- migrator:up\nSELECT 2;\n-- migrator:down\nDROP TABLE users;\n",
        encoding="utf-8",
    )

    with pytest.raises(MigrationParseError, match="must contain exactly one"):
        parse_migration_file(str(filepath))


def test_parse_migration_file_raises_for_duplicate_down_marker(tmp_path: Path) -> None:
    filepath = tmp_path / "20260412120000_bad.sql"
    filepath.write_text(
        "-- migrator:up\nSELECT 1;\n-- migrator:down\nDROP TABLE users;\n-- migrator:down\nSELECT 1;\n",
        encoding="utf-8",
    )

    with pytest.raises(MigrationParseError, match="must contain exactly one"):
        parse_migration_file(str(filepath))


def test_parse_migration_file_raises_when_markers_are_missing(tmp_path: Path) -> None:
    filepath = tmp_path / "20260412120000_bad.sql"
    filepath.write_text("SELECT 1;\nDROP TABLE users;\n", encoding="utf-8")

    with pytest.raises(MigrationParseError, match="must contain exactly one"):
        parse_migration_file(str(filepath))


def test_parse_migration_file_raises_when_down_is_before_up(tmp_path: Path) -> None:
    filepath = tmp_path / "20260412120000_bad.sql"
    filepath.write_text("-- migrator:down\nDROP TABLE users;\n-- migrator:up\nSELECT 1;\n", encoding="utf-8")

    with pytest.raises(MigrationParseError, match="must declare '-- migrator:up' before '-- migrator:down'"):
        parse_migration_file(str(filepath))
