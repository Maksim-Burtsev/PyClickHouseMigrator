"""Tests for splitting migration files into sections and ``-- @stmt`` blocks."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pytest

from py_clickhouse_migrator.errors import MigrationParseError
from py_clickhouse_migrator.migration_parser import (
    MigrationSections,
    MigrationStatements,
    extract_migration_statements,
    load_migration_sections,
)

MIGRATION_FILENAME = "20260412120000_create_users.sql"
MISSING_FILENAME = "missing.sql"
SINGLE_UP_STATEMENT = ("-- @stmt", "SELECT 1;")
MARKER_COUNT_ERROR = "Must contain exactly one '-- migrator:up' and one '-- migrator:down' section"
MARKER_ORDER_ERROR = "Must declare '-- migrator:up' before '-- migrator:down'"
EMPTY_UP_ERROR = "Must contain at least one non-empty '-- @stmt' block in '-- migrator:up'"
OUTSIDE_BLOCK_ERROR = "Non-empty content in '-- migrator:up' outside '-- @stmt' blocks"


def _write_migration(tmp_path: Path, lines: list[str]) -> str:
    filepath = tmp_path / MIGRATION_FILENAME
    filepath.write_text("".join(f"{line}\n" for line in lines), encoding="utf-8")
    return str(filepath)


def _load_sections(tmp_path: Path, lines: list[str]) -> MigrationSections:
    return load_migration_sections(_write_migration(tmp_path, lines))


def _extract_statements(up: Sequence[str], rollback: Sequence[str]) -> MigrationStatements:
    sections = MigrationSections(up="\n".join(up), rollback="\n".join(rollback))
    return extract_migration_statements(sections)


def test_load_reads_file_as_utf8(tmp_path: Path) -> None:
    """The file is decoded as UTF-8 regardless of the locale."""
    sections = _load_sections(tmp_path, ["-- migrator:up", "SELECT 'café';", "-- migrator:down"])

    assert sections == MigrationSections(up="SELECT 'café';", rollback="")


def test_load_reports_missing_file(tmp_path: Path) -> None:
    """A file that cannot be read is reported as a parse error instead of a bare OSError."""
    with pytest.raises(MigrationParseError, match="Cannot load migration"):
        load_migration_sections(str(tmp_path / MISSING_FILENAME))


def test_load_keeps_os_error_of_missing_file(tmp_path: Path) -> None:
    """The OSError raised while reading stays attached as the cause of the parse error."""
    with pytest.raises(MigrationParseError, match="Cannot load migration") as exc_info:
        load_migration_sections(str(tmp_path / MISSING_FILENAME))

    assert isinstance(exc_info.value.__cause__, OSError)


def test_load_splits_file_at_markers(tmp_path: Path) -> None:
    """Lines between the markers form ``up``, and lines after the down marker form ``rollback``."""
    sections = _load_sections(tmp_path, ["-- migrator:up", "SELECT 1;", "-- migrator:down", "DROP TABLE users;"])

    assert sections == MigrationSections(up="SELECT 1;", rollback="DROP TABLE users;")


@pytest.mark.parametrize(
    "lines",
    [
        [],
        ["-- migrator:up", "SELECT 1;"],
        ["-- migrator:down", "DROP TABLE users;"],
        ["-- migrator:up", "-- migrator:up", "-- migrator:down"],
        ["-- migrator:up", "-- migrator:down", "-- migrator:down"],
        ["-- migrator:up", "SELECT 1;", "-- migrator:up", "SELECT 2;", "-- migrator:down", "DROP TABLE users;"],
        ["-- migrator:up", "SELECT 1;", "-- migrator:down", "DROP TABLE users;", "-- migrator:down", "SELECT 1;"],
        ["SELECT 1;", "DROP TABLE users;"],
    ],
    ids=[
        "empty_file",
        "no_down_marker",
        "no_up_marker",
        "two_up_markers",
        "two_down_markers",
        "two_up_sections",
        "two_down_sections",
        "no_markers",
    ],
)
def test_load_requires_one_up_and_one_down(tmp_path: Path, lines: list[str]) -> None:
    """A file with a missing or repeated section marker is rejected."""
    with pytest.raises(MigrationParseError, match=MARKER_COUNT_ERROR):
        _load_sections(tmp_path, lines)


@pytest.mark.parametrize(
    "lines",
    [
        ["-- migrator:down", "DROP TABLE users;", "-- migrator:up", "SELECT 1;"],
        ["-- migrator:down", "-- migrator:up"],
    ],
    ids=["sections_with_sql", "empty_sections"],
)
def test_load_requires_up_before_down(tmp_path: Path, lines: list[str]) -> None:
    """A file that declares the down marker before the up marker is rejected."""
    with pytest.raises(MigrationParseError, match=MARKER_ORDER_ERROR):
        _load_sections(tmp_path, lines)


def test_load_trims_blank_edges_of_sections(tmp_path: Path) -> None:
    """Sections lose blank edge lines but keep inner blank lines; indented markers still count."""
    lines = [
        "",
        "  -- migrator:up  ",
        "",
        "CREATE TABLE users (id UInt64)",
        "",
        "ORDER BY id;",
        "",
        "-- migrator:down",
        "",
        "DROP TABLE IF EXISTS users;",
        "",
    ]

    assert _load_sections(tmp_path, lines) == MigrationSections(
        up="CREATE TABLE users (id UInt64)\n\nORDER BY id;",
        rollback="DROP TABLE IF EXISTS users;",
    )


def test_load_allows_empty_sections(tmp_path: Path) -> None:
    """Loading accepts empty sections; requiring statements is left to statement extraction."""
    sections = _load_sections(tmp_path, ["-- migrator:up", "", "-- migrator:down"])

    assert sections == MigrationSections(up="", rollback="")


def test_extract_trims_edges_of_each_block() -> None:
    """Each block loses blank edge lines but keeps inner blank lines, SQL comments, and indentation."""
    up = ["-- @stmt", "", "SELECT 1;", "", "-- comment inside statement", "-- @stmt", "  ", "SELECT", "    2;", ""]

    assert _extract_statements(up, rollback=[]) == MigrationStatements(
        up=["SELECT 1;\n\n-- comment inside statement", "SELECT\n    2;"],
        rollback=[],
    )


def test_extract_allows_blank_rollback_section() -> None:
    """A rollback section holding only whitespace yields no rollback statements."""
    statements = _extract_statements(SINGLE_UP_STATEMENT, rollback=["", "   "])

    assert statements == MigrationStatements(up=["SELECT 1;"], rollback=[])


def test_extract_skips_empty_rollback_blocks() -> None:
    """Rollback blocks holding only whitespace are dropped instead of run as empty queries."""
    statements = _extract_statements(SINGLE_UP_STATEMENT, rollback=["-- @stmt", "", "   ", "-- @stmt", ""])

    assert statements == MigrationStatements(up=["SELECT 1;"], rollback=[])


def test_extract_skips_empty_up_block() -> None:
    """An empty ``up`` block is skipped, not rejected, when another ``up`` block holds SQL."""
    statements = _extract_statements(["-- @stmt", "", "   ", *SINGLE_UP_STATEMENT], rollback=[])

    assert statements == MigrationStatements(up=["SELECT 1;"], rollback=[])


@pytest.mark.parametrize(
    "up",
    [
        ["SELECT 1;"],
        ["-- some comment", "-- @stmt", "SELECT 1;"],
        ["", "SELECT 1;"],
    ],
    ids=["sql_without_marker", "comment_before_marker", "sql_after_blank_line"],
)
def test_extract_rejects_content_outside_blocks(up: list[str]) -> None:
    """Anything but whitespace before the first ``-- @stmt`` marker is rejected, naming the section."""
    with pytest.raises(MigrationParseError, match=OUTSIDE_BLOCK_ERROR):
        _extract_statements(up, rollback=[])


@pytest.mark.parametrize(
    "lines",
    [
        ["-- migrator:up", "-- @stmt", "", "-- migrator:down"],
        ["-- migrator:up", "", "-- migrator:down"],
    ],
    ids=["only_empty_blocks", "empty_section"],
)
def test_extract_requires_up_statement(tmp_path: Path, lines: list[str]) -> None:
    """A migration whose ``up`` section holds no SQL is rejected."""
    sections = _load_sections(tmp_path, lines)

    with pytest.raises(MigrationParseError, match=EMPTY_UP_ERROR):
        extract_migration_statements(sections)


def test_extract_splits_file_into_statements(tmp_path: Path) -> None:
    """A complete migration file yields one query per ``-- @stmt`` block in each section."""
    lines = [
        "-- migrator:up",
        "",
        "-- @stmt",
        "CREATE TABLE users (",
        "    id UInt64",
        ") ENGINE = MergeTree()",
        "ORDER BY id;",
        "",
        "-- @stmt",
        "-- regular SQL comments inside the statement are preserved",
        "INSERT INTO users VALUES (1);",
        "",
        "-- migrator:down",
        "-- @stmt",
        "DROP TABLE IF EXISTS users;",
    ]

    assert extract_migration_statements(_load_sections(tmp_path, lines)) == MigrationStatements(
        up=[
            "CREATE TABLE users (\n    id UInt64\n) ENGINE = MergeTree()\nORDER BY id;",
            "-- regular SQL comments inside the statement are preserved\nINSERT INTO users VALUES (1);",
        ],
        rollback=["DROP TABLE IF EXISTS users;"],
    )


def test_extract_allows_file_without_rollback(tmp_path: Path) -> None:
    """A migration file with an empty down section yields no rollback statements."""
    sections = _load_sections(tmp_path, ["-- migrator:up", "-- @stmt", "SELECT 1;", "", "-- migrator:down"])

    assert extract_migration_statements(sections) == MigrationStatements(up=["SELECT 1;"], rollback=[])


def test_extract_rejects_file_sql_outside_blocks(tmp_path: Path) -> None:
    """A migration file with SQL outside ``-- @stmt`` blocks is rejected at extraction."""
    sections = _load_sections(tmp_path, ["-- migrator:up", "SELECT 1;", "-- migrator:down", "-- @stmt", "SELECT 2;"])

    with pytest.raises(MigrationParseError, match=OUTSIDE_BLOCK_ERROR):
        extract_migration_statements(sections)
