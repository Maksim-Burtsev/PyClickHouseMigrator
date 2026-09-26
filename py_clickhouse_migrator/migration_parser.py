"""Parser for the SQL migration file format.

A migration file has exactly one ``-- migrator:up`` and one ``-- migrator:down`` marker, in that
order. Each section holds zero or more ``-- @stmt`` blocks, and each block is one ClickHouse query.
"""

from pathlib import Path
from typing import Final, NamedTuple

from py_clickhouse_migrator.errors import MigrationParseError
from py_clickhouse_migrator.text import find_marker_lines, split_at_markers, trim_blank_lines

_UP_MARKER: Final[str] = "-- migrator:up"
_DOWN_MARKER: Final[str] = "-- migrator:down"
_STATEMENT_MARKER: Final[str] = "-- @stmt"


class MigrationSections(NamedTuple):
    """Raw text of the ``up`` and ``rollback`` sections of a migration file."""

    up: str
    rollback: str


class MigrationStatements(NamedTuple):
    """Statement blocks of the ``up`` and ``rollback`` sections, one query per item."""

    up: list[str]
    rollback: list[str]


def load_migration_sections(filepath: str) -> MigrationSections:
    """Read a migration file and split it into its ``up`` and ``rollback`` sections.

    Raises:
        MigrationParseError: the file cannot be read or its markers are missing, repeated, or out of order.

    """
    try:
        lines = Path(filepath).read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise MigrationParseError(f"Cannot load migration: {filepath}") from exc
    try:
        return _split_sections(lines)
    except MigrationParseError as exc:
        raise MigrationParseError(f"Migration {filepath}: {exc}") from exc


def extract_migration_statements(sections: MigrationSections) -> MigrationStatements:
    """Split both sections into ``-- @stmt`` blocks, skipping empty ones.

    Raises:
        MigrationParseError: SQL appears outside a ``-- @stmt`` block, or ``up`` has no statements.

    """
    up_statements = _extract_statement_blocks(sections.up.splitlines(), _UP_MARKER)
    rollback_statements = _extract_statement_blocks(sections.rollback.splitlines(), _DOWN_MARKER)

    if not up_statements:
        raise MigrationParseError(f"Must contain at least one non-empty '{_STATEMENT_MARKER}' block in '{_UP_MARKER}'.")

    return MigrationStatements(up=up_statements, rollback=rollback_statements)


def _split_sections(lines: list[str]) -> MigrationSections:
    up_lines = find_marker_lines(lines, _UP_MARKER)
    down_lines = find_marker_lines(lines, _DOWN_MARKER)
    if len(up_lines) != 1 or len(down_lines) != 1:
        raise MigrationParseError(f"Must contain exactly one '{_UP_MARKER}' and one '{_DOWN_MARKER}' section.")

    up_index, down_index = up_lines[0], down_lines[0]
    if down_index <= up_index:
        raise MigrationParseError(f"Must declare '{_UP_MARKER}' before '{_DOWN_MARKER}'.")

    return MigrationSections(
        up=trim_blank_lines(lines[up_index + 1 : down_index]),
        rollback=trim_blank_lines(lines[down_index + 1 :]),
    )


def _extract_statement_blocks(lines: list[str], section_marker: str) -> list[str]:
    preamble, blocks = split_at_markers(lines, _STATEMENT_MARKER)
    if any(line.strip() for line in preamble):
        raise MigrationParseError(f"Non-empty content in '{section_marker}' outside '{_STATEMENT_MARKER}' blocks.")
    statements = map(trim_blank_lines, blocks)
    return [statement for statement in statements if statement]
