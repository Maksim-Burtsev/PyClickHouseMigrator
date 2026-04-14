from pathlib import Path
from typing import Final

from py_clickhouse_migrator.errors import MigrationParseError

_UP_MARKER: Final[str] = "-- migrator:up"
_DOWN_MARKER: Final[str] = "-- migrator:down"
_STATEMENT_MARKER: Final[str] = "-- @stmt"


def _trim_section(lines: list[str]) -> str:
    start = 0
    end = len(lines)
    while start < end and not lines[start].strip():
        start += 1
    while end > start and not lines[end - 1].strip():
        end -= 1
    return "\n".join(lines[start:end])


def _read_migration_file(filepath: str) -> str:
    try:
        return Path(filepath).read_text(encoding="utf-8")
    except OSError as exc:
        raise MigrationParseError(f"Cannot load migration: {filepath}") from exc


def _find_marker_indexes(lines: list[str], marker: str) -> list[int]:
    return [index for index, line in enumerate(lines) if line.strip() == marker]


def _find_section_indexes(lines: list[str], filepath: str) -> tuple[int, int]:
    up_lines = _find_marker_indexes(lines, _UP_MARKER)
    down_lines = _find_marker_indexes(lines, _DOWN_MARKER)

    if len(up_lines) != 1 or len(down_lines) != 1:
        raise MigrationParseError(
            f"Migration {filepath} must contain exactly one '{_UP_MARKER}' and one '{_DOWN_MARKER}' section."
        )

    up_index = up_lines[0]
    down_index = down_lines[0]
    if down_index <= up_index:
        raise MigrationParseError(f"Migration {filepath} must declare '{_UP_MARKER}' before '{_DOWN_MARKER}'.")

    return up_index, down_index


def parse_migration_file(filepath: str) -> tuple[str, str]:
    lines = _read_migration_file(filepath).splitlines()
    up_index, down_index = _find_section_indexes(lines, filepath)

    up_sql = _trim_section(lines[up_index + 1 : down_index])
    rollback_sql = _trim_section(lines[down_index + 1 :])
    return up_sql, rollback_sql


def _parse_statement_blocks(lines: list[str], section_marker: str) -> list[str]:
    statements: list[str] = []
    current_block: list[str] | None = None

    for line in lines:
        stripped_line = line.strip()
        if stripped_line == _STATEMENT_MARKER:
            if current_block is not None:
                statement = _trim_section(current_block)
                if statement:
                    statements.append(statement)
            current_block = []
            continue

        if current_block is None:
            if stripped_line:
                raise MigrationParseError(
                    f"Non-empty content in '{section_marker}' outside '{_STATEMENT_MARKER}' blocks."
                )
            continue

        current_block.append(line)

    if current_block is not None:
        statement = _trim_section(current_block)
        if statement:
            statements.append(statement)

    return statements


def parse_migration_statements(filepath: str) -> tuple[list[str], list[str]]:
    lines = _read_migration_file(filepath).splitlines()
    up_index, down_index = _find_section_indexes(lines, filepath)

    try:
        up_statements = _parse_statement_blocks(lines[up_index + 1 : down_index], _UP_MARKER)
        rollback_statements = _parse_statement_blocks(lines[down_index + 1 :], _DOWN_MARKER)
    except MigrationParseError as exc:
        raise MigrationParseError(f"Migration {filepath}: {exc}") from exc

    if not up_statements:
        raise MigrationParseError(
            f"Migration {filepath} must contain at least one non-empty '{_STATEMENT_MARKER}' block in '{_UP_MARKER}'."
        )

    return up_statements, rollback_statements
