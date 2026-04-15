from pathlib import Path
from typing import Final, NamedTuple

from py_clickhouse_migrator.errors import MigrationParseError

_UP_MARKER: Final[str] = "-- migrator:up"
_DOWN_MARKER: Final[str] = "-- migrator:down"
_STATEMENT_MARKER: Final[str] = "-- @stmt"


class MigrationSections(NamedTuple):
    up: str
    rollback: str


class MigrationStatements(NamedTuple):
    up: list[str]
    rollback: list[str]


def _trim_section(lines: list[str]) -> str:
    start = 0
    end = len(lines)
    while start < end and not lines[start].strip():
        start += 1
    while end > start and not lines[end - 1].strip():
        end -= 1
    return "\n".join(lines[start:end])


def _load_migration_lines(filepath: str) -> list[str]:
    return Path(filepath).read_text(encoding="utf-8").splitlines()


def _find_marker_positions(lines: list[str], marker: str) -> list[int]:
    return [index for index, line in enumerate(lines) if line.strip() == marker]


def _find_section_bounds(lines: list[str]) -> tuple[int, int]:
    up_lines = _find_marker_positions(lines, _UP_MARKER)
    down_lines = _find_marker_positions(lines, _DOWN_MARKER)

    if len(up_lines) != 1 or len(down_lines) != 1:
        raise MigrationParseError(f"Must contain exactly one '{_UP_MARKER}' and one '{_DOWN_MARKER}' section.")

    up_index = up_lines[0]
    down_index = down_lines[0]
    if down_index <= up_index:
        raise MigrationParseError(f"Must declare '{_UP_MARKER}' before '{_DOWN_MARKER}'.")

    return up_index, down_index


def _extract_sections(lines: list[str]) -> MigrationSections:
    up_index, down_index = _find_section_bounds(lines)
    return MigrationSections(
        up=_trim_section(lines[up_index + 1 : down_index]),
        rollback=_trim_section(lines[down_index + 1 :]),
    )


def load_migration_sections(filepath: str) -> MigrationSections:
    try:
        lines = _load_migration_lines(filepath)
        return _extract_sections(lines)
    except OSError as exc:
        raise MigrationParseError(f"Cannot load migration: {filepath}") from exc
    except MigrationParseError as exc:
        raise MigrationParseError(f"Migration {filepath}: {exc}") from exc


def extract_migration_statements(sections: MigrationSections) -> MigrationStatements:
    up_statements = _extract_statement_blocks(sections.up.splitlines(), _UP_MARKER)
    rollback_statements = _extract_statement_blocks(sections.rollback.splitlines(), _DOWN_MARKER)

    if not up_statements:
        raise MigrationParseError(f"Must contain at least one non-empty '{_STATEMENT_MARKER}' block in '{_UP_MARKER}'.")

    return MigrationStatements(up=up_statements, rollback=rollback_statements)


def _extract_statement_blocks(lines: list[str], section_marker: str) -> list[str]:
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
