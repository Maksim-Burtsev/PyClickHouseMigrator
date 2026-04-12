from pathlib import Path
from typing import Final

from py_clickhouse_migrator.errors import MigrationParseError

_UP_MARKER: Final[str] = "-- migrator:up"
_DOWN_MARKER: Final[str] = "-- migrator:down"


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
