from py_clickhouse_migrator.errors import MigrationParseError

_UP_MARKER = "-- migrator:up"
_DOWN_MARKER = "-- migrator:down"


def _trim_section(lines: list[str]) -> str:
    start = 0
    end = len(lines)
    while start < end and not lines[start].strip():
        start += 1
    while end > start and not lines[end - 1].strip():
        end -= 1
    return "\n".join(lines[start:end])


def parse_migration_file(filepath: str) -> tuple[str, str]:
    try:
        with open(filepath, encoding="utf-8") as f:
            content = f.read()
    except OSError as exc:
        raise MigrationParseError(f"Cannot load migration: {filepath}") from exc

    lines = content.splitlines()
    up_lines: list[int] = []
    down_lines: list[int] = []

    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped == _UP_MARKER:
            up_lines.append(index)
        elif stripped == _DOWN_MARKER:
            down_lines.append(index)

    if len(up_lines) != 1 or len(down_lines) != 1:
        raise MigrationParseError(
            f"Migration {filepath} must contain exactly one '{_UP_MARKER}' and one '{_DOWN_MARKER}' section."
        )

    up_index = up_lines[0]
    down_index = down_lines[0]
    if down_index <= up_index:
        raise MigrationParseError(f"Migration {filepath} must declare '{_UP_MARKER}' before '{_DOWN_MARKER}'.")

    up_sql = _trim_section(lines[up_index + 1 : down_index])
    rollback_sql = _trim_section(lines[down_index + 1 :])
    return up_sql, rollback_sql
