from __future__ import annotations

import os
import re
from collections.abc import Sequence

from clickhouse_driver import Client

from py_clickhouse_migrator.migrator import DEFAULT_MIGRATIONS_DIR, make_migration_filename

MIGRATION_FILENAME_REGEX: re.Pattern[str] = re.compile(r"^\d{14}(?:_\w+)*\.sql$")

TEST_MIGRATION_TEMPLATE: str = """-- migrator:up
{up}

-- migrator:down
{rollback}
"""


def render_test_migration_section(statements: str | Sequence[str]) -> str:
    if isinstance(statements, str):
        return f"-- @stmt\n{statements}"
    return "\n\n".join(f"-- @stmt\n{statement}" for statement in statements)


def render_test_migration_content(up: str | Sequence[str], rollback: str | Sequence[str]) -> str:
    return TEST_MIGRATION_TEMPLATE.format(
        up=render_test_migration_section(up),
        rollback=render_test_migration_section(rollback),
    )


def table_exists(ch_client: Client, table_name: str) -> bool:
    result = ch_client.execute(
        "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = %(name)s",
        {"name": table_name},
    )
    return bool(result[0][0] > 0)


def get_engine(ch_client: Client, table_name: str) -> str:
    rows = ch_client.execute(
        "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = %(name)s",
        {"name": table_name},
    )
    return rows[0][0] if rows else ""


def create_test_migration(
    name: str,
    up: str | Sequence[str],
    rollback: str | Sequence[str],
    migrations_dir: str = DEFAULT_MIGRATIONS_DIR,
) -> str:
    filename = make_migration_filename(name)
    filepath = os.path.join(migrations_dir, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(render_test_migration_content(up, rollback))
    return filename
