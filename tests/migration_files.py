"""Write migration files for tests."""

import re
from collections.abc import Sequence
from pathlib import Path

from py_clickhouse_migrator.migrator import DEFAULT_MIGRATIONS_DIR, make_migration_filename

MIGRATION_FILENAME_REGEX: re.Pattern[str] = re.compile(r"^\d{14}(?:_\w+)*\.sql$")

TEST_MIGRATION_TEMPLATE: str = """-- migrator:up
{up}

-- migrator:down
{rollback}
"""

Statements = str | Sequence[str]


def render_test_migration_section(statements: Statements) -> str:
    """Render one section with a ``-- @stmt`` marker before each statement."""
    if isinstance(statements, str):
        return f"-- @stmt\n{statements}"
    return "\n\n".join(f"-- @stmt\n{statement}" for statement in statements)


def render_test_migration_content(up: Statements, rollback: Statements) -> str:
    """Render a complete migration file with the given up and rollback statements."""
    return TEST_MIGRATION_TEMPLATE.format(
        up=render_test_migration_section(up),
        rollback=render_test_migration_section(rollback),
    )


def write_test_migration(
    filename: str,
    up: Statements,
    rollback: Statements,
    migrations_dir: str = DEFAULT_MIGRATIONS_DIR,
) -> None:
    """Write migration ``filename``, replacing its content if it already exists."""
    Path(migrations_dir, filename).write_text(render_test_migration_content(up, rollback), encoding="utf-8")


def create_test_migration(
    name: str,
    up: Statements,
    rollback: Statements,
    migrations_dir: str = DEFAULT_MIGRATIONS_DIR,
) -> str:
    """Create a migration file named like ``migrator new`` would name it, and return that file name."""
    filename = make_migration_filename(name)
    write_test_migration(filename, up, rollback, migrations_dir)
    return filename
