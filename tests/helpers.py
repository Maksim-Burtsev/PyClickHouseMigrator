from __future__ import annotations

import re

from clickhouse_driver import Client

from py_clickhouse_migrator.migrator import DEFAULT_MIGRATIONS_DIR, Migrator

MIGRATION_FILENAME_REGEX = re.compile(r"^\d{14}(?:_\w+)*\.py$")

TEST_MIGRATION_TEMPLATE: str = '''
def up() -> str:
    return """{up}"""


def rollback() -> str:
    return """{rollback}"""
'''


def table_exists(ch_client: Client, table_name: str) -> bool:
    result = ch_client.execute(
        "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = %(name)s",
        {"name": table_name},
    )
    return bool(result[0][0] > 0)


def create_test_migration(
    name: str,
    up: str,
    rollback: str,
    migrator: Migrator,
) -> str:
    filename: str = migrator.get_new_migration_filename(name)
    filepath: str = f"{DEFAULT_MIGRATIONS_DIR}/{filename}"
    with open(filepath, "w") as f:
        f.write(TEST_MIGRATION_TEMPLATE.format(up=up, rollback=rollback))
    return filename
