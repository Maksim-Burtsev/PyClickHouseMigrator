"""Migration checksums.

The checksum format is persisted in ``db_migrations``: changing how it is computed makes every
already-applied migration look modified.
"""

import hashlib

from py_clickhouse_migrator.clickhouse import SQL
from py_clickhouse_migrator.migration_parser import MigrationSections, extract_migration_statements


def normalize_content(content: str) -> str:
    """Drop blank lines and trailing whitespace so formatting-only edits keep the checksum."""
    lines = [line.rstrip() for line in content.splitlines() if line.strip()]
    return "\n".join(lines)


def compute_checksum_from_statements(up_statements: list[SQL], rollback_statements: list[SQL]) -> str:
    """Return the SHA-256 hex digest of the normalized up and rollback statement blocks."""
    up_part = _serialize_statements(up_statements)
    rollback_part = _serialize_statements(rollback_statements)
    return hashlib.sha256(f"{up_part}\0\0{rollback_part}".encode()).hexdigest()


def compute_checksum(up: str, rollback: str) -> str:
    """Return the checksum of raw up and rollback sections, as stored for applied migrations."""
    statements = extract_migration_statements(MigrationSections(up=up, rollback=rollback))
    return compute_checksum_from_statements(statements.up, statements.rollback)


def _serialize_statements(statements: list[SQL]) -> str:
    return "\0".join(normalize_content(statement) for statement in statements)
