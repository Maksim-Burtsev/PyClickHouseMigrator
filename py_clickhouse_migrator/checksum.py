import hashlib

from py_clickhouse_migrator.migration_parser import MigrationSections, extract_migration_statements

SQL = str


def normalize_content(content: str) -> str:
    lines = [line.rstrip() for line in content.splitlines() if line.strip()]
    return "\n".join(lines)


def _serialize_statements(statements: list[SQL]) -> str:
    return "\0".join(normalize_content(statement) for statement in statements)


def compute_checksum_from_statements(up_statements: list[SQL], rollback_statements: list[SQL]) -> str:
    combined = _serialize_statements(up_statements) + "\0\0" + _serialize_statements(rollback_statements)
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()


def compute_checksum(up: str, rollback: str) -> str:
    statements = extract_migration_statements(MigrationSections(up=up, rollback=rollback))
    return compute_checksum_from_statements(statements.up, statements.rollback)
