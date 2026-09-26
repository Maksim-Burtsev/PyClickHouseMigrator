"""The migration model shared by the migrator, its reports, and its storage."""

from dataclasses import dataclass
from enum import StrEnum
from functools import cached_property

from py_clickhouse_migrator.checksum import compute_checksum_from_statements
from py_clickhouse_migrator.clickhouse import SQL
from py_clickhouse_migrator.errors import InvalidMigrationError, MigrationParseError
from py_clickhouse_migrator.migration_parser import (
    MigrationSections,
    MigrationStatements,
    extract_migration_statements,
    load_migration_sections,
)


class MigrationDirection(StrEnum):
    """Which section of a migration runs: ``up`` applies it, ``rollback`` reverts it."""

    UP = "up"
    ROLLBACK = "rollback"


class MigrationKind(StrEnum):
    """Value of the ``kind`` column in ``db_migrations``."""

    MIGRATION = "migration"
    BASELINE = "baseline"


@dataclass
class Migration:
    """A migration file name with the raw SQL of its ``up`` and ``rollback`` sections.

    Statement blocks are parsed on first access; parse errors surface as ``InvalidMigrationError``.
    """

    name: str
    up: SQL
    rollback: SQL
    kind: str = MigrationKind.MIGRATION

    @property
    def up_statements(self) -> list[SQL]:
        """Statement blocks of the ``up`` section."""
        return self._statements.up

    @property
    def rollback_statements(self) -> list[SQL]:
        """Statement blocks of the ``rollback`` section."""
        return self._statements.rollback

    @property
    def is_baseline(self) -> bool:
        """Whether the migration was recorded by ``migrator baseline`` without being executed."""
        return self.kind == MigrationKind.BASELINE

    @property
    def checksum(self) -> str:
        """Checksum of the statement blocks, as stored in ``db_migrations``."""
        return compute_checksum_from_statements(self.up_statements, self.rollback_statements)

    def sql(self, direction: MigrationDirection) -> SQL:
        """Raw text of the section that runs in ``direction``."""
        return self.up if direction is MigrationDirection.UP else self.rollback

    def statements(self, direction: MigrationDirection) -> list[SQL]:
        """Statement blocks of the section that runs in ``direction``."""
        return self.up_statements if direction is MigrationDirection.UP else self.rollback_statements

    @cached_property
    def _statements(self) -> MigrationStatements:
        try:
            return extract_migration_statements(MigrationSections(up=self.up, rollback=self.rollback))
        except MigrationParseError as exc:
            raise InvalidMigrationError(f"Migration {self.name}: {exc}") from exc


def load_migration(migrations_dir: str, name: str) -> Migration:
    """Read migration ``name`` from ``migrations_dir``.

    Raises:
        InvalidMigrationError: the file cannot be read or parsed.

    """
    try:
        sections = load_migration_sections(f"{migrations_dir}/{name}")
    except MigrationParseError as exc:
        raise InvalidMigrationError(str(exc)) from exc
    return Migration(name=name, up=sections.up, rollback=sections.rollback)
