import datetime as dt
import hashlib
import importlib.util
import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Final, NamedTuple

import click
from clickhouse_driver import Client
from clickhouse_driver.errors import ServerException

logger = logging.getLogger("py_clickhouse_migrator")

SQL = str
ClickHouseSettings = dict[str, str | int]

_SQL_IDENTIFIER_RE: Final[re.Pattern[str]] = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*\Z")  # cluster name, db name
_UNKNOWN_DATABASE_CODE: Final[int] = 81
_MIGRATION_NAME_RE: Final[re.Pattern[str]] = re.compile(r"[a-zA-Z0-9_]+\Z")  # migration name suffix in filename

_CLUSTER_SETTINGS: ClickHouseSettings = {
    "insert_quorum": "auto",
    "select_sequential_consistency": 1,
}


MIGRATION_TEMPLATE: str = '''
def up() -> str:
    return """
    """


def rollback() -> str:
    return """
    """
'''
DEFAULT_MIGRATIONS_DIR: str = "./db/migrations"


class ClickHouseServerIsNotHealthyError(Exception): ...


class MigrationDirectoryNotFoundError(Exception): ...


class InvalidMigrationError(Exception): ...


class MissingDatabaseUrlError(Exception): ...


class DatabaseNotFoundError(Exception): ...


class ChecksumMismatchError(Exception): ...


class ChecksumMismatch(NamedTuple):
    name: str
    stored: str
    actual: str


class ShowMigrationsResult(NamedTuple):
    output: str
    warning: str


def normalize_content(content: str) -> str:
    lines = [line.rstrip() for line in content.splitlines()]
    return "\n".join(lines).strip()


def compute_checksum(content: str) -> str:
    return hashlib.sha256(normalize_content(content).encode("utf-8")).hexdigest()


@dataclass
class Migration:
    name: str
    up: SQL
    rollback: SQL
    checksum: str = field(default="")


def create_migrations_dir(migrations_dir: str = DEFAULT_MIGRATIONS_DIR) -> None:
    """Create the migrations directory if it doesn't exist."""
    os.makedirs(migrations_dir, exist_ok=True)
    logger.info("Migrations directory %s successfully initialized.", migrations_dir)


def make_migration_filename(name: str = "") -> str:
    """Generate a timestamped migration filename."""
    if name and not _MIGRATION_NAME_RE.match(name):
        raise ValueError(f"Invalid migration name: '{name}'. Use only letters, digits, and underscores.")
    filename = dt.datetime.now().strftime("%Y%m%d%H%M%S")
    if name:
        filename += f"_{name}"
    filename += ".py"
    return filename


def create_migration_file(migrations_dir: str = DEFAULT_MIGRATIONS_DIR, name: str = "") -> str:
    """Create a new migration file from template. Returns the filepath."""
    if not name:
        logger.warning("Migration name is recommended: py-clickhouse-migrator new <name>")

    filename = make_migration_filename(name)
    filepath = os.path.join(migrations_dir, filename)
    try:
        with open(filepath, "w") as f:
            f.write(MIGRATION_TEMPLATE)
    except FileNotFoundError:
        raise MigrationDirectoryNotFoundError(
            f"Migration directory {migrations_dir} not found.\nMake sure you run 'init' first."
        ) from None

    logger.info("Migration %s has been created.", filepath)
    return filepath


class Migrator(object):
    """ClickHouse schema migration manager.

    Args:
        cluster: ClickHouse cluster name for replicated operations.
        connect_retries: Number of connection retry attempts on startup.
        connect_retries_interval: Seconds between connection retries.

    """

    def __init__(
        self,
        database_url: str = "",
        migrations_dir: str = DEFAULT_MIGRATIONS_DIR,
        cluster: str = "",
        connect_retries: int = 0,
        connect_retries_interval: int = 1,
    ) -> None:
        if not database_url:
            raise MissingDatabaseUrlError(
                "ClickHouse url was not provided.\nUse --url option or set CLICKHOUSE_MIGRATE_URL environment variable."
            )
        self.database_url: str = database_url
        self.migrations_dir: str = migrations_dir
        self.cluster: str = cluster
        if self.cluster and not _SQL_IDENTIFIER_RE.match(self.cluster):
            raise ValueError(f"Invalid cluster name: '{self.cluster}'. Use only letters, digits, and underscores.")
        self._connect_retries: int = connect_retries
        self._connect_retries_interval: int = connect_retries_interval
        self._settings: ClickHouseSettings = _CLUSTER_SETTINGS.copy() if self.cluster else {}
        self.ch_client: Client = Client.from_url(database_url)
        self.health_check()
        self.check_migrations_table()

    def check_migrations_table(self) -> None:
        on_cluster = f"ON CLUSTER {self.cluster}" if self.cluster else ""
        engine = (
            "ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')" if self.cluster else "MergeTree()"
        )
        migrator_table: SQL = f"""
        CREATE TABLE IF NOT EXISTS db_migrations {on_cluster} (
            name String,
            up String,
            rollback String,
            dt DateTime64 DEFAULT now(),
            checksum String DEFAULT ''
        )
        Engine {engine}
        ORDER BY dt
        """
        self.ch_client.execute(migrator_table, settings=self._settings)

    def health_check(self) -> None:
        for attempt in range(self._connect_retries + 1):
            try:
                self.ch_client.execute("SELECT 1")
                return
            except Exception as exc:
                if isinstance(exc, ServerException) and exc.code == _UNKNOWN_DATABASE_CODE:
                    db_name = self.get_db_name()
                    raise DatabaseNotFoundError(
                        f"Database '{db_name}' does not exist.\n"
                        f"Create it manually before running migrations:\n"
                        f"  CREATE DATABASE {db_name}"
                    ) from exc
                if attempt == self._connect_retries:
                    raise ClickHouseServerIsNotHealthyError(f"ClickHouse server is not healthy: {exc}.") from exc
                logger.warning(
                    "Connection attempt %d/%d failed, retrying in %ds",
                    attempt + 1,
                    self._connect_retries,
                    self._connect_retries_interval,
                )
                time.sleep(self._connect_retries_interval)

    def get_db_name(self) -> str:
        db_name: str = self.database_url.rsplit("/", 1)[-1]
        if "?" in db_name:
            db_name = db_name[: db_name.find("?")]
        return db_name

    def check_integrity(self, allow_dirty: bool = False) -> None:
        mismatches = self.validate_checksums()
        if not mismatches:
            return
        if allow_dirty:
            logger.warning("Checksum mismatches found but --allow-dirty is set, continuing.")
            return
        details = "\n".join(
            f"  {name}: file missing" if not actual else f"  {name}: stored={stored[:12]}... actual={actual[:12]}..."
            for name, stored, actual in mismatches
        )
        raise ChecksumMismatchError(
            f"Checksum mismatch for applied migrations:\n{details}\n\n"
            "Run 'py-clickhouse-migrator repair' to update checksums, or use --allow-dirty to skip this check."
        )

    def up(self, n: int | None = None, dry_run: bool = False, allow_dirty: bool = False) -> None:
        """Apply pending migrations.

        Args:
            n: Maximum number of migrations to apply. All pending if None.
            dry_run: Print SQL without executing.
            allow_dirty: Skip checksum validation for modified files.

        """
        self.check_integrity(allow_dirty=allow_dirty)
        migrations: list[Migration] = self.get_migrations_for_apply(n)
        if not migrations:
            logger.info("There are no migrations to apply.")
        for migration in migrations:
            if dry_run:
                logger.info("-- %s (up)", migration.name)
                logger.info("%s\n", migration.up.strip())
                continue
            self.apply_migration(query=migration.up)
            self.save_applied_migration(
                name=migration.name,
                up=migration.up,
                rollback=migration.rollback,
                checksum=migration.checksum,
            )
            logger.info("%s applied [✔]", migration.name)

    def rollback(self, number: int = 1, dry_run: bool = False) -> None:
        """Rollback applied migrations in reverse order."""
        migrations: list[Migration] = self.get_migrations_for_rollback(number=number)
        for migration in migrations:
            if dry_run:
                logger.info("-- %s (rollback)", migration.name)
                logger.info("%s\n", migration.rollback.strip())
                continue
            self.apply_migration(
                query=migration.rollback,
            )
            self.delete_migration(
                name=migration.name,
            )
            logger.info("%s rolled back [✔].", migration.name)

    def apply_migration(self, query: SQL) -> None:
        queries: list[SQL] = query.split(";")
        for query in queries:
            query = query.strip("\n ")
            if not query:
                continue
            try:
                self.ch_client.execute(query)
            except ServerException as exc:
                raise InvalidMigrationError(f"Query {query} raise error: {exc}") from exc

    def get_migrations_for_apply(self, number: int | None = None) -> list[Migration]:
        filenames: list[str] = self.get_unapplied_migration_names()

        if number:
            filenames = filenames[:number]

        result: list[Migration] = []
        for filename in filenames:
            filepath = f"{self.migrations_dir}/{filename}"
            spec = importlib.util.spec_from_file_location(filename, filepath)
            if spec is None or spec.loader is None:
                raise InvalidMigrationError(f"Cannot load migration: {filepath}")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            up: str = module.up()
            if not up.strip():
                logger.warning("Skip empty migration: %s", filename)
                continue
            with open(filepath) as f:
                file_content = f.read()
            result.append(
                Migration(
                    name=filename,
                    up=up,
                    rollback=module.rollback(),
                    checksum=compute_checksum(file_content),
                )
            )

        return result

    def get_unapplied_migration_names(self) -> list[str]:
        filenames: list[str] = [file for file in os.listdir(self.migrations_dir) if file.endswith(".py")]
        applied_migrations: list[str] = self.get_applied_migrations_names()
        return sorted(list(set(filenames) - set(applied_migrations)))

    def get_applied_migrations_names(self) -> list[str]:
        return [
            row[0]
            for row in self.ch_client.execute("SELECT name FROM db_migrations ORDER BY dt", settings=self._settings)
        ]

    def get_migrations_for_rollback(self, number: int = 1) -> list[Migration]:
        return [
            Migration(name=row[0], up=row[1], rollback=row[2])
            for row in self.ch_client.execute(
                f"SELECT name, up, rollback FROM db_migrations ORDER BY dt DESC LIMIT {number}",
                settings=self._settings,
            )
        ]

    def save_applied_migration(self, name: str, up: SQL, rollback: SQL, checksum: str = "") -> None:
        self.ch_client.execute(
            "INSERT INTO db_migrations (name, up, rollback, checksum) VALUES",
            [[name, up, rollback, checksum]],
            settings=self._settings,
        )

    def delete_migration(self, name: str) -> None:
        settings: ClickHouseSettings = {**self._settings, "mutations_sync": "1"}
        self.ch_client.execute(
            "DELETE FROM db_migrations WHERE name = %(name)s",
            {"name": name},
            settings=settings,
        )

    def validate_checksums(self) -> list[ChecksumMismatch]:
        rows: list[tuple[str, str]] = self.ch_client.execute(
            "SELECT name, checksum FROM db_migrations ORDER BY dt", settings=self._settings
        )
        mismatches: list[ChecksumMismatch] = []
        for name, stored_checksum in rows:
            if not stored_checksum:
                continue
            filepath = f"{self.migrations_dir}/{name}"
            if not os.path.exists(filepath):
                mismatches.append(ChecksumMismatch(name, stored_checksum, ""))
                continue
            with open(filepath) as f:
                actual_checksum = compute_checksum(f.read())
            if actual_checksum != stored_checksum:
                mismatches.append(ChecksumMismatch(name, stored_checksum, actual_checksum))
        return mismatches

    def repair(self) -> list[str]:
        """Update stored checksums to match current migration files."""
        mismatches = self.validate_checksums()
        if not mismatches:
            logger.info("Nothing to repair.")
            return []
        repaired: list[str] = []
        for name, _, actual in mismatches:
            if not actual:
                logger.warning("Skipping %s: file missing.", name)
                continue
            settings: ClickHouseSettings = {**self._settings, "mutations_sync": "1"}
            self.ch_client.execute(
                "ALTER TABLE db_migrations UPDATE checksum = %(checksum)s WHERE name = %(name)s",
                {"checksum": actual, "name": name},
                settings=settings,
            )
            repaired.append(name)
        return repaired

    def show_migrations(self, show_all: bool = False) -> ShowMigrationsResult:
        """Return formatted migration status and integrity warnings."""
        applied_names = self.get_applied_migrations_names()[::-1]
        unapplied_names = self.get_unapplied_migration_names()
        total_applied = len(applied_names)
        total_pending = len(unapplied_names)

        mismatch_map: dict[str, str] = {}
        for name, _, actual in self.validate_checksums():
            mismatch_map[name] = "missing" if not actual else "modified"

        lines: list[str] = [click.style("Applied:", bold=True)]
        if not applied_names:
            lines.append("  none")
        else:
            visible = applied_names if show_all else applied_names[:5]
            for i, name in enumerate(visible):
                suffixes: list[str] = []
                if i == 0:
                    suffixes.append("HEAD")
                status = mismatch_map.get(name, "")
                if status:
                    suffixes.append(status)

                prefix = click.style("[X]", fg="green")
                line = f"  {prefix} {name}"

                if suffixes:
                    suffix_text = ", ".join(suffixes)
                    if "missing" in suffixes:
                        color = "red"
                    elif "modified" in suffixes:
                        color = "yellow"
                    else:
                        color = "cyan"
                    line += " " + click.style(f"({suffix_text})", fg=color)

                lines.append(line)
            if not show_all and total_applied > 5:
                lines.append(f"  ... and {total_applied - 5} more applied")

        lines.append("")
        if unapplied_names:
            lines.append(click.style("Pending:", bold=True))
            for name in unapplied_names:
                prefix = click.style("[ ]", dim=True)
                lines.append(f"  {prefix} {name}")
        else:
            lines.append(click.style("Pending:", bold=True) + " none")

        lines.append("")
        lines.append(
            click.style(f"Applied: {total_applied}", fg="green")
            + " | "
            + click.style(f"Pending: {total_pending}", fg="yellow")
        )

        warning = ""
        if mismatch_map:
            count = len(mismatch_map)
            issue_word = "issue" if count == 1 else "issues"
            issue_lines: list[str] = [
                click.style(f"WARNING: {count} integrity {issue_word} found", fg="yellow", bold=True)
            ]
            for name, status in mismatch_map.items():
                if status == "missing":
                    issue_lines.append("  " + click.style(f"{name}: migration file missing", fg="red"))
                else:
                    issue_lines.append("  " + click.style(f"{name}: checksum mismatch", fg="yellow"))
            warning = "\n".join(issue_lines)

        return ShowMigrationsResult("\n".join(lines), warning)
