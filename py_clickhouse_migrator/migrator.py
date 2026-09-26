"""Migration runner for one ClickHouse database, plus helpers that create migration files."""

import datetime as dt
import itertools
import logging
import re
from pathlib import Path
from typing import Final, NamedTuple

from clickhouse_driver import Client
from clickhouse_driver.errors import ServerException

from py_clickhouse_migrator.clickhouse import (
    SQL,
    ClickHouseSettings,
    cluster_settings,
    is_sql_identifier,
    on_cluster_clause,
    wait_until_healthy,
)
from py_clickhouse_migrator.errors import (
    BaselineError,
    ChecksumMismatchError,
    InvalidMigrationError,
    InvalidStatementError,
    MigrationDirectoryNotFoundError,
    MissingDatabaseUrlError,
)
from py_clickhouse_migrator.migration import Migration, MigrationDirection, MigrationKind, load_migration
from py_clickhouse_migrator.reports import (
    MISSING,
    MODIFIED,
    MigrationStatus,
    echo_dry_run,
    render_integrity_warning,
    render_status,
)

logger = logging.getLogger("py_clickhouse_migrator")

MIGRATION_TEMPLATE: str = """-- migrator:up
-- @stmt


-- migrator:down
-- @stmt
"""
DEFAULT_MIGRATIONS_DIR: str = "./db/migrations"
DEFAULT_SEND_RECEIVE_TIMEOUT: Final = 600
CHECKSUM_PREVIEW_LENGTH: Final = 12

_MIGRATION_NAME_RE: Final[re.Pattern[str]] = re.compile(r"[a-zA-Z0-9_]+\Z")
_STATEMENT_PREVIEW_LENGTH: Final = 500
_ENGINE: Final = "MergeTree()"
_REPLICATED_ENGINE: Final = "ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')"
_SELECT_APPLIED_NAMES: Final = "SELECT name FROM db_migrations ORDER BY dt"
_SELECT_NAMES_BY_KIND: Final = "SELECT name FROM db_migrations WHERE kind = %(kind)s ORDER BY dt"
_SELECT_CHECKSUMS: Final = """
    SELECT name, checksum
    FROM db_migrations
    WHERE kind = %(kind)s
    ORDER BY dt
"""
_SELECT_FOR_ROLLBACK: Final = """
    SELECT name, up, rollback, kind
    FROM db_migrations
    WHERE kind = %(kind)s
    ORDER BY dt DESC
    LIMIT %(number)s
"""
_INSERT_APPLIED: Final = "INSERT INTO db_migrations (name, kind, up, rollback, checksum) VALUES"
_INSERT_BASELINED: Final = "INSERT INTO db_migrations (name, kind, up, rollback, dt, checksum) VALUES"
_DELETE_MIGRATION: Final = "DELETE FROM db_migrations WHERE name = %(name)s"
_UPDATE_CHECKSUM: Final = "ALTER TABLE db_migrations UPDATE checksum = %(checksum)s WHERE name = %(name)s"


class ChecksumMismatch(NamedTuple):
    """An applied migration whose file changed; ``actual`` is empty when the file is missing."""

    name: str
    stored: str
    actual: str


class ShowMigrationsResult(NamedTuple):
    """Output of ``Migrator.show_migrations``: the status report and an integrity warning, if any."""

    output: str
    warning: str


def create_migrations_dir(migrations_dir: str = DEFAULT_MIGRATIONS_DIR) -> None:
    """Create the migrations directory if it doesn't exist."""
    Path(migrations_dir).mkdir(parents=True, exist_ok=True)
    logger.info("Migrations directory %s successfully initialized.", migrations_dir)


def make_migration_filename(name: str = "") -> str:
    """Generate a timestamped migration filename.

    The timestamp uses local time with second precision, so files sort in creation order.

    Raises:
        ValueError: ``name`` contains characters other than letters, digits, and underscores.

    """
    if name and not _MIGRATION_NAME_RE.match(name):
        raise ValueError(f"Invalid migration name: '{name}'. Use only letters, digits, and underscores.")
    timestamp = dt.datetime.now(tz=dt.UTC).astimezone().strftime("%Y%m%d%H%M%S")
    suffix = f"_{name}" if name else ""
    return f"{timestamp}{suffix}.sql"


def create_migration_file(migrations_dir: str = DEFAULT_MIGRATIONS_DIR, name: str = "") -> str:
    """Create a new migration file from template. Returns the filepath.

    Raises:
        MigrationDirectoryNotFoundError: ``migrations_dir`` does not exist.

    """
    if not name:
        logger.warning("Migration name is recommended: py-clickhouse-migrator new <name>")

    directory = migrations_dir.removesuffix("/")
    filepath = f"{directory}/{make_migration_filename(name)}"
    try:
        Path(filepath).write_text(MIGRATION_TEMPLATE, encoding="utf-8")
    except FileNotFoundError:
        raise MigrationDirectoryNotFoundError(
            f"Migration directory {migrations_dir} not found.\nMake sure you run 'init' first.",
        ) from None

    logger.info("Migration %s has been created.", filepath)
    return filepath


class Migrator:
    """ClickHouse schema migration manager.

    Creating an instance connects to ClickHouse, checks that the database exists, and creates the
    ``db_migrations`` service table if needed.

    Args:
        database_url: ClickHouse connection URL; the database in it must already exist.
        migrations_dir: Directory with ``.sql`` migration files.
        cluster: ClickHouse cluster name for replicated operations.
        connect_retries: Number of connection retry attempts on startup.
        connect_retries_interval: Seconds between connection retries.
        send_receive_timeout: ClickHouse client send/receive timeout in seconds.

    Raises:
        MissingDatabaseUrlError: ``database_url`` is empty.
        ValueError: ``cluster`` is not a valid SQL identifier.

    """

    def __init__(
        self,
        database_url: str = "",
        migrations_dir: str = DEFAULT_MIGRATIONS_DIR,
        cluster: str = "",
        connect_retries: int = 0,
        connect_retries_interval: int = 1,
        send_receive_timeout: int = DEFAULT_SEND_RECEIVE_TIMEOUT,
    ) -> None:
        """Connect to ClickHouse and prepare the ``db_migrations`` table."""
        if not database_url:
            raise MissingDatabaseUrlError(
                "ClickHouse url was not provided.\n"
                "Use --url option or set CLICKHOUSE_MIGRATE_URL environment variable.",
            )
        if cluster and not is_sql_identifier(cluster):
            raise ValueError(f"Invalid cluster name: '{cluster}'. Use only letters, digits, and underscores.")
        self.database_url: str = database_url
        self.migrations_dir: str = migrations_dir
        self.cluster: str = cluster
        self._connect_retries = connect_retries
        self._connect_retries_interval = connect_retries_interval
        self._settings: ClickHouseSettings = cluster_settings(cluster)
        self.ch_client: Client = Client.from_url(database_url)
        self.ch_client.connection.send_receive_timeout = send_receive_timeout
        self.health_check()
        self.check_migrations_table()

    def check_migrations_table(self) -> None:
        """Create the ``db_migrations`` service table if it does not exist."""
        on_cluster = on_cluster_clause(self.cluster)
        engine = _REPLICATED_ENGINE if self.cluster else _ENGINE
        default_kind = MigrationKind.MIGRATION
        migrator_table: SQL = f"""
        CREATE TABLE IF NOT EXISTS db_migrations {on_cluster} (
            name String,
            kind Enum8('migration' = 1, 'baseline' = 2) DEFAULT '{default_kind}',
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
        """Check the connection, retrying as configured.

        Raises:
            DatabaseNotFoundError: the database from the URL does not exist.
            ClickHouseServerIsNotHealthyError: ClickHouse is unreachable after all retries.

        """
        wait_until_healthy(
            self.ch_client,
            database=self.get_db_name(),
            retries=self._connect_retries,
            interval=self._connect_retries_interval,
        )

    def get_db_name(self) -> str:
        """Return the database name from the connection URL."""
        url_path = self.database_url.rsplit("/", 1)[-1]
        return url_path.split("?", 1)[0]

    def check_integrity(self, allow_dirty: bool = False) -> None:
        """Fail if applied migration files were modified or deleted, unless ``allow_dirty`` is set.

        Raises:
            ChecksumMismatchError: an applied migration file changed and ``allow_dirty`` is not set.

        """
        mismatches = self.validate_checksums()
        if not mismatches:
            return
        if allow_dirty:
            logger.warning("Checksum mismatches found but --allow-dirty is set, continuing.")
            return
        details = "\n".join(_describe_mismatch(mismatch) for mismatch in mismatches)
        raise ChecksumMismatchError(
            f"Checksum mismatch for applied migrations:\n{details}\n\n"
            "Run 'migrator repair' to update checksums, or use --allow-dirty to skip this check.",
        )

    def up(
        self,
        n: int | None = None,
        dry_run: bool = False,
        allow_dirty: bool = False,
        validate: bool = True,
    ) -> None:
        """Apply pending migrations.

        Args:
            n: Maximum number of migrations to apply. All pending if None.
            dry_run: Print SQL without executing.
            allow_dirty: Skip checksum validation for modified files.
            validate: Run preflight validation before apply or dry-run output.

        """
        self.check_integrity(allow_dirty=allow_dirty)
        migrations = self.get_migrations_for_apply(n)
        if not migrations:
            logger.info("There are no migrations to apply.")
        if validate:
            self.validate_migrations(migrations, direction=MigrationDirection.UP)
        if dry_run:
            echo_dry_run(migrations, MigrationDirection.UP)
            return
        for migration in migrations:
            self.apply_migration(migration.up_statements)
            self.save_applied_migration(
                name=migration.name,
                up=migration.up,
                rollback=migration.rollback,
                checksum=migration.checksum,
            )
            logger.info("%s applied [✔]", migration.name)

    def rollback(self, number: int = 1, dry_run: bool = False, validate: bool = True) -> None:
        """Rollback applied migrations in reverse order.

        Baseline rows have no rollback SQL and are never rolled back.
        """
        migrations = self.get_migrations_for_rollback(number=number)
        if validate:
            self.validate_migrations(migrations, direction=MigrationDirection.ROLLBACK)
        if dry_run:
            echo_dry_run(migrations, MigrationDirection.ROLLBACK)
            return
        for migration in migrations:
            self.apply_migration(migration.rollback_statements)
            self.delete_migration(name=migration.name)
            logger.info("%s rolled back [✔].", migration.name)

    def apply_migration(self, queries: list[SQL]) -> None:
        """Execute queries one by one; earlier queries stay applied if a later one fails.

        Raises:
            InvalidMigrationError: ClickHouse rejected a query.

        """
        for query in queries:
            try:
                self.ch_client.execute(query)
            except ServerException as exc:
                raise InvalidMigrationError(f"Query {query} raise error: {exc}") from exc

    def validate_statements(self, statements: list[SQL]) -> None:
        """Check each statement with ``EXPLAIN AST`` without executing it.

        Raises:
            InvalidStatementError: ClickHouse rejected a statement.

        """
        for statement in statements:
            try:
                self.ch_client.execute(f"EXPLAIN AST {statement}", settings=self._settings)
            except ServerException as exc:
                preview = statement[:_STATEMENT_PREVIEW_LENGTH]
                raise InvalidStatementError(f"Query:\n{preview}\n\nClickHouse error:\n{exc}") from exc

    def validate_migrations(self, migrations: list[Migration], direction: MigrationDirection) -> None:
        """Validate the statements that ``direction`` would run for each migration.

        Raises:
            InvalidMigrationError: a statement failed validation.

        """
        for migration in migrations:
            try:
                self.validate_statements(statements=migration.statements(direction))
            except InvalidStatementError as exc:
                raise InvalidMigrationError(f"Validation failed for migration {migration.name}.\n\n{exc}") from exc

    def get_migrations_for_apply(self, number: int | None = None) -> list[Migration]:
        """Load pending migration files in apply order, at most ``number`` of them when it is set.

        Raises:
            InvalidMigrationError: a pending file cannot be read or parsed.

        """
        names = self.get_unapplied_migration_names()
        if number:
            names = names[:number]
        return [load_migration(self.migrations_dir, name) for name in names]

    def baseline(self) -> list[str]:
        """Record every migration file as applied without running it; returns the recorded names.

        Raises:
            BaselineError: ``db_migrations`` already has rows.

        """
        if self.get_applied_migrations_names():
            raise BaselineError("Baseline requires an empty db_migrations table.")
        filenames = self._get_sql_migration_filenames()
        if filenames:
            self.save_baselined_migrations(filenames)
        return filenames

    def get_unapplied_migration_names(self) -> list[str]:
        """Return names of migration files that are not in ``db_migrations``, sorted."""
        filenames = self._get_sql_migration_filenames()
        return sorted(set(filenames) - set(self.get_applied_migrations_names()))

    def get_applied_migrations_names(self) -> list[str]:
        """Return names from ``db_migrations``, oldest first."""
        return [row[0] for row in self.ch_client.execute(_SELECT_APPLIED_NAMES, settings=self._settings)]

    def get_migrations_for_rollback(self, number: int = 1) -> list[Migration]:
        """Load the ``number`` newest applied migrations from ``db_migrations``, newest first."""
        rows = self.ch_client.execute(
            _SELECT_FOR_ROLLBACK,
            {"kind": MigrationKind.MIGRATION.value, "number": number},
            settings=self._settings,
        )
        return list(itertools.starmap(Migration, rows))

    def save_applied_migration(self, name: str, up: SQL, rollback: SQL, checksum: str = "") -> None:
        """Insert an applied migration into ``db_migrations``."""
        self.ch_client.execute(
            _INSERT_APPLIED,
            [[name, MigrationKind.MIGRATION.value, up, rollback, checksum]],
            settings=self._settings,
        )

    def save_baselined_migrations(self, names: list[str]) -> None:
        """Insert baseline rows, spaced one millisecond apart so they keep the order of ``names``."""
        rows = []
        applied_at = dt.datetime.now(dt.UTC).replace(tzinfo=None)
        for name in names:
            rows.append([name, MigrationKind.BASELINE.value, "", "", applied_at, ""])
            applied_at += dt.timedelta(milliseconds=1)
        self.ch_client.execute(_INSERT_BASELINED, rows, settings=self._settings)

    def delete_migration(self, name: str) -> None:
        """Delete a migration row, waiting for the mutation so the next read does not see it."""
        self.ch_client.execute(_DELETE_MIGRATION, {"name": name}, settings=self._mutation_settings)

    def validate_checksums(self) -> list[ChecksumMismatch]:
        """Compare stored checksums of applied migrations with their files.

        Rows without a stored checksum (applied before checksums existed) and baseline rows are skipped.

        Raises:
            InvalidMigrationError: an applied migration file exists but cannot be parsed.

        """
        rows: list[tuple[str, str]] = self.ch_client.execute(
            _SELECT_CHECKSUMS,
            {"kind": MigrationKind.MIGRATION.value},
            settings=self._settings,
        )
        mismatches: list[ChecksumMismatch] = []
        for name, stored_checksum in rows:
            if not stored_checksum:
                continue
            actual_checksum = self._get_file_checksum(name)
            if actual_checksum != stored_checksum:
                mismatches.append(ChecksumMismatch(name, stored_checksum, actual_checksum))
        return mismatches

    def repair(self) -> list[str]:
        """Update stored checksums to match current migration files.

        Migrations whose file is missing are skipped. Returns the names of repaired migrations.
        """
        mismatches = self.validate_checksums()
        if not mismatches:
            logger.info("Nothing to repair.")
            return []
        repaired: list[str] = []
        for name, _, actual in mismatches:
            if not actual:
                logger.warning("Skipping %s: file missing.", name)
                continue
            self.ch_client.execute(
                _UPDATE_CHECKSUM,
                {"checksum": actual, "name": name},
                settings=self._mutation_settings,
            )
            repaired.append(name)
        return repaired

    def show_migrations(self, show_all: bool = False) -> ShowMigrationsResult:
        """Return formatted migration status and integrity warnings."""
        problems = {mismatch.name: MODIFIED if mismatch.actual else MISSING for mismatch in self.validate_checksums()}
        baseline_rows = self.ch_client.execute(
            _SELECT_NAMES_BY_KIND,
            {"kind": MigrationKind.BASELINE.value},
            settings=self._settings,
        )
        status = MigrationStatus(
            applied=list(reversed(self.get_applied_migrations_names())),
            pending=self.get_unapplied_migration_names(),
            baseline=frozenset(row[0] for row in baseline_rows),
            problems=problems,
        )
        return ShowMigrationsResult(render_status(status, show_all=show_all), render_integrity_warning(problems))

    @property
    def _mutation_settings(self) -> ClickHouseSettings:
        return {**self._settings, "mutations_sync": "1"}

    def _get_sql_migration_filenames(self) -> list[str]:
        try:
            entries = [path.name for path in Path(self.migrations_dir).iterdir()]
        except FileNotFoundError:
            raise MigrationDirectoryNotFoundError(
                f"Migration directory {self.migrations_dir} not found.\n"
                "Run 'migrator init' first or specify a migrations directory with the --path flag or "
                "the CLICKHOUSE_MIGRATE_DIR environment variable.",
            ) from None
        return sorted(entry for entry in entries if entry.endswith(".sql"))

    def _get_file_checksum(self, name: str) -> str:
        if not Path(self.migrations_dir, name).exists():
            return ""
        return load_migration(self.migrations_dir, name).checksum


def _describe_mismatch(mismatch: ChecksumMismatch) -> str:
    if not mismatch.actual:
        return f"  {mismatch.name}: file missing"
    stored = mismatch.stored[:CHECKSUM_PREVIEW_LENGTH]
    actual = mismatch.actual[:CHECKSUM_PREVIEW_LENGTH]
    return f"  {mismatch.name}: stored={stored}... actual={actual}..."
