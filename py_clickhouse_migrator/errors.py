class ClickHouseServerIsNotHealthyError(Exception):
    """ClickHouse did not answer ``SELECT 1`` within the configured connection retries."""


class MigrationDirectoryNotFoundError(Exception):
    """The migrations directory does not exist."""


class InvalidMigrationError(Exception):
    """A migration cannot be parsed, validated, or executed."""


class InvalidStatementError(Exception):
    """ClickHouse rejected a statement during preflight ``EXPLAIN AST`` validation."""


class MissingDatabaseUrlError(Exception):
    """No ClickHouse connection URL was provided."""


class DatabaseNotFoundError(Exception):
    """The database named in the connection URL does not exist."""


class ChecksumMismatchError(Exception):
    """Applied migration files were modified or deleted after they were applied."""


class MigrationParseError(ValueError):
    """A migration file violates the ``-- migrator:up`` / ``-- migrator:down`` / ``-- @stmt`` format."""


class BaselineError(Exception):
    """Baseline was requested for a database that already has applied migrations."""
