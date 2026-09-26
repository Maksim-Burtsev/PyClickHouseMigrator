"""Settings and option groups shared by the CLI commands."""

from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from typing import Final, TypedDict

import click

from py_clickhouse_migrator.lock import DEFAULT_LOCK_TTL, MigrationLock
from py_clickhouse_migrator.migrator import Migrator

Command = Callable[..., None]

_CLI_LOCK_TTL: Final = 600
_CLI_LOCK_RETRIES: Final = 3


@dataclass(frozen=True)
class CliSettings:
    """Global ``migrator`` options that every command needs to reach ClickHouse."""

    url: str
    path: str
    cluster: str
    connect_retries: int
    connect_retries_interval: int
    send_receive_timeout: int

    def make_migrator(self) -> Migrator:
        """Connect to ClickHouse with these settings."""
        return Migrator(
            database_url=self.url,
            migrations_dir=self.path,
            cluster=self.cluster,
            connect_retries=self.connect_retries,
            connect_retries_interval=self.connect_retries_interval,
            send_receive_timeout=self.send_receive_timeout,
        )

    def make_lock(self, migrator: Migrator, *, ttl: int = DEFAULT_LOCK_TTL, retries: int = 0) -> MigrationLock:
        """Create the migration lock in the database that ``migrator`` works with."""
        return MigrationLock(
            client=migrator.ch_client,
            db=migrator.get_db_name(),
            ttl=ttl,
            retry_count=retries,
            cluster=self.cluster,
        )


class LockOptions(TypedDict):
    """Values of ``--lock/--no-lock``, ``--lock-ttl``, and ``--lock-retry``."""

    lock: bool
    lock_ttl: int
    lock_retry: int


pass_settings = click.make_pass_decorator(CliSettings)


def with_options(*options: Callable[[Command], Command]) -> Callable[[Command], Command]:
    """Combine click decorators into one, applied in the order they are listed."""

    def decorator(command: Command) -> Command:
        for option in reversed(options):
            command = option(command)
        return command

    return decorator


def migration_lock(
    settings: CliSettings,
    migrator: Migrator,
    options: LockOptions,
) -> AbstractContextManager[MigrationLock | None]:
    """Return the lock to hold while changing migration state, or a no-op with ``--no-lock``."""
    if not options["lock"]:
        return nullcontext()
    return settings.make_lock(migrator, ttl=options["lock_ttl"], retries=options["lock_retry"])


lock_options = with_options(
    click.option("--lock/--no-lock", default=True, help="Enable/disable migration lock."),
    click.option("--lock-ttl", type=click.IntRange(min=1), default=_CLI_LOCK_TTL, help="Lock TTL in seconds."),
    click.option(
        "--lock-retry",
        type=click.IntRange(min=0),
        default=_CLI_LOCK_RETRIES,
        help="Number of lock acquire retries.",
    ),
)
dry_run_option = click.option("--dry-run", is_flag=True, default=False, help="Show SQL without executing.")
validate_option = click.option(
    "--validate/--no-validate",
    default=True,
    help="Enable/disable preflight validation.",
)
