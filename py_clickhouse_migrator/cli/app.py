"""The ``migrator`` command group and its global options."""

import logging
from importlib.metadata import version
from typing import Final, TypedDict, Unpack

import click

from py_clickhouse_migrator.cli.maintenance import force_unlock, lock_info, repair, show
from py_clickhouse_migrator.cli.migrate import baseline, init, new, rollback, up
from py_clickhouse_migrator.cli.options import CliSettings, with_options
from py_clickhouse_migrator.errors import (
    BaselineError,
    ChecksumMismatchError,
    ClickHouseServerIsNotHealthyError,
    DatabaseNotFoundError,
    InvalidMigrationError,
    MigrationDirectoryNotFoundError,
    MissingDatabaseUrlError,
)
from py_clickhouse_migrator.lock import LockError
from py_clickhouse_migrator.migrator import DEFAULT_MIGRATIONS_DIR, DEFAULT_SEND_RECEIVE_TIMEOUT

_DISTRIBUTION: Final = "py-clickhouse-migrator"
_HANDLED_EXCEPTIONS: Final = (
    BaselineError,
    LockError,
    ChecksumMismatchError,
    InvalidMigrationError,
    ClickHouseServerIsNotHealthyError,
    MissingDatabaseUrlError,
    MigrationDirectoryNotFoundError,
    DatabaseNotFoundError,
)


class SafeGroup(click.Group):
    """Command group that prints expected errors as one ``Error:`` line and exits with code 1."""

    def invoke(self, ctx: click.Context) -> None:
        """Run the command; unexpected exceptions still propagate with a traceback."""
        try:
            super().invoke(ctx)
        except _HANDLED_EXCEPTIONS as exc:
            prefix = click.style("Error: ", fg="red", bold=True)
            click.echo(f"{prefix}{exc}", err=True)
            ctx.exit(1)


class GlobalOptions(TypedDict):
    """Options accepted by ``migrator`` before the command name."""

    url: str
    path: str
    verbose: bool
    quiet: bool
    cluster: str
    connect_retries: int
    connect_retries_interval: int
    send_receive_timeout: int


@click.group(cls=SafeGroup)
@with_options(
    click.version_option(version=version(_DISTRIBUTION), prog_name=_DISTRIBUTION),
    click.option(
        "--url",
        type=str,
        help="ClickHouse url. Example: clickhouse://default@127.0.0.1:9000/default",
        default="",
        envvar="CLICKHOUSE_MIGRATE_URL",
    ),
    click.option(
        "--path",
        type=str,
        help="Path to migrations directory. Default: ./db/migrations",
        default=DEFAULT_MIGRATIONS_DIR,
        envvar="CLICKHOUSE_MIGRATE_DIR",
    ),
    click.option("--verbose", "-v", is_flag=True, default=False, help="Enable verbose (DEBUG) logging."),
    click.option(
        "--quiet",
        "-q",
        is_flag=True,
        default=False,
        help="Suppress INFO/WARNING logs. Command output may still be printed.",
    ),
    click.option(
        "--cluster",
        type=str,
        help="ClickHouse cluster name for ON CLUSTER DDL and replicated service tables.",
        default="",
        envvar="CLICKHOUSE_MIGRATE_CLUSTER",
    ),
    click.option(
        "--connect-retries",
        type=click.IntRange(min=0),
        default=0,
        envvar="CLICKHOUSE_MIGRATE_CONNECT_RETRIES",
        help="Max retries when connecting to ClickHouse.",
    ),
    click.option(
        "--connect-retries-interval",
        type=click.IntRange(min=0),
        default=1,
        envvar="CLICKHOUSE_MIGRATE_CONNECT_RETRIES_INTERVAL",
        help="Seconds between connection retries.",
    ),
    click.option(
        "--send-receive-timeout",
        type=int,
        default=DEFAULT_SEND_RECEIVE_TIMEOUT,
        envvar="CLICKHOUSE_MIGRATE_SEND_RECEIVE_TIMEOUT",
        help="Timeout in seconds for sending/receiving data. Default: 600.",
    ),
)
@click.pass_context
def main(ctx: click.Context, **options: Unpack[GlobalOptions]) -> None:
    """SQL-first ClickHouse schema migrations."""
    if options["verbose"]:
        level = logging.DEBUG
    elif options["quiet"]:
        level = logging.ERROR
    else:
        level = logging.INFO
    logging.basicConfig(level=level, format="%(message)s")

    ctx.obj = CliSettings(
        url=options["url"],
        path=options["path"],
        cluster=options["cluster"],
        connect_retries=options["connect_retries"],
        connect_retries_interval=options["connect_retries_interval"],
        send_receive_timeout=options["send_receive_timeout"],
    )


main.add_command(init)
main.add_command(new)
main.add_command(up)
main.add_command(rollback)
main.add_command(show)
main.add_command(baseline)
main.add_command(repair)
main.add_command(force_unlock)
main.add_command(lock_info)
