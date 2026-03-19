import logging
from typing import Final
import typing as t

import click
from py_clickhouse_migrator import Migrator
from py_clickhouse_migrator.lock import LockError, MigrationLock
from py_clickhouse_migrator.migrator import (
    ChecksumMismatchError,
    ClickHouseServerIsNotHealthyError,
    DatabaseNotFoundError,
    DEFAULT_MIGRATIONS_DIR,
    InvalidMigrationError,
    MigrationDirectoryNotFoundError,
    MissingDatabaseUrlError,
)

logger = logging.getLogger("py_clickhouse_migrator")


_HANDLED_EXCEPTIONS: Final[tuple[type[Exception], ...]] = (
    LockError,
    ChecksumMismatchError,
    InvalidMigrationError,
    ClickHouseServerIsNotHealthyError,
    MissingDatabaseUrlError,
    MigrationDirectoryNotFoundError,
    DatabaseNotFoundError,
)


class SafeGroup(click.Group):
    def invoke(self, ctx: click.Context) -> None:
        try:
            super().invoke(ctx)
        except _HANDLED_EXCEPTIONS as exc:
            click.echo(click.style("Error: ", fg="red", bold=True) + str(exc), err=True)
            ctx.exit(1)


class ContextObj(t.TypedDict):
    url: str
    path: str
    cluster: str
    connect_retries: int
    connect_retries_interval: int


@click.command()
@click.pass_context
def init(ctx: click.Context) -> None:
    Migrator(
        database_url=ctx.obj["url"],
        migrations_dir=ctx.obj["path"],
        cluster=ctx.obj["cluster"],
        connect_retries=ctx.obj["connect_retries"],
        connect_retries_interval=ctx.obj["connect_retries_interval"],
    ).init()


@click.command()
@click.argument(
    "number",
    type=click.IntRange(min=1),
    default=None,
    required=False,
)
@click.option("--lock/--no-lock", default=True, help="Enable/disable migration lock.")
@click.option("--lock-ttl", type=click.IntRange(min=1), default=300, help="Lock TTL in seconds.")
@click.option("--lock-retry", type=click.IntRange(min=0), default=3, help="Number of lock acquire retries.")
@click.option("--dry-run", is_flag=True, default=False, help="Show SQL without executing.")
@click.option("--allow-dirty", is_flag=True, default=False, help="Skip checksum validation.")
@click.pass_context
def up(
    ctx: click.Context,
    number: int,
    lock: bool,
    lock_ttl: int,
    lock_retry: int,
    dry_run: bool,
    allow_dirty: bool,
) -> None:
    cluster = ctx.obj["cluster"]
    migrator = Migrator(
        database_url=ctx.obj["url"],
        migrations_dir=ctx.obj["path"],
        cluster=cluster,
        connect_retries=ctx.obj["connect_retries"],
        connect_retries_interval=ctx.obj["connect_retries_interval"],
    )
    if dry_run:
        migrator.up(n=number, dry_run=True, allow_dirty=allow_dirty)
        return
    if lock:
        if not migrator.get_unapplied_migration_names():
            logger.info("No pending migrations, skipping lock.")
            return
        with MigrationLock(
            client=migrator.ch_client, db=migrator.get_db_name(), ttl=lock_ttl, retry_count=lock_retry, cluster=cluster
        ):
            migrator.up(n=number, allow_dirty=allow_dirty)
    else:
        migrator.up(n=number, allow_dirty=allow_dirty)


@click.command()
@click.argument(
    "number",
    type=click.IntRange(min=1),
    default=1,
    required=False,
)
@click.option("--lock/--no-lock", default=True, help="Enable/disable migration lock.")
@click.option("--lock-ttl", type=click.IntRange(min=1), default=300, help="Lock TTL in seconds.")
@click.option("--lock-retry", type=click.IntRange(min=0), default=3, help="Number of lock acquire retries.")
@click.option("--dry-run", is_flag=True, default=False, help="Show SQL without executing.")
@click.pass_context
def rollback(ctx: click.Context, number: int, lock: bool, lock_ttl: int, lock_retry: int, dry_run: bool) -> None:
    cluster = ctx.obj["cluster"]
    migrator = Migrator(
        database_url=ctx.obj["url"],
        migrations_dir=ctx.obj["path"],
        cluster=cluster,
        connect_retries=ctx.obj["connect_retries"],
        connect_retries_interval=ctx.obj["connect_retries_interval"],
    )
    if dry_run:
        migrator.rollback(number=number, dry_run=True)
        return
    if lock:
        with MigrationLock(
            client=migrator.ch_client, db=migrator.get_db_name(), ttl=lock_ttl, retry_count=lock_retry, cluster=cluster
        ):
            migrator.rollback(number=number)
    else:
        migrator.rollback(number=number)


@click.command()
@click.option("--all", "show_all", is_flag=True, default=False, help="Show all migrations.")
@click.pass_context
def show(ctx: click.Context, show_all: bool) -> None:
    output, warning = Migrator(
        database_url=ctx.obj["url"],
        migrations_dir=ctx.obj["path"],
        cluster=ctx.obj["cluster"],
        connect_retries=ctx.obj["connect_retries"],
        connect_retries_interval=ctx.obj["connect_retries_interval"],
    ).show_migrations(show_all=show_all)
    click.echo(output)
    if warning:
        click.echo(f"\n{warning}", err=True)


@click.command()
@click.pass_context
@click.argument(
    "name",
    type=str,
    default="",
    required=False,
)
def new(ctx: click.Context, name: str) -> None:
    Migrator(
        database_url=ctx.obj["url"],
        migrations_dir=ctx.obj["path"],
        cluster=ctx.obj["cluster"],
        connect_retries=ctx.obj["connect_retries"],
        connect_retries_interval=ctx.obj["connect_retries_interval"],
    ).create_new_migration(name=name)


@click.command()
@click.pass_context
def repair(ctx: click.Context) -> None:
    migrator = Migrator(
        database_url=ctx.obj["url"],
        migrations_dir=ctx.obj["path"],
        cluster=ctx.obj["cluster"],
        connect_retries=ctx.obj["connect_retries"],
        connect_retries_interval=ctx.obj["connect_retries_interval"],
    )
    mismatches = migrator.validate_checksums()
    if not mismatches:
        click.echo("Nothing to repair. All checksums are valid.")
        return
    click.echo("Modified migrations:")
    for name, stored, actual in mismatches:
        if actual:
            click.echo(f"  {name}: {stored[:12]}... \u2192 {actual[:12]}...")
        else:
            click.echo(f"  {name}: file missing (skipped)")
    repaired = migrator.repair()
    if repaired:
        click.echo(f"\nRepaired {len(repaired)} migration(s).")


@click.command("force-unlock")
@click.pass_context
def force_unlock(ctx: click.Context) -> None:
    cluster = ctx.obj["cluster"]
    migrator = Migrator(
        database_url=ctx.obj["url"],
        migrations_dir=ctx.obj["path"],
        cluster=cluster,
        connect_retries=ctx.obj["connect_retries"],
        connect_retries_interval=ctx.obj["connect_retries_interval"],
    )
    lock = MigrationLock(client=migrator.ch_client, db=migrator.get_db_name(), cluster=cluster)
    lock.force_release()
    click.echo("Lock forcefully released.")


@click.command("lock-info")
@click.pass_context
def lock_info(ctx: click.Context) -> None:
    cluster = ctx.obj["cluster"]
    migrator = Migrator(
        database_url=ctx.obj["url"],
        migrations_dir=ctx.obj["path"],
        cluster=cluster,
        connect_retries=ctx.obj["connect_retries"],
        connect_retries_interval=ctx.obj["connect_retries_interval"],
    )
    ml = MigrationLock(client=migrator.ch_client, db=migrator.get_db_name(), cluster=cluster)
    info = ml.get_lock_info()
    if info is None:
        click.echo("No active lock.")
    else:
        click.echo(f"Locked by: {info.locked_by}")
        click.echo(f"Locked at: {info.locked_at:%Y-%m-%d %H:%M:%S}")
        click.echo(f"Expires at: {info.expires_at:%Y-%m-%d %H:%M:%S}")


@click.group(cls=SafeGroup)
@click.option(
    "--url",
    type=str,
    help="ClickHouse url. Example: clickhouse://default@127.0.0.1:9000/default",
    default="",
    envvar="CLICKHOUSE_MIGRATE_URL",
)
@click.option(
    "--path",
    type=str,
    help="Path to migrations directory. Default: ./db/migrations",
    default=DEFAULT_MIGRATIONS_DIR,
    envvar="CLICKHOUSE_MIGRATE_DIR",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Enable verbose (DEBUG) logging.",
)
@click.option(
    "--quiet",
    "-q",
    is_flag=True,
    default=False,
    help="Suppress all output except errors.",
)
@click.option(
    "--cluster",
    type=str,
    help="ClickHouse cluster name for ON CLUSTER DDL and replicated service tables.",
    default="",
    envvar="CLICKHOUSE_MIGRATE_CLUSTER",
)
@click.option(
    "--connect-retries",
    type=click.IntRange(min=0),
    default=0,
    envvar="CLICKHOUSE_MIGRATE_CONNECT_RETRIES",
    help="Max retries when connecting to ClickHouse.",
)
@click.option(
    "--connect-retries-interval",
    type=click.IntRange(min=0),
    default=1,
    envvar="CLICKHOUSE_MIGRATE_CONNECT_RETRIES_INTERVAL",
    help="Seconds between connection retries.",
)
@click.pass_context
def main(
    ctx: click.Context,
    url: str,
    path: str,
    verbose: bool,
    quiet: bool,
    cluster: str,
    connect_retries: int,
    connect_retries_interval: int,
) -> None:
    if verbose:
        level = logging.DEBUG
    elif quiet:
        level = logging.ERROR
    else:
        level = logging.INFO
    logging.basicConfig(level=level, format="%(message)s")

    ctx.obj = ContextObj(
        url=url,
        path=path,
        cluster=cluster,
        connect_retries=connect_retries,
        connect_retries_interval=connect_retries_interval,
    )


main.add_command(init)
main.add_command(new)
main.add_command(up)
main.add_command(rollback)
main.add_command(show)
main.add_command(repair)
main.add_command(force_unlock)
main.add_command(lock_info)
