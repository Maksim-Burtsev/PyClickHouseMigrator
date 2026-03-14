import logging
import typing as t

import click

from py_clickhouse_migrator import Migrator
from py_clickhouse_migrator.lock import MigrationLock

logger = logging.getLogger("py_clickhouse_migrator")


class ContextObj(t.TypedDict):
    url: str
    path: str | None


@click.command()
@click.pass_context
def init(ctx: click.Context) -> None:
    Migrator(database_url=ctx.obj["url"], migrations_dir=ctx.obj["path"]).init()


@click.command()
@click.argument(
    "number",
    type=int,
    default=None,
    required=False,
)
@click.option("--lock/--no-lock", default=True, help="Enable/disable migration lock.")
@click.option("--lock-ttl", type=int, default=300, help="Lock TTL in seconds.")
@click.option("--lock-retry", type=int, default=3, help="Number of lock acquire retries.")
@click.option("--dry-run", is_flag=True, default=False, help="Show SQL without executing.")
@click.pass_context
def up(ctx: click.Context, number: int, lock: bool, lock_ttl: int, lock_retry: int, dry_run: bool) -> None:
    migrator = Migrator(database_url=ctx.obj["url"], migrations_dir=ctx.obj["path"])
    if dry_run:
        migrator.up(n=number, dry_run=True)
        return
    if lock:
        if not migrator.get_unapplied_migration_names():
            logger.info("No pending migrations, skipping lock.")
            return
        with MigrationLock(client=migrator.ch_client, db=migrator.get_db_name(), ttl=lock_ttl, retry_count=lock_retry):
            migrator.up(n=number)
    else:
        migrator.up(n=number)


@click.command()
@click.argument(
    "number",
    type=int,
    default=1,
    required=False,
)
@click.option("--lock/--no-lock", default=True, help="Enable/disable migration lock.")
@click.option("--lock-ttl", type=int, default=300, help="Lock TTL in seconds.")
@click.option("--lock-retry", type=int, default=3, help="Number of lock acquire retries.")
@click.option("--dry-run", is_flag=True, default=False, help="Show SQL without executing.")
@click.pass_context
def rollback(ctx: click.Context, number: int, lock: bool, lock_ttl: int, lock_retry: int, dry_run: bool) -> None:
    migrator = Migrator(database_url=ctx.obj["url"], migrations_dir=ctx.obj["path"])
    if dry_run:
        migrator.rollback(number=number, dry_run=True)
        return
    if lock:
        with MigrationLock(client=migrator.ch_client, db=migrator.get_db_name(), ttl=lock_ttl, retry_count=lock_retry):
            migrator.rollback(number=number)
    else:
        migrator.rollback(number=number)


@click.command()
@click.option("--all", "show_all", is_flag=True, default=False, help="Show all migrations.")
@click.pass_context
def show(ctx: click.Context, show_all: bool) -> None:
    output = Migrator(database_url=ctx.obj["url"], migrations_dir=ctx.obj["path"]).show_migrations(show_all=show_all)
    click.echo(output)


@click.command()
@click.pass_context
@click.argument(
    "name",
    type=str,
    default="",
    required=False,
)
def new(ctx: click.Context, name: str) -> None:
    Migrator(database_url=ctx.obj["url"], migrations_dir=ctx.obj["path"]).create_new_migration(name=name)


@click.command("force-unlock")
@click.pass_context
def force_unlock(ctx: click.Context) -> None:
    migrator = Migrator(database_url=ctx.obj["url"], migrations_dir=ctx.obj["path"])
    lock = MigrationLock(client=migrator.ch_client, db=migrator.get_db_name())
    lock.force_release()
    click.echo("Lock forcefully released.")


@click.command("lock-info")
@click.pass_context
def lock_info(ctx: click.Context) -> None:
    migrator = Migrator(database_url=ctx.obj["url"], migrations_dir=ctx.obj["path"])
    ml = MigrationLock(client=migrator.ch_client, db=migrator.get_db_name())
    info = ml.get_lock_info()
    if info is None:
        click.echo("No active lock.")
    else:
        click.echo(f"Locked by: {info.locked_by}")
        click.echo(f"Locked at: {info.locked_at:%Y-%m-%d %H:%M:%S}")
        click.echo(f"Expires at: {info.expires_at:%Y-%m-%d %H:%M:%S}")


@click.group()
@click.option(
    "--url",
    type=str,
    help="ClickHouse url.\ntExample: clickhouse://default@127.0.0.1:9000/default",
    default="",
    required=False,
)
@click.option(
    "--path",
    type=str,
    help="Path to migrations directory.\nDefault: ./db/migrations",
    default=None,
    required=False,
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
@click.pass_context
def main(ctx: click.Context, url: str, path: str, verbose: bool, quiet: bool) -> None:
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
    )


main.add_command(init)
main.add_command(new)
main.add_command(up)
main.add_command(rollback)
main.add_command(show)
main.add_command(force_unlock)
main.add_command(lock_info)
