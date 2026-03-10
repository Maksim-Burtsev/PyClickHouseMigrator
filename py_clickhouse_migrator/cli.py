import logging
import typing as t

import click

from py_clickhouse_migrator import Migrator


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
@click.pass_context
def up(ctx: click.Context, number: int) -> None:
    Migrator(database_url=ctx.obj["url"], migrations_dir=ctx.obj["path"]).up(n=number)


@click.command()
@click.argument(
    "number",
    type=int,
    default=1,
    required=False,
)
@click.pass_context
def rollback(ctx: click.Context, number: int) -> None:
    Migrator(database_url=ctx.obj["url"], migrations_dir=ctx.obj["path"]).rollback(number=number)


@click.command()
@click.pass_context
def show(ctx: click.Context) -> None:
    output = Migrator(database_url=ctx.obj["url"], migrations_dir=ctx.obj["path"]).show_migrations()
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
