import typing as t

import click
from migrator import Migrator


class ContextObj(t.TypedDict):
    url: str
    path: str | None


@click.command()
@click.pass_context
def init(ctx: click.Context):
    Migrator(database_url=ctx.obj["url"], migrations_dir=ctx.obj["path"]).init()


@click.command()
@click.argument(
    "number",
    type=int,
    default=None,
    required=False,
)
@click.pass_context
def up(ctx: click.Context, number: int):
    Migrator(database_url=ctx.obj["url"], migrations_dir=ctx.obj["path"]).up(n=number)


@click.command()
@click.argument(
    "number",
    type=int,
    default=None,
    required=False,
)
@click.pass_context
def rollback(ctx: click.Context, number: int):
    Migrator(database_url=ctx.obj["url"], migrations_dir=ctx.obj["path"]).rollback(n=number)


@click.command()
@click.pass_context
def show(ctx: click.Context):
    Migrator(database_url=ctx.obj["url"], migrations_dir=ctx.obj["path"]).show_migrations()


@click.command()
@click.pass_context
@click.argument(
    "name",
    type=str,
    default="",
    required=False,
)
def new(ctx: click.Context, name: str):
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
@click.pass_context
def main(ctx: click.Context, url: str, path: str):
    ctx.obj = ContextObj(
        url=url,
        path=path,
    )


main.add_command(init)
main.add_command(new)
main.add_command(up)
main.add_command(rollback)
main.add_command(show)

if __name__ == "__main__":
    main()
