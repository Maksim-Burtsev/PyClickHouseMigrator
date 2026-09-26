"""Commands that create migration files and change what is applied."""

from typing import Unpack

import click

from py_clickhouse_migrator.cli.options import (
    CliSettings,
    LockOptions,
    dry_run_option,
    lock_options,
    migration_lock,
    pass_settings,
    validate_option,
    with_options,
)
from py_clickhouse_migrator.migrator import create_migration_file, create_migrations_dir


class UpOptions(LockOptions):
    """Options of ``migrator up``."""

    dry_run: bool
    validate: bool
    allow_dirty: bool


class RollbackOptions(LockOptions):
    """Options of ``migrator rollback``."""

    dry_run: bool
    validate: bool


@click.command()
@pass_settings
def init(settings: CliSettings) -> None:
    """Create the migrations directory."""
    create_migrations_dir(migrations_dir=settings.path)


@click.command()
@click.argument("name", type=str, default="", required=False)
@pass_settings
def new(settings: CliSettings, name: str) -> None:
    """Create an empty migration file named TIMESTAMP_NAME.sql."""
    create_migration_file(migrations_dir=settings.path, name=name)


@click.command()
@click.argument("number", type=click.IntRange(min=1), default=None, required=False)
@with_options(
    lock_options,
    dry_run_option,
    validate_option,
    click.option("--allow-dirty", is_flag=True, default=False, help="Skip checksum validation."),
)
@pass_settings
def up(settings: CliSettings, number: int | None, **options: Unpack[UpOptions]) -> None:
    """Apply pending migrations: all of them, or the first NUMBER."""
    migrator = settings.make_migrator()
    allow_dirty = options["allow_dirty"]
    validate = options["validate"]
    if options["dry_run"]:
        migrator.up(n=number, dry_run=True, allow_dirty=allow_dirty, validate=validate)
        return
    with migration_lock(settings, migrator, options):
        migrator.up(n=number, allow_dirty=allow_dirty, validate=validate)


@click.command()
@click.argument("number", type=click.IntRange(min=1), default=1, required=False)
@with_options(lock_options, dry_run_option, validate_option)
@pass_settings
def rollback(settings: CliSettings, number: int, **options: Unpack[RollbackOptions]) -> None:
    """Roll back the last NUMBER applied migrations (default: 1)."""
    migrator = settings.make_migrator()
    if options["dry_run"]:
        migrator.rollback(number=number, dry_run=True, validate=options["validate"])
        return
    with migration_lock(settings, migrator, options):
        migrator.rollback(number=number, validate=options["validate"])


@click.command()
@lock_options
@pass_settings
def baseline(settings: CliSettings, **options: Unpack[LockOptions]) -> None:
    """Mark every migration file as applied without running it."""
    migrator = settings.make_migrator()
    with migration_lock(settings, migrator, options):
        migration_names = migrator.baseline()

    if not migration_names:
        click.echo(click.style("No SQL migration files found to baseline.", fg="yellow"))
        return

    summary = f"Baselined {len(migration_names)} migration(s)."
    click.echo(click.style(summary, fg="green", bold=True))
    marker = click.style("[B]", fg="cyan")
    for name in migration_names:
        click.echo(f"  {marker} {name}")
