"""Commands that inspect migration state and the migration lock."""

import click

from py_clickhouse_migrator.cli.options import CliSettings, pass_settings
from py_clickhouse_migrator.migrator import CHECKSUM_PREVIEW_LENGTH, ChecksumMismatch

_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


@click.command()
@click.option("--all", "show_all", is_flag=True, default=False, help="Show all migrations.")
@pass_settings
def show(settings: CliSettings, *, show_all: bool) -> None:
    """Show applied and pending migrations.

    Warnings about modified or missing migration files go to stderr.
    """
    output, warning = settings.make_migrator().show_migrations(show_all=show_all)
    click.echo(output)
    if warning:
        click.echo(f"\n{warning}", err=True)


@click.command()
@pass_settings
def repair(settings: CliSettings) -> None:
    """Sync stored checksums with the current migration files."""
    migrator = settings.make_migrator()
    mismatches = migrator.validate_checksums()
    if not mismatches:
        click.echo("Nothing to repair. All checksums are valid.")
        return
    click.echo("Modified migrations:")
    for mismatch in mismatches:
        click.echo(_describe_change(mismatch))
    repaired = migrator.repair()
    if repaired:
        click.echo(f"\nRepaired {len(repaired)} migration(s).")


@click.command("force-unlock")
@pass_settings
def force_unlock(settings: CliSettings) -> None:
    """Release the migration lock, whoever holds it."""
    settings.make_lock(settings.make_migrator()).release(force=True)
    click.echo("Lock forcefully released.")


@click.command("lock-info")
@pass_settings
def lock_info(settings: CliSettings) -> None:
    """Show who holds the migration lock and when it expires."""
    holder = settings.make_lock(settings.make_migrator()).get_lock_info()
    if holder is None:
        click.echo("No active lock.")
        return
    locked_at = holder.locked_at.strftime(_TIME_FORMAT)
    expires_at = holder.expires_at.strftime(_TIME_FORMAT)
    click.echo(f"Locked by: {holder.locked_by}")
    click.echo(f"Locked at: {locked_at}")
    click.echo(f"Expires at: {expires_at}")


def _describe_change(mismatch: ChecksumMismatch) -> str:
    if not mismatch.actual:
        return f"  {mismatch.name}: file missing (skipped)"
    stored = mismatch.stored[:CHECKSUM_PREVIEW_LENGTH]
    actual = mismatch.actual[:CHECKSUM_PREVIEW_LENGTH]
    return f"  {mismatch.name}: {stored}... → {actual}..."
