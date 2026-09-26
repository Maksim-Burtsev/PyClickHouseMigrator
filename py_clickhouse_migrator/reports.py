"""Text reports printed by the CLI: migration status, integrity warnings, and dry-run SQL."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Final

import click

from py_clickhouse_migrator.migration import Migration, MigrationDirection

MISSING: Final = "missing"
MODIFIED: Final = "modified"

_VISIBLE_APPLIED_LIMIT: Final = 5
_GREEN: Final = "green"
_YELLOW: Final = "yellow"
_PROBLEM_MESSAGES: Final[Mapping[str, str]] = MappingProxyType({
    MISSING: "migration file missing",
    MODIFIED: "checksum mismatch",
})
_PROBLEM_COLORS: Final[Mapping[str, str]] = MappingProxyType({MISSING: "red", MODIFIED: _YELLOW})
_DRY_RUN_COLORS: Final[Mapping[MigrationDirection, str]] = MappingProxyType({
    MigrationDirection.UP: "cyan",
    MigrationDirection.ROLLBACK: _YELLOW,
})


@dataclass(frozen=True)
class MigrationStatus:
    """Snapshot of migration state for ``migrator show``.

    Attributes:
        applied: applied migration names, newest first.
        pending: migration files that are not applied yet, in apply order.
        baseline: applied names recorded by ``migrator baseline``.
        problems: applied names whose file is ``MISSING`` or ``MODIFIED``, in apply order.

    """

    applied: Sequence[str]
    pending: Sequence[str]
    baseline: frozenset[str]
    problems: Mapping[str, str]

    def labels(self, name: str) -> list[str]:
        """Labels shown next to applied migration ``name``: HEAD, baseline, and its integrity problem."""
        labels = ["HEAD"] if name in self.applied[:1] else []
        if name in self.baseline:
            labels.append("baseline")
        problem = self.problems.get(name)
        if problem:
            labels.append(problem)
        return labels


def render_status(status: MigrationStatus, *, show_all: bool) -> str:
    """Render applied and pending migrations with a summary line.

    Only the newest five applied migrations are listed unless ``show_all`` is set.
    """
    applied_count = click.style(f"Applied: {len(status.applied)}", fg=_GREEN)
    pending_count = click.style(f"Pending: {len(status.pending)}", fg=_YELLOW)
    lines = [
        *_applied_section(status, show_all=show_all),
        "",
        *_pending_section(status.pending),
        "",
        f"{applied_count} | {pending_count}",
    ]
    return "\n".join(lines)


def render_integrity_warning(problems: Mapping[str, str]) -> str:
    """Render one warning line per missing or modified migration file, or nothing if all files match."""
    if not problems:
        return ""
    noun = "issue" if len(problems) == 1 else "issues"
    title = f"WARNING: {len(problems)} integrity {noun} found"
    lines = [click.style(title, fg=_YELLOW, bold=True)]
    lines.extend(
        click.style(f"{name}: {_PROBLEM_MESSAGES[problem]}", fg=_PROBLEM_COLORS[problem])
        for name, problem in problems.items()
    )
    return "\n  ".join(lines)


def echo_dry_run(migrations: Sequence[Migration], direction: MigrationDirection) -> None:
    """Print the SQL that ``direction`` would run for each migration, under a header per migration.

    Uses ``click.echo`` rather than logging, so ``--quiet`` does not hide it.
    """
    for position, migration in enumerate(migrations):
        if position:
            click.echo("")
        header = f"-- {migration.name} ({direction})"
        click.echo(click.style(header, fg=_DRY_RUN_COLORS[direction], bold=True))
        click.echo(migration.sql(direction).strip())


def _applied_section(status: MigrationStatus, *, show_all: bool) -> list[str]:
    lines = [click.style("Applied:", bold=True)]
    if not status.applied:
        lines.append("  none")
        return lines
    visible = status.applied if show_all else status.applied[:_VISIBLE_APPLIED_LIMIT]
    lines.extend(_applied_line(name, status) for name in visible)
    hidden_count = len(status.applied) - len(visible)
    if hidden_count:
        lines.append(f"  ... and {hidden_count} more applied")
    return lines


def _applied_line(name: str, status: MigrationStatus) -> str:
    marker = click.style("[X]", fg=_GREEN)
    labels = status.labels(name)
    if not labels:
        return f"  {marker} {name}"
    color = _PROBLEM_COLORS.get(status.problems.get(name, ""), "cyan")
    label_text = ", ".join(labels)
    suffix = click.style(f"({label_text})", fg=color)
    return f"  {marker} {name} {suffix}"


def _pending_section(pending: Sequence[str]) -> list[str]:
    title = click.style("Pending:", bold=True)
    if not pending:
        return [f"{title} none"]
    marker = click.style("[ ]", dim=True)
    return [title, *(f"  {marker} {name}" for name in pending)]
