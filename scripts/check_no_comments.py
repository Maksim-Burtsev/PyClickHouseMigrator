"""Fail if Python source files contain comments.

Comments are banned in this repository, tool directives such as ``noqa`` and
``type: ignore`` included: code explains itself through names, types, and
docstrings. See the "No comments" section of AGENTS.md.

Usage: ``python scripts/check_no_comments.py [PATH ...]``. Directories are
scanned recursively for ``*.py`` files. Without arguments the package, tests,
and scripts directories are checked.
"""

from __future__ import annotations

import sys
import tokenize
from collections.abc import Iterator
from pathlib import Path

DEFAULT_PATHS = ("py_clickhouse_migrator", "tests", "scripts")


def _iter_python_files(paths: list[str]) -> Iterator[Path]:
    for raw_path in paths:
        path = Path(raw_path)
        if path.is_dir():
            yield from sorted(path.rglob("*.py"))
        elif path.suffix == ".py":
            yield path


def _find_comments(path: Path) -> Iterator[tuple[int, str]]:
    with path.open("rb") as source:
        for token in tokenize.tokenize(source.readline):
            if token.type == tokenize.COMMENT:
                yield token.start[0], token.string


def main(argv: list[str]) -> int:
    """Print every comment found in ``argv`` paths; return 1 if there are any, else 0."""
    violations = [
        f"{path}:{line}: {text}"
        for path in _iter_python_files(argv or list(DEFAULT_PATHS))
        for line, text in _find_comments(path)
    ]
    if not violations:
        return 0
    sys.stdout.write("".join(f"{violation}\n" for violation in violations))
    sys.stderr.write(f"Found {len(violations)} comment(s). Comments are banned, see AGENTS.md.\n")
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
