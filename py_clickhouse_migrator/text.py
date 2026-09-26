"""Line helpers for parsing migration files."""

from itertools import pairwise

Lines = list[str]


def trim_blank_lines(lines: list[str]) -> str:
    """Join ``lines`` after dropping leading and trailing lines that contain only whitespace."""
    start = 0
    end = len(lines)
    while start < end and not lines[start].strip():
        start += 1
    while end > start and not lines[end - 1].strip():
        end -= 1
    return "\n".join(lines[start:end])


def split_at_markers(lines: Lines, marker: str) -> tuple[Lines, list[Lines]]:
    """Split ``lines`` at marker lines.

    Returns the lines before the first marker, and for each marker the lines up to the next one.
    """
    starts = find_marker_lines(lines, marker)
    if not starts:
        return lines, []
    bounds = pairwise([*starts, len(lines)])
    blocks = [lines[start + 1 : end] for start, end in bounds]
    return lines[: starts[0]], blocks


def find_marker_lines(lines: list[str], marker: str) -> list[int]:
    """Return indexes of the lines that consist of ``marker`` and optional surrounding whitespace."""
    return [index for index, line in enumerate(lines) if line.strip() == marker]
