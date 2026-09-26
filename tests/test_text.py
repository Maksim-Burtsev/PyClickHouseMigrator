"""Tests for the line helpers that migration parsing is built on."""

from py_clickhouse_migrator.text import find_marker_lines, split_at_markers, trim_blank_lines

STATEMENT_MARKER = "-- @stmt"


def test_trim_blank_lines_keeps_inner_blank_lines() -> None:
    """Only blank lines at the edges are dropped; blank lines inside the text stay."""
    lines = ["", "   ", "CREATE TABLE users (id UInt64)", "", "ORDER BY id", "   ", ""]

    assert trim_blank_lines(lines) == "CREATE TABLE users (id UInt64)\n\nORDER BY id"


def test_trim_blank_lines_of_only_blank_lines() -> None:
    """Lines holding only spaces and tabs trim down to an empty string."""
    assert not trim_blank_lines(["", "   ", "\t"])


def test_find_marker_lines_matches_stripped_lines() -> None:
    """A marker matches with surrounding whitespace, but not as part of a different line."""
    lines = ["  -- migrator:up  ", "SELECT 1;", "-- migrator:down", "-- not-a-marker:up"]

    assert find_marker_lines(lines, "-- migrator:up") == [0]
    assert find_marker_lines(lines, "-- migrator:down") == [2]


def test_split_at_markers_without_markers() -> None:
    """Without markers every line is preamble and there are no blocks."""
    lines = ["", "SELECT 1;"]

    assert split_at_markers(lines, STATEMENT_MARKER) == (lines, [])


def test_split_at_markers_returns_every_block() -> None:
    """Blocks run from one marker to the next, drop the marker lines, and include empty blocks."""
    lines = ["", "-- @stmt", "SELECT 1;", "", "  -- @stmt  ", "   ", "-- @stmt"]
    blocks = [["SELECT 1;", ""], ["   "], []]

    assert split_at_markers(lines, STATEMENT_MARKER) == ([""], blocks)
