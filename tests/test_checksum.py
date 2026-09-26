from __future__ import annotations

import os

import click
import pytest
from click.testing import CliRunner
from clickhouse_driver import Client

from py_clickhouse_migrator.checksum import compute_checksum, normalize_content
from py_clickhouse_migrator.errors import ChecksumMismatchError, InvalidMigrationError
from py_clickhouse_migrator.migrator import (
    DEFAULT_MIGRATIONS_DIR,
    Migrator,
)

from py_clickhouse_migrator.cli import main
from tests.helpers import create_test_migration, render_test_migration_content, render_test_migration_section

SHA256_HEX_DIGEST_LENGTH = 64


def test_normalize_strips_trailing_whitespace() -> None:
    content = "SELECT 1;   \nSELECT 2;\t\n"
    result = normalize_content(content)
    assert result == "SELECT 1;\nSELECT 2;"


def test_normalize_unifies_line_endings() -> None:
    content = "line1\r\nline2\rline3\n"
    result = normalize_content(content)
    assert result == "line1\nline2\nline3"


def test_normalize_strips_leading_trailing_blank_lines() -> None:
    content = "\n\n  SELECT 1;\n\n"
    result = normalize_content(content)
    assert result == "  SELECT 1;"


def test_normalize_filters_blank_lines_between_statements() -> None:
    assert normalize_content("SELECT 1;\n\nSELECT 2;") == "SELECT 1;\nSELECT 2;"


def test_normalize_filters_multiple_blank_lines() -> None:
    assert normalize_content("SELECT 1;\n\n\n\nSELECT 2;") == "SELECT 1;\nSELECT 2;"


def test_normalize_filters_blank_lines_at_edges() -> None:
    assert normalize_content("\n\nSELECT 1;\n\n") == "SELECT 1;"


def test_normalize_filters_blank_lines_with_spaces() -> None:
    assert normalize_content("SELECT 1;\n   \n  \nSELECT 2;") == "SELECT 1;\nSELECT 2;"


def test_checksum_deterministic() -> None:
    up = render_test_migration_section(["SELECT 1;", "SELECT 2;"])
    rb = render_test_migration_section("SELECT 1;")
    assert compute_checksum(up, rb) == compute_checksum(up, rb)


def test_checksum_ignores_whitespace_changes() -> None:
    up_1 = render_test_migration_section("SELECT 1;   \r\nSELECT 2;\t\n")
    up_2 = render_test_migration_section("SELECT 1;\nSELECT 2;\n")
    rb = render_test_migration_section("SELECT 1;")
    assert compute_checksum(up_1, rb) == compute_checksum(up_2, rb)


def test_checksum_detects_content_changes() -> None:
    rb = render_test_migration_section("SELECT 1;")
    assert compute_checksum(render_test_migration_section("SELECT 1;"), rb) != compute_checksum(
        render_test_migration_section("SELECT 2;"), rb
    )


def test_checksum_stable_across_blank_lines() -> None:
    up_1 = render_test_migration_section("SELECT 1;\n\nSELECT 2;")
    up_2 = render_test_migration_section("SELECT 1;\nSELECT 2;")
    rb = render_test_migration_section("SELECT 1;")
    assert compute_checksum(up_1, rb) == compute_checksum(up_2, rb)


def test_checksum_uses_both_up_and_rollback() -> None:
    up = render_test_migration_section("CREATE TABLE t (id Int32) ENGINE = MergeTree ORDER BY id")
    rb_1 = render_test_migration_section("DROP TABLE t")
    rb_2 = render_test_migration_section("DROP TABLE IF EXISTS t")
    assert compute_checksum(up, rb_1) != compute_checksum(up, rb_2)


def test_checksum_no_collision_on_concatenation() -> None:
    assert compute_checksum(
        render_test_migration_section("SELECT 1;"),
        render_test_migration_section(["SELECT 2;", "SELECT 3;"]),
    ) != compute_checksum(
        render_test_migration_section(["SELECT 1;", "SELECT 2;"]),
        render_test_migration_section("SELECT 3;"),
    )


def test_checksum_saved_on_apply(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """After up(), db_migrations should contain a non-empty checksum."""
    create_test_migration(
        name="test_cksum",
        up="CREATE TABLE IF NOT EXISTS test_cksum (id Int32) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_cksum",
    )
    migrator.up()

    row = ch_client.execute("SELECT checksum FROM db_migrations LIMIT 1")[0]
    assert row[0] != ""
    assert len(row[0]) == SHA256_HEX_DIGEST_LENGTH

    ch_client.execute("DROP TABLE IF EXISTS test_cksum")


def test_up_fails_on_checksum_mismatch(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """Modifying a migration file after apply should cause ChecksumMismatchError on next up()."""
    filename = create_test_migration(
        name="test_mismatch",
        up="CREATE TABLE IF NOT EXISTS test_mismatch (id Int32) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_mismatch",
    )
    migrator.up()

    filepath = f"{DEFAULT_MIGRATIONS_DIR}/{filename}"
    with open(filepath, "w") as f:
        f.write(
            render_test_migration_content(
                up="CREATE TABLE IF NOT EXISTS test_mismatch (id Int32, name String) Engine=MergeTree() ORDER BY id;",
                rollback="DROP TABLE IF EXISTS test_mismatch",
            )
        )

    with pytest.raises(ChecksumMismatchError, match="Checksum mismatch"):
        migrator.up()

    ch_client.execute("DROP TABLE IF EXISTS test_mismatch")


def test_up_allow_dirty_skips_validation(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """allow_dirty=True should not raise on checksum mismatch."""
    filename = create_test_migration(
        name="test_dirty",
        up="CREATE TABLE IF NOT EXISTS test_dirty (id Int32) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_dirty",
    )
    migrator.up()

    filepath = f"{DEFAULT_MIGRATIONS_DIR}/{filename}"
    with open(filepath, "w") as f:
        f.write(
            render_test_migration_content(
                up="CREATE TABLE IF NOT EXISTS test_dirty (id Int32, name String) Engine=MergeTree() ORDER BY id;",
                rollback="DROP TABLE IF EXISTS test_dirty",
            )
        )

    migrator.up(allow_dirty=True)

    ch_client.execute("DROP TABLE IF EXISTS test_dirty")


def test_up_skips_validation_for_empty_checksum(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """Legacy migrations without checksum should not trigger validation errors."""
    ch_client.execute(
        "INSERT INTO db_migrations (name, up, rollback, checksum) VALUES",
        [["legacy.sql", "SELECT 1", "SELECT 1", ""]],
    )

    create_test_migration(
        name="test_after_legacy",
        up="CREATE TABLE IF NOT EXISTS test_legacy (id Int32) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_legacy",
    )

    migrator.up()

    ch_client.execute("DROP TABLE IF EXISTS test_legacy")


def test_validate_detects_missing_file(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """validate_checksums should detect missing migration files."""
    filename = create_test_migration(
        name="test_missing",
        up="CREATE TABLE IF NOT EXISTS test_missing (id Int32) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_missing",
    )
    migrator.up()

    os.remove(f"{DEFAULT_MIGRATIONS_DIR}/{filename}")

    mismatches = migrator.validate_checksums()
    assert len(mismatches) == 1
    assert mismatches[0].name == filename
    assert mismatches[0].actual == ""

    ch_client.execute("DROP TABLE IF EXISTS test_missing")


def test_validate_passes_when_no_changes(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """No mismatches when files are unchanged."""
    create_test_migration(
        name="test_ok",
        up="CREATE TABLE IF NOT EXISTS test_ok (id Int32) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_ok",
    )
    migrator.up()

    assert migrator.validate_checksums() == []

    ch_client.execute("DROP TABLE IF EXISTS test_ok")


def test_validate_raises_for_invalid_applied_migration_file(
    migrator: Migrator,
    migrator_init: None,
    ch_client: Client,
) -> None:
    filename = create_test_migration(
        name="test_invalid_checksum_file",
        up="CREATE TABLE IF NOT EXISTS test_invalid_checksum_file (id Int32) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_invalid_checksum_file",
    )
    migrator.up()

    filepath = f"{DEFAULT_MIGRATIONS_DIR}/{filename}"
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("-- migrator:up\nSELECT 1;\n-- migrator:down\n")

    with pytest.raises(InvalidMigrationError, match=r"outside '-- @stmt' blocks"):
        migrator.validate_checksums()

    ch_client.execute("DROP TABLE IF EXISTS test_invalid_checksum_file")


def test_validate_ignores_baselined_missing_file(migrator: Migrator, migrator_init: None) -> None:
    filename = create_test_migration(
        name="baseline_missing",
        up="CREATE TABLE IF NOT EXISTS baseline_missing (id Int32) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS baseline_missing",
    )
    migrator.baseline()

    os.remove(f"{DEFAULT_MIGRATIONS_DIR}/{filename}")

    assert migrator.validate_checksums() == []


def test_repair_updates_checksum(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """repair() should update checksum in DB after file modification.

    The update runs with mutations_sync=1, so the next validation sees it without waiting.
    """
    filename = create_test_migration(
        name="test_repair",
        up="CREATE TABLE IF NOT EXISTS test_repair (id Int32) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_repair",
    )
    migrator.up()

    filepath = f"{DEFAULT_MIGRATIONS_DIR}/{filename}"
    with open(filepath, "w") as f:
        f.write(
            render_test_migration_content(
                up="CREATE TABLE IF NOT EXISTS test_repair (id Int32, v String) Engine=MergeTree() ORDER BY id;",
                rollback="DROP TABLE IF EXISTS test_repair",
            )
        )

    assert len(migrator.validate_checksums()) == 1

    repaired = migrator.repair()
    assert repaired == [filename]

    assert migrator.validate_checksums() == []

    ch_client.execute("DROP TABLE IF EXISTS test_repair")


def test_repair_nothing_to_fix(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """repair() returns empty list when nothing is broken."""
    create_test_migration(
        name="test_repair_ok",
        up="CREATE TABLE IF NOT EXISTS test_repair_ok (id Int32) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_repair_ok",
    )
    migrator.up()

    assert migrator.repair() == []

    ch_client.execute("DROP TABLE IF EXISTS test_repair_ok")


def test_repair_ignores_baselined_rows(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    filename = create_test_migration(
        name="baseline_repair",
        up="CREATE TABLE IF NOT EXISTS baseline_repair (id Int32) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS baseline_repair",
    )
    migrator.baseline()

    with open(f"{DEFAULT_MIGRATIONS_DIR}/{filename}", "w", encoding="utf-8") as f:
        f.write(
            render_test_migration_content(
                up="CREATE TABLE IF NOT EXISTS baseline_repair (id Int32, v String) Engine=MergeTree() ORDER BY id;",
                rollback="DROP TABLE IF EXISTS baseline_repair",
            )
        )

    assert migrator.repair() == []
    assert ch_client.execute("SELECT checksum FROM db_migrations WHERE name = %(name)s", {"name": filename}) == [("",)]


def test_repair_skips_missing_files(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """repair() should skip missing files and only log a warning."""
    filename = create_test_migration(
        name="test_repair_missing",
        up="CREATE TABLE IF NOT EXISTS test_repair_missing (id Int32) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_repair_missing",
    )
    migrator.up()

    os.remove(f"{DEFAULT_MIGRATIONS_DIR}/{filename}")

    repaired = migrator.repair()
    assert repaired == []

    ch_client.execute("DROP TABLE IF EXISTS test_repair_missing")


def test_show_clean_output(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """No integrity issues — no warning block."""
    create_test_migration(
        name="test_clean",
        up="CREATE TABLE IF NOT EXISTS test_clean (id Int32) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_clean",
    )
    migrator.up()

    output, warning = migrator.show_migrations()
    plain = click.unstyle(output)

    assert "[X]" in plain
    assert "(HEAD)" in plain
    assert "WARNING" not in plain
    assert warning == ""

    ch_client.execute("DROP TABLE IF EXISTS test_clean")


def test_show_baseline_suffix_without_warning_for_missing_file(migrator: Migrator, migrator_init: None) -> None:
    filename = create_test_migration(
        name="baseline_show",
        up="CREATE TABLE IF NOT EXISTS baseline_show (id Int32) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS baseline_show",
    )
    migrator.baseline()

    os.remove(f"{DEFAULT_MIGRATIONS_DIR}/{filename}")

    output, warning = migrator.show_migrations()
    plain = click.unstyle(output)

    assert f"{filename} (HEAD, baseline)" in plain
    assert warning == ""


def test_show_modified_suffix_and_warning(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """Modified migration should show (modified) suffix and WARNING block."""
    filename = create_test_migration(
        name="test_show_mod",
        up="CREATE TABLE IF NOT EXISTS test_show_mod (id Int32) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_show_mod",
    )
    migrator.up()

    filepath = f"{DEFAULT_MIGRATIONS_DIR}/{filename}"
    with open(filepath, "w") as f:
        f.write(
            render_test_migration_content(
                up="CREATE TABLE IF NOT EXISTS test_show_mod (id Int32, v String) Engine=MergeTree() ORDER BY id;",
                rollback="DROP TABLE IF EXISTS test_show_mod",
            )
        )

    output, warning = migrator.show_migrations()
    plain = click.unstyle(output)
    plain_warning = click.unstyle(warning)

    assert "(HEAD, modified)" in plain
    assert "WARNING: 1 integrity issue found" in plain_warning
    assert f"{filename}: checksum mismatch" in plain_warning

    ch_client.execute("DROP TABLE IF EXISTS test_show_mod")


def test_show_missing_suffix_and_warning(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """Missing migration file should show (missing) suffix and WARNING block."""
    filename = create_test_migration(
        name="test_show_miss",
        up="CREATE TABLE IF NOT EXISTS test_show_miss (id Int32) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_show_miss",
    )
    migrator.up()

    os.remove(f"{DEFAULT_MIGRATIONS_DIR}/{filename}")

    output, warning = migrator.show_migrations()
    plain = click.unstyle(output)
    plain_warning = click.unstyle(warning)

    assert "(HEAD, missing)" in plain
    assert "WARNING: 1 integrity issue found" in plain_warning
    assert f"{filename}: migration file missing" in plain_warning

    ch_client.execute("DROP TABLE IF EXISTS test_show_miss")


def test_show_head_without_issues(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """HEAD migration without issues shows only (HEAD), not (HEAD, )."""
    create_test_migration(
        name="test_head_ok",
        up="CREATE TABLE IF NOT EXISTS test_head_ok (id Int32) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_head_ok",
    )
    migrator.up()

    output, _ = migrator.show_migrations()
    plain = click.unstyle(output)

    assert "(HEAD)" in plain
    assert "(HEAD," not in plain

    ch_client.execute("DROP TABLE IF EXISTS test_head_ok")


def test_show_truncated_list_still_warns(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """Hidden migrations (beyond top 5) still appear in WARNING block."""
    for i in range(7):
        create_test_migration(
            name=f"table_{i}",
            up=f"CREATE TABLE IF NOT EXISTS t_{i} (id Int32) Engine=MergeTree() ORDER BY id;",
            rollback=f"DROP TABLE IF EXISTS t_{i}",
        )
    migrator.up()

    applied = migrator.get_applied_migrations_names()
    oldest = applied[0]
    filepath = f"{DEFAULT_MIGRATIONS_DIR}/{oldest}"
    with open(filepath, "w") as f:
        f.write(
            render_test_migration_content(
                up="CREATE TABLE IF NOT EXISTS modified_table (id Int32) Engine=MergeTree() ORDER BY id;",
                rollback="DROP TABLE IF EXISTS modified_table",
            )
        )

    output, warning = migrator.show_migrations()
    plain = click.unstyle(output)
    plain_warning = click.unstyle(warning)

    assert "... and 2 more applied" in plain
    assert "WARNING: 1 integrity issue found" in plain_warning
    assert f"{oldest}: checksum mismatch" in plain_warning

    for i in range(7):
        ch_client.execute(f"DROP TABLE IF EXISTS t_{i}")


def test_show_warning_plural(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """Multiple integrity issues should use plural 'issues'."""
    filename1 = create_test_migration(
        name="test_plural_1",
        up="CREATE TABLE IF NOT EXISTS test_plural_1 (id Int32) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_plural_1",
    )
    filename2 = create_test_migration(
        name="test_plural_2",
        up="CREATE TABLE IF NOT EXISTS test_plural_2 (id Int32) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_plural_2",
    )
    migrator.up()

    filepath1 = f"{DEFAULT_MIGRATIONS_DIR}/{filename1}"
    with open(filepath1, "w") as f:
        f.write(
            render_test_migration_content(
                up="CREATE TABLE IF NOT EXISTS test_plural_1 (id Int32, v String) Engine=MergeTree() ORDER BY id;",
                rollback="DROP TABLE IF EXISTS test_plural_1",
            )
        )
    os.remove(f"{DEFAULT_MIGRATIONS_DIR}/{filename2}")

    _, warning = migrator.show_migrations()
    plain_warning = click.unstyle(warning)

    assert "WARNING: 2 integrity issues found" in plain_warning
    assert f"{filename1}: checksum mismatch" in plain_warning
    assert f"{filename2}: migration file missing" in plain_warning

    ch_client.execute("DROP TABLE IF EXISTS test_plural_1")
    ch_client.execute("DROP TABLE IF EXISTS test_plural_2")


def test_show_warning_stderr(migrator: Migrator, migrator_init: None, ch_client: Client, test_db: str) -> None:
    """WARNING block should be routed to stderr via CLI."""
    filename = create_test_migration(
        name="test_stderr",
        up="CREATE TABLE IF NOT EXISTS test_stderr (id Int32) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_stderr",
    )
    migrator.up()

    filepath = f"{DEFAULT_MIGRATIONS_DIR}/{filename}"
    with open(filepath, "w") as f:
        f.write(
            render_test_migration_content(
                up="CREATE TABLE IF NOT EXISTS test_stderr (id Int32, v String) Engine=MergeTree() ORDER BY id;",
                rollback="DROP TABLE IF EXISTS test_stderr",
            )
        )

    runner = CliRunner()
    result = runner.invoke(main, ["--url", test_db, "show"])

    assert "WARNING" in click.unstyle(result.stderr)

    ch_client.execute("DROP TABLE IF EXISTS test_stderr")


def test_show_head_modified_combo_color(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """HEAD migration with modified file should show (HEAD, modified) suffix."""
    filename = create_test_migration(
        name="test_combo",
        up="CREATE TABLE IF NOT EXISTS test_combo (id Int32) Engine=MergeTree() ORDER BY id;",
        rollback="DROP TABLE IF EXISTS test_combo",
    )
    migrator.up()

    filepath = f"{DEFAULT_MIGRATIONS_DIR}/{filename}"
    with open(filepath, "w") as f:
        f.write(
            render_test_migration_content(
                up="CREATE TABLE IF NOT EXISTS test_combo (id Int32, v String) Engine=MergeTree() ORDER BY id;",
                rollback="DROP TABLE IF EXISTS test_combo",
            )
        )

    output, _ = migrator.show_migrations()
    plain = click.unstyle(output)

    assert "(HEAD, modified)" in plain
    assert click.style("(HEAD, modified)", fg="yellow") in output

    ch_client.execute("DROP TABLE IF EXISTS test_combo")
