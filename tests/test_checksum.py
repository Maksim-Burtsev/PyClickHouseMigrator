from pathlib import Path
from unittest.mock import ANY

import click
import pytest
from click.testing import CliRunner
from clickhouse_driver import Client

from py_clickhouse_migrator.checksum import compute_checksum, normalize_content
from py_clickhouse_migrator.cli import main
from py_clickhouse_migrator.errors import ChecksumMismatchError, InvalidMigrationError
from py_clickhouse_migrator.migrator import DEFAULT_MIGRATIONS_DIR, ChecksumMismatch, Migrator, ShowMigrationsResult
from tests.migration_files import create_test_migration, render_test_migration_section, write_test_migration
from tests.queries import drop_tables

SHA256_HEX_DIGEST_LENGTH = 64
SELECT_ONE_SECTION = render_test_migration_section("SELECT 1;")
SQL_OUTSIDE_STMT_BLOCKS = "-- migrator:up\nSELECT 1;\n-- migrator:down\n"
TRUNCATED_SHOW_TABLES = tuple(f"t_{index}" for index in range(7))


def _create_table_sql(table: str, columns: str = "id Int32") -> str:
    return f"CREATE TABLE IF NOT EXISTS {table} ({columns}) Engine=MergeTree() ORDER BY id;"


def _drop_table_sql(table: str) -> str:
    return f"DROP TABLE IF EXISTS {table}"


def _create_table_migration(table: str) -> str:
    return create_test_migration(name=table, up=_create_table_sql(table), rollback=_drop_table_sql(table))


def _add_column_to_migration(filename: str, table: str) -> None:
    write_test_migration(filename, up=_create_table_sql(table, "id Int32, v String"), rollback=_drop_table_sql(table))


def _migration_path(filename: str) -> Path:
    return Path(DEFAULT_MIGRATIONS_DIR, filename)


def _delete_migration_file(filename: str) -> None:
    _migration_path(filename).unlink()


def _show_unstyled(migrator: Migrator) -> ShowMigrationsResult:
    report = migrator.show_migrations()
    return ShowMigrationsResult(click.unstyle(report.output), click.unstyle(report.warning))


@pytest.mark.parametrize(
    ("raw_sql", "normalized_sql"),
    [
        pytest.param("SELECT 1;   \nSELECT 2;\t\n", "SELECT 1;\nSELECT 2;", id="strips-trailing-whitespace"),
        pytest.param("line1\r\nline2\rline3\n", "line1\nline2\nline3", id="unifies-line-endings"),
        pytest.param("\n\n  SELECT 1;\n\n", "  SELECT 1;", id="strips-outer-blank-lines-keeps-indent"),
        pytest.param("SELECT 1;\n\nSELECT 2;", "SELECT 1;\nSELECT 2;", id="filters-blank-line-between-statements"),
        pytest.param("SELECT 1;\n\n\n\nSELECT 2;", "SELECT 1;\nSELECT 2;", id="filters-multiple-blank-lines"),
        pytest.param("\n\nSELECT 1;\n\n", "SELECT 1;", id="filters-blank-lines-at-edges"),
        pytest.param("SELECT 1;\n   \n  \nSELECT 2;", "SELECT 1;\nSELECT 2;", id="filters-whitespace-only-lines"),
    ],
)
def test_normalize_content(raw_sql: str, normalized_sql: str) -> None:
    """Stored checksums hash normalized SQL, so these rules must never change for applied migrations."""
    assert normalize_content(raw_sql) == normalized_sql


def test_checksum_deterministic() -> None:
    """The same sections always hash alike, so an untouched file is never reported as modified."""
    up = render_test_migration_section(["SELECT 1;", "SELECT 2;"])
    assert compute_checksum(up, SELECT_ONE_SECTION) == compute_checksum(up, SELECT_ONE_SECTION)


def test_checksum_ignores_whitespace_changes() -> None:
    """Trailing whitespace and CRLF line endings do not count as edits of an applied migration."""
    messy_up = render_test_migration_section("SELECT 1;   \r\nSELECT 2;\t\n")
    clean_up = render_test_migration_section("SELECT 1;\nSELECT 2;\n")
    assert compute_checksum(messy_up, SELECT_ONE_SECTION) == compute_checksum(clean_up, SELECT_ONE_SECTION)


def test_checksum_detects_content_changes() -> None:
    """Editing a statement changes the checksum, which is how drift in applied migrations is caught."""
    edited_up = render_test_migration_section("SELECT 2;")
    assert compute_checksum(SELECT_ONE_SECTION, SELECT_ONE_SECTION) != compute_checksum(edited_up, SELECT_ONE_SECTION)


def test_checksum_stable_across_blank_lines() -> None:
    """Blank lines inside a statement block do not count as edits of an applied migration."""
    spaced_up = render_test_migration_section("SELECT 1;\n\nSELECT 2;")
    compact_up = render_test_migration_section("SELECT 1;\nSELECT 2;")
    assert compute_checksum(spaced_up, SELECT_ONE_SECTION) == compute_checksum(compact_up, SELECT_ONE_SECTION)


def test_checksum_uses_both_up_and_rollback() -> None:
    """Edits to the rollback section are detected too, not only edits to the up section."""
    up = render_test_migration_section("CREATE TABLE t (id Int32) ENGINE = MergeTree ORDER BY id")
    plain_rollback = render_test_migration_section("DROP TABLE t")
    guarded_rollback = render_test_migration_section("DROP TABLE IF EXISTS t")
    assert compute_checksum(up, plain_rollback) != compute_checksum(up, guarded_rollback)


def test_checksum_no_collision_on_concatenation() -> None:
    """Moving a statement across the up/rollback boundary changes the checksum: sections are delimited."""
    one_up_two_rollback = compute_checksum(
        SELECT_ONE_SECTION,
        render_test_migration_section(["SELECT 2;", "SELECT 3;"]),
    )
    two_up_one_rollback = compute_checksum(
        render_test_migration_section(["SELECT 1;", "SELECT 2;"]),
        render_test_migration_section("SELECT 3;"),
    )
    assert one_up_two_rollback != two_up_one_rollback


def test_checksum_saved_on_apply(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """Applying a migration stores a SHA-256 hex checksum that later runs compare the file against."""
    _create_table_migration("test_cksum")
    migrator.up()

    stored_checksum = ch_client.execute("SELECT checksum FROM db_migrations LIMIT 1")[0][0]
    assert stored_checksum
    assert len(stored_checksum) == SHA256_HEX_DIGEST_LENGTH

    drop_tables(ch_client, "test_cksum")


def test_up_fails_on_checksum_mismatch(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """Editing an applied migration file blocks the next up(), so drift from the database is never silent."""
    filename = _create_table_migration("test_mismatch")
    migrator.up()
    _add_column_to_migration(filename, "test_mismatch")

    with pytest.raises(ChecksumMismatchError, match="Checksum mismatch"):
        migrator.up()

    drop_tables(ch_client, "test_mismatch")


def test_up_allow_dirty_skips_validation(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """allow_dirty=True is the escape hatch that lets up() run despite an edited applied migration."""
    filename = _create_table_migration("test_dirty")
    migrator.up()
    _add_column_to_migration(filename, "test_dirty")

    migrator.up(allow_dirty=True)

    drop_tables(ch_client, "test_dirty")


def test_up_skips_validation_for_empty_checksum(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """Rows applied before checksums existed have an empty checksum and must not fail validation."""
    ch_client.execute(
        "INSERT INTO db_migrations (name, up, rollback, checksum) VALUES",
        [["legacy.sql", "SELECT 1", "SELECT 1", ""]],
    )
    _create_table_migration("test_legacy")

    migrator.up()

    drop_tables(ch_client, "test_legacy")


def test_validate_detects_missing_file(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """A deleted applied migration file is reported as the only mismatch, with an empty actual checksum."""
    filename = _create_table_migration("test_missing")
    migrator.up()
    _delete_migration_file(filename)

    assert migrator.validate_checksums() == [ChecksumMismatch(name=filename, stored=ANY, actual="")]

    drop_tables(ch_client, "test_missing")


def test_validate_passes_when_no_changes(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """Unchanged applied migration files produce no mismatches."""
    _create_table_migration("test_ok")
    migrator.up()

    assert migrator.validate_checksums() == []

    drop_tables(ch_client, "test_ok")


def test_validate_raises_for_unparsable_file(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """An applied file that no longer parses raises instead of being reported as a plain checksum mismatch."""
    filename = _create_table_migration("test_invalid_checksum_file")
    migrator.up()
    _migration_path(filename).write_text(SQL_OUTSIDE_STMT_BLOCKS, encoding="utf-8")

    with pytest.raises(InvalidMigrationError, match=r"outside '-- @stmt' blocks"):
        migrator.validate_checksums()

    drop_tables(ch_client, "test_invalid_checksum_file")


def test_validate_ignores_baselined_missing_file(migrator: Migrator, migrator_init: None) -> None:
    """Baselined rows carry no checksum, so deleting their files is not an integrity issue."""
    filename = _create_table_migration("baseline_missing")
    migrator.baseline()
    _delete_migration_file(filename)

    assert migrator.validate_checksums() == []


def test_repair_updates_checksum(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """repair() stores the edited file's checksum, which clears the mismatch.

    The update runs with mutations_sync=1, so the next validation sees it without waiting.
    """
    filename = _create_table_migration("test_repair")
    migrator.up()
    _add_column_to_migration(filename, "test_repair")

    assert len(migrator.validate_checksums()) == 1
    assert migrator.repair() == [filename]
    assert migrator.validate_checksums() == []

    drop_tables(ch_client, "test_repair")


def test_repair_nothing_to_fix(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """repair() reports no repaired migrations when every stored checksum matches its file."""
    _create_table_migration("test_repair_ok")
    migrator.up()

    assert migrator.repair() == []

    drop_tables(ch_client, "test_repair_ok")


def test_repair_ignores_baselined_rows(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """Baselined rows keep their empty checksum even after their file changes."""
    filename = _create_table_migration("baseline_repair")
    migrator.baseline()
    _add_column_to_migration(filename, "baseline_repair")

    assert migrator.repair() == []
    assert ch_client.execute("SELECT checksum FROM db_migrations WHERE name = %(name)s", {"name": filename}) == [("",)]


def test_repair_skips_missing_files(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """repair() leaves a migration with a missing file alone instead of storing an empty checksum."""
    filename = _create_table_migration("test_repair_missing")
    migrator.up()
    _delete_migration_file(filename)

    assert migrator.repair() == []

    drop_tables(ch_client, "test_repair_missing")


def test_show_clean_output(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """Without integrity issues, show marks the applied HEAD and prints no warning block."""
    _create_table_migration("test_clean")
    migrator.up()

    report = migrator.show_migrations()
    plain_output = click.unstyle(report.output)

    assert "[X]" in plain_output
    assert "(HEAD)" in plain_output
    assert "WARNING" not in plain_output
    assert not report.warning

    drop_tables(ch_client, "test_clean")


def test_show_baseline_missing_file_no_warning(migrator: Migrator, migrator_init: None) -> None:
    """A baselined migration keeps its baseline suffix and raises no warning after its file is deleted."""
    filename = _create_table_migration("baseline_show")
    migrator.baseline()
    _delete_migration_file(filename)

    report = migrator.show_migrations()

    assert f"{filename} (HEAD, baseline)" in click.unstyle(report.output)
    assert not report.warning


def test_show_modified_suffix_and_warning(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """An edited applied file gets the (modified) suffix and a checksum-mismatch warning."""
    filename = _create_table_migration("test_show_mod")
    migrator.up()
    _add_column_to_migration(filename, "test_show_mod")

    plain_output, plain_warning = _show_unstyled(migrator)

    assert "(HEAD, modified)" in plain_output
    assert "WARNING: 1 integrity issue found" in plain_warning
    assert f"{filename}: checksum mismatch" in plain_warning

    drop_tables(ch_client, "test_show_mod")


def test_show_missing_suffix_and_warning(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """A deleted applied file gets the (missing) suffix and a missing-file warning."""
    filename = _create_table_migration("test_show_miss")
    migrator.up()
    _delete_migration_file(filename)

    plain_output, plain_warning = _show_unstyled(migrator)

    assert "(HEAD, missing)" in plain_output
    assert "WARNING: 1 integrity issue found" in plain_warning
    assert f"{filename}: migration file missing" in plain_warning

    drop_tables(ch_client, "test_show_miss")


def test_show_head_without_issues(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """A healthy HEAD migration shows a bare (HEAD) suffix, never a dangling '(HEAD, )'."""
    _create_table_migration("test_head_ok")
    migrator.up()

    plain_output = _show_unstyled(migrator).output

    assert "(HEAD)" in plain_output
    assert "(HEAD," not in plain_output

    drop_tables(ch_client, "test_head_ok")


def test_show_truncated_list_still_warns(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """Show lists only the 5 newest applied migrations, yet issues in the hidden ones still reach the warning."""
    for table in TRUNCATED_SHOW_TABLES:
        _create_table_migration(table)
    migrator.up()
    oldest = migrator.get_applied_migrations_names()[0]
    write_test_migration(oldest, up=_create_table_sql("modified_table"), rollback=_drop_table_sql("modified_table"))

    plain_output, plain_warning = _show_unstyled(migrator)

    assert "... and 2 more applied" in plain_output
    assert "WARNING: 1 integrity issue found" in plain_warning
    assert f"{oldest}: checksum mismatch" in plain_warning

    drop_tables(ch_client, *TRUNCATED_SHOW_TABLES)


def test_show_warning_plural(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """Several integrity issues are counted with the plural 'issues' and each one is listed."""
    modified_filename = _create_table_migration("test_plural_modified")
    missing_filename = _create_table_migration("test_plural_missing")
    migrator.up()
    _add_column_to_migration(modified_filename, "test_plural_modified")
    _delete_migration_file(missing_filename)

    plain_warning = _show_unstyled(migrator).warning

    assert "WARNING: 2 integrity issues found" in plain_warning
    assert f"{modified_filename}: checksum mismatch" in plain_warning
    assert f"{missing_filename}: migration file missing" in plain_warning

    drop_tables(ch_client, "test_plural_modified", "test_plural_missing")


def test_show_warning_stderr(migrator: Migrator, migrator_init: None, ch_client: Client, test_db: str) -> None:
    """The CLI prints the integrity warning to stderr, apart from the status report."""
    filename = _create_table_migration("test_stderr")
    migrator.up()
    _add_column_to_migration(filename, "test_stderr")

    show_run = CliRunner().invoke(main, ["--url", test_db, "show"])

    assert "WARNING" in click.unstyle(show_run.stderr)

    drop_tables(ch_client, "test_stderr")


def test_show_head_modified_combo_color(migrator: Migrator, migrator_init: None, ch_client: Client) -> None:
    """A modified HEAD migration gets one combined (HEAD, modified) suffix, colored yellow."""
    filename = _create_table_migration("test_combo")
    migrator.up()
    _add_column_to_migration(filename, "test_combo")

    styled_output = migrator.show_migrations().output

    assert "(HEAD, modified)" in click.unstyle(styled_output)
    assert click.style("(HEAD, modified)", fg="yellow") in styled_output

    drop_tables(ch_client, "test_combo")
