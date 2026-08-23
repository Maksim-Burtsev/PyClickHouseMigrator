# Demo GIF for the README.
#
# The committed assets/demo.gif is rendered from this tape by hand and updated
# through a normal pull request. .github/workflows/demo.yml renders the tape on
# demand and uploads the result as an artifact, so you can check that the tape
# still works and download a fresh GIF without recording anything.
#
# To render locally:
#
#   docker compose -f docker-compose.test.yml up -d --wait
#   uv sync --dev && export PATH="$PWD/.venv/bin:$PATH"
#   vhs demo/demo.tape
#
# Run it from the repository root — the tape resolves demo/events.sql
# relative to the current directory before switching to a temp workspace.

Output assets/demo.gif

Require migrator

Set FontSize 16
Set Width 1000
Set Height 440
Set Padding 18
Set TypingSpeed 45ms
Set Theme "Catppuccin Mocha"

# Setup: point at ClickHouse and work in a throwaway directory.
Hide
Type "export CLICKHOUSE_MIGRATE_URL=clickhouse://default@localhost:19000/test"
Enter
Type "FIXTURE=$PWD/demo/events.sql; cd $(mktemp -d)"
Enter
Type "clear"
Enter
Show

Sleep 500ms

Type "migrator init"
Enter
Sleep 1500ms

Type "migrator new add_events"
Enter
Sleep 1500ms

# Write the DDL — off camera, this is just an editor step.
Hide
Type "cp $FIXTURE db/migrations/*_add_events.sql"
Enter
Type "clear"
Enter
Show

Type "cat db/migrations/*_add_events.sql"
Enter
Sleep 3s

Type "clear"
Enter

Type "migrator up --dry-run"
Enter
Sleep 3s

Type "clear"
Enter

Type "migrator up"
Enter
Sleep 1500ms

Type "migrator show"
Enter
Sleep 3s

Type "migrator rollback"
Enter
Sleep 2500ms
