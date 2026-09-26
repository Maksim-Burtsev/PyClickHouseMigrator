## What

<!-- What does this change, and why? Link an issue if there is one. -->

## Checklist

- [ ] `uv run ruff check . && uv run ruff format --check .`
- [ ] `uv run flake8 .` (wemake-python-styleguide)
- [ ] `uv run mypy`
- [ ] `uv run python scripts/check_no_comments.py`
- [ ] `uv run pytest` passes against a live ClickHouse
- [ ] Tests added or updated for the behavior change
- [ ] Docs / README / `llms.txt` updated if the CLI surface changed
- [ ] `CHANGELOG.md` entry added
