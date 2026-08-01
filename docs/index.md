---
title: PyClickHouseMigrator
description: SQL-first ClickHouse migrations for Python teams.
hide:
  - navigation
  - toc
---

<div class="pcm-home">

<section class="pcm-hero">
  <div class="pcm-hero__copy">
    <p class="pcm-eyebrow">Python CLI for ClickHouse</p>
    <h1><span>Plain SQL.</span><span>Auditable migrations.</span></h1>
    <p class="pcm-hero__lead">Version schema changes in Git, validate them before execution, and run them from the CLI or CI.</p>
    <div class="pcm-actions">
      <a class="pcm-button pcm-button--primary" href="migration-format.md">Documentation</a>
      <a class="pcm-button pcm-button--secondary" href="#actual-flow">How it works</a>
    </div>
  </div>

  <figure class="pcm-hero__visual">
    <img src="assets/hero-mascot.webp" alt="PyClickHouseMigrator mascot carrying ClickHouse columns">
    <figcaption><code>pip install py-clickhouse-migrator</code></figcaption>
  </figure>
</section>

<section class="pcm-code-story">
  <div class="pcm-section-copy">
    <h2>Every query stays explicit.</h2>
    <p>Each <code>-- @stmt</code> block is sent to ClickHouse as one query. The same file carries the up and down SQL.</p>
    <a class="pcm-text-link" href="migration-format.md">Format guide <span aria-hidden="true">→</span></a>
  </div>

  <div class="pcm-code-story__code" markdown>
```sql
-- migrator:up
-- @stmt
CREATE TABLE IF NOT EXISTS events
(
    id UInt64,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY id

-- migrator:down
-- @stmt
DROP TABLE IF EXISTS events
```
  </div>
</section>

<section class="pcm-capabilities">
  <div class="pcm-section-copy">
    <h2>A focused migration workflow</h2>
    <p>No ORM, schema diff, or framework. You write the SQL. The migrator handles ordering, checks, state, and advisory locking.</p>
  </div>

  <div class="pcm-bento">
    <article class="pcm-tile pcm-tile--wide">
      <h3>Plain SQL</h3>
      <p>Keep migrations in versioned files. Every statement boundary is explicit and reviewable in Git.</p>
      <code>20260801120000_create_events.sql</code>
    </article>
    <article class="pcm-tile pcm-tile--accent">
      <h3>Integrity checks</h3>
      <p>Detect edited or missing migrations that the tool previously applied.</p>
    </article>
    <article class="pcm-tile">
      <h3>Rollback you control</h3>
      <p>Write the down SQL yourself. The stored version is used for rollback.</p>
    </article>
    <article class="pcm-tile pcm-tile--code">
      <h3>Preflight validation</h3>
      <p><code>EXPLAIN AST</code> runs before up and rollback by default.</p>
    </article>
    <article class="pcm-tile">
      <h3>Deployment locking</h3>
      <p>Reduce common CI races with a TTL-based advisory lock.</p>
    </article>
    <article class="pcm-tile pcm-tile--soft">
      <h3>Cluster-aware state</h3>
      <p>Replicate service tables while leaving migration SQL unchanged.</p>
    </article>
  </div>
</section>

<section class="pcm-flow" id="actual-flow" aria-labelledby="pcm-flow-title">
  <div class="pcm-section-copy">
    <h2 id="pcm-flow-title">Inside <code>migrator up</code></h2>
    <p>By default, an apply run follows this path from connection to recorded state. Select a step to inspect it.</p>
  </div>

  <ol class="pcm-flow__steps" role="tablist" aria-label="migrator up execution steps">
    <li>
      <button class="pcm-flow__step" id="pcm-step-connect" type="button" role="tab" aria-selected="false" aria-controls="pcm-panel-connect" data-flow-step="connect">
        <span class="pcm-flow__icon" aria-hidden="true"><img src="assets/icons/square-terminal.svg" alt=""></span>
        <span class="pcm-flow__number">01</span>
        <strong>Connect</strong>
        <span class="pcm-flow__summary">Check ClickHouse and prepare the ledger.</span>
      </button>
    </li>
    <li>
      <button class="pcm-flow__step" id="pcm-step-lock" type="button" role="tab" aria-selected="false" aria-controls="pcm-panel-lock" data-flow-step="lock">
        <span class="pcm-flow__icon" aria-hidden="true"><img src="assets/icons/lock-keyhole.svg" alt=""></span>
        <span class="pcm-flow__number">02</span>
        <strong>Lock</strong>
        <span class="pcm-flow__summary">Acquire the TTL-based advisory lock.</span>
      </button>
    </li>
    <li>
      <button class="pcm-flow__step is-active" id="pcm-step-verify" type="button" role="tab" aria-selected="true" aria-controls="pcm-panel-verify" data-flow-step="verify" tabindex="0">
        <span class="pcm-flow__icon" aria-hidden="true"><img src="assets/icons/hash.svg" alt=""></span>
        <span class="pcm-flow__number">03</span>
        <strong>Verify</strong>
        <span class="pcm-flow__summary">Compare stored and local checksums.</span>
      </button>
    </li>
    <li>
      <button class="pcm-flow__step" id="pcm-step-validate" type="button" role="tab" aria-selected="false" aria-controls="pcm-panel-validate" data-flow-step="validate">
        <span class="pcm-flow__icon" aria-hidden="true"><img src="assets/icons/list-tree.svg" alt=""></span>
        <span class="pcm-flow__number">04</span>
        <strong>Validate</strong>
        <span class="pcm-flow__summary">Run EXPLAIN AST on pending SQL.</span>
      </button>
    </li>
    <li>
      <button class="pcm-flow__step" id="pcm-step-apply" type="button" role="tab" aria-selected="false" aria-controls="pcm-panel-apply" data-flow-step="apply">
        <span class="pcm-flow__icon" aria-hidden="true"><img src="assets/icons/list-start.svg" alt=""></span>
        <span class="pcm-flow__number">05</span>
        <strong>Apply</strong>
        <span class="pcm-flow__summary">Execute statement blocks in order.</span>
      </button>
    </li>
    <li>
      <button class="pcm-flow__step" id="pcm-step-record" type="button" role="tab" aria-selected="false" aria-controls="pcm-panel-record" data-flow-step="record">
        <span class="pcm-flow__icon" aria-hidden="true"><img src="assets/icons/list-plus.svg" alt=""></span>
        <span class="pcm-flow__number">06</span>
        <strong>Record</strong>
        <span class="pcm-flow__summary">Store SQL and checksum after success.</span>
      </button>
    </li>
  </ol>

  <div class="pcm-flow__panels">
    <article class="pcm-flow__panel" id="pcm-panel-connect" role="tabpanel" aria-labelledby="pcm-step-connect" data-flow-panel="connect" hidden>
      <div class="pcm-flow__panel-copy">
        <h3>Connect and prepare the ledger</h3>
        <p>Check the target with <code>SELECT 1</code> and ensure <code>db_migrations</code> exists. The database in the URL must already exist.</p>
      </div>
      <div class="pcm-terminal" aria-label="Migration service tables">
        <div><span class="pcm-terminal__prompt">$</span> <code>migrator show</code></div>
        <div>Applied:</div>
        <div class="pcm-terminal__indent">none</div>
        <div>Pending: none</div>
        <div><span class="pcm-terminal__ok">Applied: 0</span> | Pending: 0</div>
      </div>
    </article>

    <article class="pcm-flow__panel" id="pcm-panel-lock" role="tabpanel" aria-labelledby="pcm-step-lock" data-flow-panel="lock" hidden>
      <div class="pcm-flow__panel-copy">
        <h3>Acquire the advisory lock</h3>
        <p>Create or reuse <code>_migrations_lock</code>, then acquire a TTL-based advisory lock. By default, <code>up</code> retries three times.</p>
      </div>
      <div class="pcm-terminal" aria-label="Active migration lock">
        <div><span class="pcm-terminal__prompt">$</span> <code>migrator up</code></div>
        <div><span class="pcm-terminal__error">Error:</span> Migration lock is held by runner-01:1234:abc123ef</div>
        <div class="pcm-terminal__muted">since 2026-08-01 14:30:00, expires 2026-08-01 14:40:00</div>
        <div>Timed out after 3 retries.</div>
      </div>
    </article>

    <article class="pcm-flow__panel is-active" id="pcm-panel-verify" role="tabpanel" aria-labelledby="pcm-step-verify" data-flow-panel="verify">
      <div class="pcm-flow__panel-copy">
        <h3>Compare stored checksums</h3>
        <p>Compare stored SHA-256 values with the current up and down blocks. A mismatch or missing file stops the run by default.</p>
      </div>
      <div class="pcm-terminal" aria-label="Checksum mismatch output from migrator up">
        <div><span class="pcm-terminal__prompt">$</span> <code>migrator up</code></div>
        <div class="pcm-terminal__message"><strong>Error:</strong> Checksum mismatch for applied migrations:</div>
        <div class="pcm-terminal__indent">001_create_events.sql:</div>
        <div class="pcm-terminal__indent">stored=dd495f29709d... actual=6a406ca4f1d7...</div>
        <div class="pcm-terminal__muted">Run 'migrator repair' to update checksums.</div>
      </div>
    </article>

    <article class="pcm-flow__panel" id="pcm-panel-validate" role="tabpanel" aria-labelledby="pcm-step-validate" data-flow-panel="validate" hidden>
      <div class="pcm-flow__panel-copy">
        <h3>Preflight pending SQL</h3>
        <p>Run <code>EXPLAIN AST</code> for every pending statement before execution. Dry-run follows the same best-effort validation path.</p>
      </div>
      <div class="pcm-terminal" aria-label="Preflight query sent to ClickHouse">
        <div><span class="pcm-terminal__prompt">$</span> <code>migrator up --dry-run</code></div>
        <div class="pcm-terminal__accent">-- 002_add_user_id.sql (up)</div>
        <div>-- @stmt</div>
        <div>ALTER TABLE users</div>
        <div class="pcm-terminal__indent">ADD COLUMN IF NOT EXISTS user_id UInt64</div>
      </div>
    </article>

    <article class="pcm-flow__panel" id="pcm-panel-apply" role="tabpanel" aria-labelledby="pcm-step-apply" data-flow-panel="apply" hidden>
      <div class="pcm-flow__panel-copy">
        <h3>Execute statement blocks</h3>
        <p>Run migrations in filename order and each <code>-- @stmt</code> block in file order. Each block is one ClickHouse query.</p>
      </div>
      <div class="pcm-terminal" aria-label="Successful migration output">
        <div><span class="pcm-terminal__prompt">$</span> <code>migrator up</code></div>
        <div>001_create_events.sql applied <span class="pcm-terminal__ok">[✔]</span></div>
        <div>002_add_user_id.sql applied <span class="pcm-terminal__ok">[✔]</span></div>
      </div>
    </article>

    <article class="pcm-flow__panel" id="pcm-panel-record" role="tabpanel" aria-labelledby="pcm-step-record" data-flow-panel="record" hidden>
      <div class="pcm-flow__panel-copy">
        <h3>Store successful state</h3>
        <p>After each success, record its name, up SQL, down SQL, and SHA-256 checksum in <code>db_migrations</code>.</p>
      </div>
      <div class="pcm-terminal" aria-label="Migration status output">
        <div><span class="pcm-terminal__prompt">$</span> <code>migrator show</code></div>
        <div>Applied:</div>
        <div class="pcm-terminal__indent">[X] 002_add_user_id.sql (HEAD)</div>
        <div class="pcm-terminal__indent">[X] 001_create_events.sql</div>
        <div>Pending: none</div>
        <div><span class="pcm-terminal__ok">Applied: 2</span> | Pending: 0</div>
      </div>
    </article>
  </div>
</section>

<section class="pcm-guardrails" aria-labelledby="pcm-guardrails-title">
  <div class="pcm-section-copy">
    <h2 id="pcm-guardrails-title">Guardrails before execution.</h2>
    <p>The default path checks applied history, preflights pending SQL, and coordinates migration runners.</p>
  </div>

  <div class="pcm-guardrails__grid">
    <article class="pcm-guardrail">
      <div class="pcm-guardrail__heading">
        <span class="pcm-guardrail__icon" aria-hidden="true"><img src="assets/icons/hash.svg" alt=""></span>
        <div><h3>Checksum integrity</h3><p>Compares current blocks with the checksum stored at apply time.</p></div>
      </div>
      <div class="pcm-terminal pcm-terminal--compact" aria-label="Migration integrity status">
        <div><span class="pcm-terminal__prompt">$</span> <code>migrator show</code></div>
        <div>Applied:</div>
        <div class="pcm-terminal__indent">[X] 001_create_events.sql (HEAD, modified)</div>
        <div class="pcm-terminal__message"><strong>WARNING:</strong> 1 integrity issue found</div>
        <div class="pcm-terminal__indent">001_create_events.sql: checksum mismatch</div>
      </div>
      <p>A mismatch or missing checksummed file stops <code>migrator up</code> before pending SQL by default.</p>
    </article>

    <article class="pcm-guardrail">
      <div class="pcm-guardrail__heading">
        <span class="pcm-guardrail__icon" aria-hidden="true"><img src="assets/icons/list-tree.svg" alt=""></span>
        <div><h3>Preflight validation</h3><p>Runs <code>EXPLAIN AST</code> before up or rollback execution.</p></div>
      </div>
      <div class="pcm-terminal pcm-terminal--compact" aria-label="Dry run output after preflight validation">
        <div><span class="pcm-terminal__prompt">$</span> <code>migrator up --dry-run</code></div>
        <div class="pcm-terminal__accent">-- 002_add_user_id.sql (up)</div>
        <div>ALTER TABLE users</div>
        <div class="pcm-terminal__indent">ADD COLUMN IF NOT EXISTS user_id UInt64</div>
      </div>
      <p>Dry-run uses the same validation path. This is a best-effort check, not a safety guarantee.</p>
    </article>

    <article class="pcm-guardrail">
      <div class="pcm-guardrail__heading">
        <span class="pcm-guardrail__icon" aria-hidden="true"><img src="assets/icons/lock-keyhole.svg" alt=""></span>
        <div><h3>Advisory locking</h3><p>Uses a TTL-based lock to reduce concurrent migration races.</p></div>
      </div>
      <div class="pcm-terminal pcm-terminal--compact" aria-label="Active advisory lock information">
        <div><span class="pcm-terminal__prompt">$</span> <code>migrator lock-info</code></div>
        <div>Locked by: runner-01:1234:abc123ef</div>
        <div>Locked at: 2026-08-01 14:30:00</div>
        <div>Expires at: 2026-08-01 14:40:00</div>
      </div>
      <p>Up, rollback, and baseline use it by default. Keep one migration runner per deployment.</p>
    </article>
  </div>

  <div class="pcm-guardrails__footer">
    <a class="pcm-text-link" href="troubleshooting.md">Troubleshooting <span aria-hidden="true">→</span></a>
    <p><strong>ClickHouse DDL is not transactional.</strong> If a later statement fails, earlier statements may already have run. Prefer idempotent DDL where practical.</p>
  </div>
</section>

<section class="pcm-paths">
  <div class="pcm-section-copy">
    <h2>Choose your path</h2>
    <p>Pick the guide that matches your database and delivery process.</p>
  </div>

  <div class="pcm-paths__list">
    <a href="migration-format.md"><strong>New project</strong><span class="pcm-paths__description">Create a migration and preview pending SQL before it runs.</span><span class="pcm-paths__arrow" aria-hidden="true">→</span></a>
    <a href="baseline.md"><strong>Existing database</strong><span class="pcm-paths__description">Adopt historical files without replaying existing DDL.</span><span class="pcm-paths__arrow" aria-hidden="true">→</span></a>
    <a href="cluster-mode.md"><strong>Cluster deployment</strong><span class="pcm-paths__description">Replicate service state and keep cluster DDL explicit.</span><span class="pcm-paths__arrow" aria-hidden="true">→</span></a>
    <a href="ci-cd.md"><strong>CI/CD</strong><span class="pcm-paths__description">Run migrations once before the application rollout.</span><span class="pcm-paths__arrow" aria-hidden="true">→</span></a>
  </div>
</section>

</div>
