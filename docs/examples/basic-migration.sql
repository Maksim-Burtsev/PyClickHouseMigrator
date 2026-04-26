-- migrator:up
-- @stmt
CREATE TABLE IF NOT EXISTS users
(
    id UInt64,
    name String,
    email Nullable(String),
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY id

-- @stmt
CREATE TABLE IF NOT EXISTS user_events
(
    user_id UInt64,
    event_name LowCardinality(String),
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (created_at, user_id)

-- migrator:down
-- @stmt
DROP TABLE IF EXISTS user_events

-- @stmt
DROP TABLE IF EXISTS users
