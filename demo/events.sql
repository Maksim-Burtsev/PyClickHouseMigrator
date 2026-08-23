-- migrator:up
-- @stmt
CREATE TABLE IF NOT EXISTS events (
    id        UInt64,
    user_id   UInt64,
    name      String,
    ts        DateTime
) ENGINE = MergeTree
ORDER BY (ts, id)

-- migrator:down
-- @stmt
DROP TABLE IF EXISTS events
