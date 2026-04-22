-- migrator:up
-- @stmt
CREATE TABLE IF NOT EXISTS events_local ON CLUSTER my_cluster
(
    id UInt64,
    event_name LowCardinality(String),
    created_at DateTime
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (created_at, id)

-- @stmt
CREATE TABLE IF NOT EXISTS events ON CLUSTER my_cluster
AS events_local
ENGINE = Distributed(my_cluster, currentDatabase(), events_local, rand())

-- migrator:down
-- @stmt
DROP TABLE IF EXISTS events ON CLUSTER my_cluster

-- @stmt
DROP TABLE IF EXISTS events_local ON CLUSTER my_cluster
