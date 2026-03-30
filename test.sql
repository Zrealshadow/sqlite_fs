-- test_feature.sql
CREATE FEATURE daily_clicks
AS (SELECT user_id, COUNT(*) AS clicks FROM events)
PARTITION BY created_at BY DAY;


SELECT feature_name, query_definition, partition_column, granularity
FROM _sqlite_fs_features;