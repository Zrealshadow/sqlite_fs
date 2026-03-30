-- test.sql — Validation tests for CREATE FEATURE partition_column check
-- Run with: ./bld/sqlite3 < test.sql

-- Setup: base table
DROP TABLE IF EXISTS events;
CREATE TABLE events (
    user_id    INTEGER,
    event_date TEXT,
    action     TEXT
);
INSERT INTO events VALUES (1, '2026-01-01', 'click');
INSERT INTO events VALUES (2, '2026-01-01', 'view');
INSERT INTO events VALUES (1, '2026-01-02', 'click');

-- ✅ PASS: partition_column 'event_date' exists in SELECT output
CREATE FEATURE daily_clicks AS (
    SELECT user_id, event_date, COUNT(*) AS cnt
    FROM events
    GROUP BY user_id, event_date
) PARTITION BY event_date BY DAY;

-- ✅ PASS: partition_column matches an alias (AS event_date)
CREATE FEATURE daily_actions AS (
    SELECT user_id, action, date(event_date) AS event_date
    FROM events
) PARTITION BY event_date BY DAY;

-- ❌ FAIL: partition_column 'ts' not in SELECT output
CREATE FEATURE bad_partition AS (
    SELECT user_id, event_date, COUNT(*) AS cnt
    FROM events
    GROUP BY user_id, event_date
) PARTITION BY ts BY DAY;

-- ❌ FAIL: query references a table that does not exist
CREATE FEATURE bad_query AS (
    SELECT user_id, event_date FROM no_such_table
) PARTITION BY event_date BY DAY;

-- Verify only the two valid features were registered
SELECT feature_name, partition_column, granularity
FROM _sqlite_fs_features
ORDER BY feature_name;
