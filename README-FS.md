# SQLite-FS: A Feature Store Extension for SQLite

SQLite-FS adds first-class **feature store** semantics directly into the SQLite SQL dialect. Define, materialize, and serve ML features with plain SQL -- no external infrastructure, no client-server overhead, no Python glue code.

```sql
CREATE FEATURE daily_spending
  ENTITY      transactions
  TIMESTAMP   txn_date
  GRANULARITY DAY
  DURATION    7
  RETAIN      30
AS (
  SELECT user_id, txn_date, SUM(amount) AS total, COUNT(*) AS txn_count
  FROM transactions
  GROUP BY user_id, txn_date
);

REFRESH FEATURE daily_spending;

-- Query materialized features directly
SELECT * FROM daily_spending WHERE txn_date = '2026-04-01';
```

## Why

Enterprise feature stores (Feast, Feathr, Tecton) assume cloud infrastructure, high concurrency, and petabyte scale. None of them work for:

- **On-device ML** -- mobile apps doing personalization with local data
- **Edge/IoT** -- sensor analytics on a Raspberry Pi with no internet
- **Prototyping** -- data scientists who just want features in a single `.db` file
- **Offline-first apps** -- privacy-preserving ML where data never leaves the device

SQLite is already deployed on billions of devices. SQLite-FS makes it a feature store too.

## How It Works

SQLite-FS extends the SQLite parser with four new statements. All logic lives in a single isolated file (`src/featurestore.c`), with minimal hooks into the core.

### `CREATE FEATURE`

Declares a feature with its full profile -- entity table, timestamp column, partition granularity, lookback window, refresh strategy, and retention policy. The inner query is stored as a VIEW for AST-level manipulation at refresh time.

```
CREATE FEATURE <name>
  ENTITY      <table>          -- entity table (join anchor)
  TIMESTAMP   <column>         -- event-time column in SELECT output
  GRANULARITY HOUR | DAY       -- partition unit
  [ DURATION  <n> ]            -- lookback window in granularity units
  [ REFRESH   FULL | INCREMENTAL ]
  [ RETAIN    <n> ]            -- max partitions to keep
AS ( <select> );
```

### `REFRESH FEATURE`

Materializes the current partition. Computes the partition key from `datetime('now')`, rewrites the stored query AST to inject time-range predicates, and inserts results into the physical feature table. Supports incremental (skip if exists) and full (replace) modes. Enforces retention by dropping the oldest partition when the count exceeds `RETAIN N`.

```sql
REFRESH FEATURE daily_spending;
```

### `DESCRIBE FEATURE`

Returns the feature profile and partition metadata.

```sql
DESCRIBE FEATURE daily_spending;
```

### `DROP FEATURE`

Removes the feature definition, backing view, physical table, and all partition data.

```sql
DROP FEATURE daily_spending;
```

## Internal Architecture

```
SQL: CREATE FEATURE ...
       |
  parse.y (grammar rules)
       |
  featurestore.c (all feature store logic)
       |
  +-- _sqlite_fs_features    (feature registry -- one row per feature)
  +-- _sqlite_fs_view_<name> (stored query as a VIEW for AST rewriting)
  +-- _sqlite_fs_feat_<name> (physical materialized data with version column)
```

The extension modifies five core files with minimal, mechanical edits:

| File | Change |
|------|--------|
| `src/parse.y` | ~15 lines: grammar rules for the four statements |
| `src/sqliteInt.h` | 1 line: `#include "featurestore.h"` |
| `src/main.c` | 1 line: register extension |
| `main.mk` | 3 lines: add `featurestore.o` to build |
| `tool/mkkeywordhash.c` | 1 line: add `FEATURE` keyword |

All implementation logic (~870 lines of C) is in `src/featurestore.c`.

## Building

SQLite-FS builds with the standard SQLite build system:

```bash
mkdir bld && cd bld
../configure --all --debug CFLAGS='-O0 -g'   # debug build
make sqlite3                                   # build CLI with feature store
```

## Current Status

This is a learning project -- a working proof of concept, not production-ready.

**Implemented:**
- `CREATE FEATURE` with full profile (entity, timestamp, granularity, duration, refresh mode, retention)
- `REFRESH FEATURE` with AST rewriting, incremental/full modes, and `RETAIN N` enforcement
- `DESCRIBE FEATURE` and `DROP FEATURE`
- Query features through auto-generated views

**Not yet implemented:**
- Backfilling historical partitions (`UPDATE FEATURE ... WHERE ...`)
- Shared-scan optimization across features with common source tables
- Entity PK resolution from `PRAGMA table_info`
- Comprehensive test suite

## Background

Inspired by the [SQL-ML paper](https://dl.acm.org/doi/10.1145/3626246.3653374) and its approach to embedding feature store semantics into the query engine. The full project proposal is in `guide/SQLite-fs.md`.

Built on SQLite 3.52.0. The SQLite source code is public domain.
