/*
** featurestore.c -- SQLite-FS Feature Store Extension
**
** This file contains ALL feature store logic. The rest of the SQLite
** codebase interacts with this file only through:
**   sqlite3CreateFeature(), sqlite3RefreshFeature(), sqlite3DescribeFeature(),
**   sqlite3DropFeature() -- called from parse.y grammar actions.
**
** This design minimizes edits to core SQLite source files.
*/
#include "sqliteInt.h"

/* #include "featurestore.h"  -- already included via sqliteInt.h */

/* ============================================================
** Table and column name constants.
** All SQL in this file must reference these macros — never
** hardcode the strings directly, so a rename only touches here.
** ============================================================ */
#define SQLITEFS_META_TABLE "_sqlite_fs_features"
#define SQLITEFS_PART_TABLE "_sqlite_fs_partitions"
#define SQLITEFS_FEATURE_TABLE_PREFIX "_sqlite_fs_feat_"

/* _sqlite_fs_features columns */
#define SQLITEFS_COL_NAME       "feature_name"
#define SQLITEFS_COL_QUERY      "query_definition"
#define SQLITEFS_COL_SRCTABLES  "source_tables"
#define SQLITEFS_COL_ENTITYCOLS "entity_table"
#define SQLITEFS_COL_TSCOL      "timestamp_column"  /* raw event-time col in source */
#define SQLITEFS_COL_GRAN       "col_gran"           /* HOUR | DAY only */
#define SQLITEFS_COL_GRANEXPR   "gran_expr"          /* e.g. DATE(event_time) */
#define SQLITEFS_COL_WINDOW     "window_size"
#define SQLITEFS_COL_FEATTYPE   "feature_type"
#define SQLITEFS_COL_VALUECOLS  "value_columns"
#define SQLITEFS_COL_REFRESH    "refresh_mode"
#define SQLITEFS_COL_RETAIN     "retention_count"
#define SQLITEFS_COL_CREATED    "created_at"
#define SQLITEFS_COL_REFRESHED  "last_refreshed"

/* _sqlite_fs_partitions columns */
#define SQLITEFS_PCOL_FEATNAME "feature_name"
#define SQLITEFS_PCOL_PARTKEY "partition_key"
#define SQLITEFS_PCOL_ROWCOUNT "row_count"
#define SQLITEFS_PCOL_REFRESHED "refreshed_at"

/* eFeatureType values */
#define SQLITEFS_TYPE_SNAPSHOT 1
#define SQLITEFS_TYPE_AGGREGATE 2

/* eRefreshMode values */
#define SQLITEFS_REFRESH_INCR 1 /* skip if partition already exists */
#define SQLITEFS_REFRESH_FULL 2 /* delete existing rows, then recompute */

/* ============================================================
** DDL — assembled from the macros above via C string concatenation.
** ============================================================ */

/* _sqlite_fs_features: one row per CREATE FEATURE, stores full profile */
static const char sqlitefs_meta_ddl[] =
    "CREATE TABLE IF NOT EXISTS " SQLITEFS_META_TABLE "("
    "  " SQLITEFS_COL_NAME      " TEXT PRIMARY KEY,"
    "  " SQLITEFS_COL_QUERY     " TEXT NOT NULL,"    /* raw data scan, no GROUP BY */
    "  " SQLITEFS_COL_SRCTABLES " TEXT NOT NULL,"    /* JSON: ["t1","t2"] */
    "  " SQLITEFS_COL_ENTITYCOLS" TEXT NOT NULL,"    /* JSON: ["user_id"] */
    "  " SQLITEFS_COL_TSCOL     " TEXT NOT NULL,"    /* raw event-time column name */
    "  " SQLITEFS_COL_GRAN      " TEXT NOT NULL,"    /* HOUR | DAY */
    "  " SQLITEFS_COL_GRANEXPR  " TEXT NOT NULL,"    /* e.g. DATE(event_time) */
    "  " SQLITEFS_COL_WINDOW    " INTEGER,"           /* NULL = SNAPSHOT */
    "  " SQLITEFS_COL_FEATTYPE  " TEXT NOT NULL DEFAULT 'AGGREGATE',"
    "  " SQLITEFS_COL_VALUECOLS " TEXT NOT NULL,"    /* JSON: [{"name":...,"type":...,"expr":...}] */
    "  " SQLITEFS_COL_REFRESH   " TEXT NOT NULL DEFAULT 'INCREMENTAL',"
    "  " SQLITEFS_COL_RETAIN    " INTEGER,"           /* NULL = unlimited */
    "  " SQLITEFS_COL_CREATED   " TEXT DEFAULT (datetime('now')),"
    "  " SQLITEFS_COL_REFRESHED " TEXT"
    ")";

/* NEW: _sqlite_fs_partitions: one row per (feature, partition) after REFRESH */
static const char sqlitefs_part_ddl[] =
    "CREATE TABLE IF NOT EXISTS " SQLITEFS_PART_TABLE "("
    "  " SQLITEFS_PCOL_FEATNAME " TEXT NOT NULL,"
    "  " SQLITEFS_PCOL_PARTKEY " TEXT NOT NULL,"
    "  " SQLITEFS_PCOL_ROWCOUNT " INTEGER NOT NULL DEFAULT 0,"
    "  " SQLITEFS_PCOL_REFRESHED " TEXT NOT NULL DEFAULT (datetime('now')),"
    "  PRIMARY KEY (" SQLITEFS_PCOL_FEATNAME ", " SQLITEFS_PCOL_PARTKEY "),"
    "  FOREIGN KEY (" SQLITEFS_PCOL_FEATNAME ")"
    "    REFERENCES " SQLITEFS_META_TABLE "(" SQLITEFS_COL_NAME ")"
    ")";

/* INSERT full feature profile.
** window_size and retention_count use %s (not %d) so caller can pass
** "NULL" for SQL NULL or a numeric string. -1 sentinel → "NULL". */
static const char sqlitefs_insert_feature[] =
    "INSERT INTO " SQLITEFS_META_TABLE
    " (" SQLITEFS_COL_NAME      ","
         SQLITEFS_COL_QUERY     ","
         SQLITEFS_COL_SRCTABLES ","
         SQLITEFS_COL_ENTITYCOLS","
         SQLITEFS_COL_TSCOL     ","
         SQLITEFS_COL_GRAN      ","
         SQLITEFS_COL_GRANEXPR  ","
         SQLITEFS_COL_WINDOW    ","
         SQLITEFS_COL_FEATTYPE  ","
         SQLITEFS_COL_VALUECOLS ","
         SQLITEFS_COL_REFRESH   ","
         SQLITEFS_COL_RETAIN    ")"
    " VALUES (%Q,%Q,%Q,%Q,%Q,%Q,%Q,%s,%Q,%Q,%Q,%s)";
/*          name qry  src  ent  ts   gran gexp win  type val  ref  ret  */

/* SELECT full profile — column indices used in sqlitefs_load_feature_def */
static const char sqlitefs_search_feature[] =
    "SELECT "
    SQLITEFS_COL_QUERY     ","  /* 0 */
    SQLITEFS_COL_SRCTABLES ","  /* 1 */
    SQLITEFS_COL_ENTITYCOLS","  /* 2 */
    SQLITEFS_COL_TSCOL     ","  /* 3 */
    SQLITEFS_COL_GRAN      ","  /* 4 */
    SQLITEFS_COL_GRANEXPR  ","  /* 5 */
    SQLITEFS_COL_WINDOW    ","  /* 6  integer, NULL for SNAPSHOT */
    SQLITEFS_COL_FEATTYPE  ","  /* 7 */
    SQLITEFS_COL_VALUECOLS ","  /* 8 */
    SQLITEFS_COL_REFRESH   ","  /* 9 */
    SQLITEFS_COL_RETAIN         /* 10 integer, NULL for unlimited */
    " FROM " SQLITEFS_META_TABLE
    " WHERE " SQLITEFS_COL_NAME "=%Q";

/* ============================================================
** FeatureDef — in-memory representation of a feature profile.
** All char* fields are sqlite3_mprintf'd.
** Free with sqlitefs_free_feature_def().
** ============================================================ */
typedef struct FeatureDef FeatureDef;
struct FeatureDef
{
    /* identity */
    char *zName;         /* feature name */
    char *zFeatTable;    /* derived: _feat_<name> — not stored in DB */

    /* source */
    char *zQuery;        /* raw data scan — no GROUP BY, no aggregation */
    char *zSourceTables; /* JSON: ["transactions","users"] */

    /* entity (who) */
    char *zEntityTable;  /* name of the entity table, e.g. "users" */

    /* temporal (when) */
    char *zTimestampCol; /* raw event-time column name in AS query output */
    char *zGranularity;  /* "HOUR" | "DAY" */
    char *zGranExpr;     /* derived at CREATE: "DATE(event_time)"
                         ** or "strftime('%Y-%m-%dT%H', event_time)" */
    int   nWindowSize;   /* lookback units; -1 = SNAPSHOT */
    int   eFeatureType;  /* SQLITEFS_TYPE_SNAPSHOT | SQLITEFS_TYPE_AGGREGATE */

    /* value schema (what) */
    char *zValueCols;    /* JSON schema: {"user_id":"TEXT","transaction_date":"TEXT","total_spend":"NUMERIC",...} */

    /* lifecycle */
    int   eRefreshMode;    /* SQLITEFS_REFRESH_INCR | SQLITEFS_REFRESH_FULL */
    int   nRetentionCount; /* max partitions; -1 = unlimited */
};

static void sqlitefs_free_feature_def(FeatureDef *p)
{
    if (p == 0) return;
    sqlite3_free(p->zName);
    sqlite3_free(p->zFeatTable);
    sqlite3_free(p->zQuery);
    sqlite3_free(p->zSourceTables);
    sqlite3_free(p->zEntityTable);
    sqlite3_free(p->zTimestampCol);
    sqlite3_free(p->zGranularity);
    sqlite3_free(p->zGranExpr);
    sqlite3_free(p->zValueCols);
    sqlite3_free(p);
}

/* ============================================================
** sqlitefs_feat_table_ddl()
**
** Generate CREATE TABLE DDL for _feat_<name> from the feature profile.
** Column order: refreshed_at | entity_cols | partition_date | value_cols
**
** TODO (Phase 4): implement dynamic DDL from pDef->zEntityTable and
** pDef->zValueCols JSON. Current stub uses a placeholder schema.
** ============================================================ */
static char *sqlitefs_feat_table_ddl(const FeatureDef *pDef)
{
    /* Phase 4 stub — dynamic DDL from profile not yet implemented */
    return sqlite3_mprintf(
        "CREATE TABLE \"%w\" ("
        "  \"refreshed_at\"   TEXT,"
        "  \"partition_date\" TEXT,"
        "  \"entity_id\"      TEXT,"
        "  \"feature_value\"  REAL"
        ")",
        pDef->zFeatTable);
}

static char *sqlitefs_meta_feat_refresh(const FeatureDef *pDef)
{
    return sqlite3_mprintf(
        "UPDATE " SQLITEFS_META_TABLE
        " SET " SQLITEFS_COL_REFRESHED " = datetime('now')"
        " WHERE " SQLITEFS_COL_NAME " = %Q",
        pDef->zName);
}

/* ============================================================
** sqlitefs_feat_insert_ddl()
**
** Returns the full INSERT SQL to materialize a feature's query results
** into its partition table with a refresh timestamp prepended.
** Returns a sqlite3_mprintf'd string the caller must sqlite3_free.
** ============================================================ */
static char *sqlitefs_feat_insert_ddl(const FeatureDef *pDef)
{
    /* TODO: prepend datetime('now') as refreshed_at once column name
    ** tracking (zEntityCol/zValueCol) is added to FeatureDef, so the
    ** INSERT can explicitly map query columns to table columns. */
    return sqlite3_mprintf(
        "INSERT INTO \"%w\" SELECT * FROM (%s)",
        pDef->zFeatTable, pDef->zQuery);
}

/* ============================================================
** sqlitefs_LoadFeatureDef()
**
** Load the full feature profile for zName into a heap-allocated
** FeatureDef. Caller must free with sqlitefs_free_feature_def().
** Returns SQLITE_OK on success.
** Returns SQLITE_NOTFOUND if the feature does not exist.
** Returns SQLITE_ERROR on a database error (see sqlite3_errmsg).
** Returns SQLITE_NOMEM on allocation failure.
** ============================================================ */
static int sqlitefs_load_feature_def(
    sqlite3 *db,
    const char *zName,
    FeatureDef **ppDef /* OUT */
)
{
    sqlite3_stmt *pStmt = 0;
    char *zSql = 0;
    FeatureDef *p = 0;
    int rc;

    *ppDef = 0;

    zSql = sqlite3_mprintf(sqlitefs_search_feature, zName);
    if (zSql == 0)
        return SQLITE_NOMEM;

    rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
    sqlite3_free(zSql);
    if (rc != SQLITE_OK)
        return SQLITE_ERROR;

    if (sqlite3_step(pStmt) != SQLITE_ROW)
    {
        sqlite3_finalize(pStmt);
        return SQLITE_NOTFOUND;
    }

    p = sqlite3_malloc(sizeof(FeatureDef));
    if (p == 0)
    {
        sqlite3_finalize(pStmt);
        return SQLITE_NOMEM;
    }
    memset(p, 0, sizeof(FeatureDef));

    /* Column indices match sqlitefs_search_feature (0-10) */
    const char *zRawQuery    = (const char *)sqlite3_column_text(pStmt, 0);
    const char *zRawSrcTbls  = (const char *)sqlite3_column_text(pStmt, 1);
    const char *zRawEntCols  = (const char *)sqlite3_column_text(pStmt, 2);
    const char *zRawTsCol    = (const char *)sqlite3_column_text(pStmt, 3);
    const char *zRawGran     = (const char *)sqlite3_column_text(pStmt, 4);
    const char *zRawGranExpr = (const char *)sqlite3_column_text(pStmt, 5);
    /* col 6: window_size — integer, NULL for SNAPSHOT */
    int nWindow  = (sqlite3_column_type(pStmt, 6) == SQLITE_NULL)
                       ? -1 : sqlite3_column_int(pStmt, 6);
    const char *zRawFeatType = (const char *)sqlite3_column_text(pStmt, 7);
    const char *zRawValCols  = (const char *)sqlite3_column_text(pStmt, 8);
    const char *zRawRefresh  = (const char *)sqlite3_column_text(pStmt, 9);
    /* col 10: retention_count — integer, NULL for unlimited */
    int nRetain  = (sqlite3_column_type(pStmt, 10) == SQLITE_NULL)
                       ? -1 : sqlite3_column_int(pStmt, 10);

    p->zName         = sqlite3_mprintf("%s", zName);
    p->zFeatTable    = sqlite3_mprintf("%s%s", SQLITEFS_FEATURE_TABLE_PREFIX, zName);
    p->zQuery        = sqlite3_mprintf("%s", zRawQuery    ? zRawQuery    : "");
    p->zSourceTables = sqlite3_mprintf("%s", zRawSrcTbls  ? zRawSrcTbls  : "[]");
    p->zEntityTable   = sqlite3_mprintf("%s", zRawEntCols  ? zRawEntCols  : "[]");
    p->zTimestampCol = sqlite3_mprintf("%s", zRawTsCol    ? zRawTsCol    : "");
    p->zGranularity  = sqlite3_mprintf("%s", zRawGran     ? zRawGran     : "DAY");
    p->zGranExpr     = sqlite3_mprintf("%s", zRawGranExpr ? zRawGranExpr : "");
    p->nWindowSize   = nWindow;
    p->zValueCols    = sqlite3_mprintf("%s", zRawValCols  ? zRawValCols  : "[]");
    sqlite3_finalize(pStmt);

    if (zRawFeatType && sqlite3_stricmp(zRawFeatType, "SNAPSHOT") == 0)
        p->eFeatureType = SQLITEFS_TYPE_SNAPSHOT;
    else
        p->eFeatureType = SQLITEFS_TYPE_AGGREGATE;

    if (zRawRefresh && sqlite3_stricmp(zRawRefresh, "FULL") == 0)
        p->eRefreshMode = SQLITEFS_REFRESH_FULL;
    else
        p->eRefreshMode = SQLITEFS_REFRESH_INCR;

    p->nRetentionCount = nRetain;

    if (!p->zName || !p->zFeatTable || !p->zQuery  ||
        !p->zSourceTables || !p->zEntityTable       ||
        !p->zTimestampCol || !p->zGranularity      ||
        !p->zGranExpr     || !p->zValueCols)
    {
        sqlitefs_free_feature_def(p);
        return SQLITE_NOMEM;
    }

    *ppDef = p;
    return SQLITE_OK;
}

#ifndef SQLITE_OMIT_FEATURE

/* ============================================================
** sqlitefs_gran_expr()
**
** Derive the SQL truncation expression from the raw timestamp column
** name and the granularity. Only HOUR and DAY are supported.
** Coarser periods (week, month, year) are expressed via WINDOW + DAY.
**
**   HOUR → strftime('%Y-%m-%dT%H', <col>)   partition_key: '2026-04-03T14'
**   DAY  → DATE(<col>)                       partition_key: '2026-04-03'
**
** Returns sqlite3_mprintf'd string; caller must sqlite3_free().
** ============================================================ */
static char *sqlitefs_gran_expr(const char *zTsCol, const char *zGran)
{
    if (sqlite3_stricmp(zGran, "HOUR") == 0)
        return sqlite3_mprintf("strftime('%%Y-%%m-%%dT%%H', %s)", zTsCol);
    if (sqlite3_stricmp(zGran, "DAY") == 0)
        return sqlite3_mprintf("DATE(%s)", zTsCol);
    return 0;  /* unreachable — granularity already validated */
}




/* ============================================================
** sqlitefs_feature_def_from_tokens()
**
** Construct a fully-populated FeatureDef from parser tokens.
** This is the counterpart to sqlitefs_load_feature_def() which loads
** from the DB — here we build from grammar-supplied inputs.
**
** Encapsulates all validation and inference:
**   - name, timestamp_col, granularity: extract from tokens, validate
**   - entity_table: store entity table name token
**   - query_definition: slice raw SQL text from pLp/pRp
**   - query syntax: validated via sqlite3_prepare_v2
**   - timestamp_column: validated against SELECT output
**   - feat_schema: parsed from SELECT output column names and declared types
**   - source_tables: parsed from pSelect->pSrc
**   - feature_type: inferred from nWindowSize (-1 → SNAPSHOT)
**   - refresh_mode: resolved from pRefreshMode token
**
** Returns heap-allocated FeatureDef on success (caller must
** sqlitefs_free_feature_def()); returns NULL and sets pParse error on failure.
** ============================================================ */
static FeatureDef *sqlitefs_feature_def_from_tokens(
    Parse *pParse,
    Token *pName,
    Token *pEntityTable,
    Token *pTsCol,
    Token *pGran,
    int nWindowSize,
    Token *pRefreshMode,
    int nRetainCount,
    Select *pSelect,
    Token *pLp,
    Token *pRp)
{
    sqlite3 *db = pParse->db;
    FeatureDef *p = 0;
    sqlite3_stmt *pStmt = 0;
    int rc, i, nCol;

    p = sqlite3_malloc(sizeof(FeatureDef));
    if (!p)
    {
        sqlite3OomFault(db);
        return 0;
    }
    memset(p, 0, sizeof(FeatureDef));
    p->nWindowSize = -1;
    p->nRetentionCount = -1;

    /* ── Name ── */
    p->zName = sqlite3NameFromToken(db, pName);
    if (!p->zName)
        goto fail;
    p->zFeatTable = sqlite3_mprintf("%s%s", SQLITEFS_FEATURE_TABLE_PREFIX, p->zName);
    if (!p->zFeatTable)
    {
        sqlite3OomFault(db);
        goto fail;
    }

    /* ── Timestamp column ── */
    p->zTimestampCol = sqlite3DbStrNDup(db, pTsCol->z, pTsCol->n);
    if (!p->zTimestampCol)
        goto fail;

    /* ── Granularity: validate (HOUR|DAY only) then store ── */
    if (sqlite3_strnicmp(pGran->z, "HOUR", pGran->n) != 0 &&
        sqlite3_strnicmp(pGran->z, "DAY",  pGran->n) != 0)
    {
        sqlite3ErrorMsg(pParse,
            "granularity must be HOUR or DAY"
            " (use WINDOW + DAY for week/month/year)");
        goto fail;
    }
    p->zGranularity = sqlite3DbStrNDup(db, pGran->z, pGran->n);
    if (!p->zGranularity) goto fail;

    /* ── Gran expression: derive from timestamp_col + granularity ── */
    p->zGranExpr = sqlitefs_gran_expr(p->zTimestampCol, p->zGranularity);
    if (!p->zGranExpr) { sqlite3OomFault(db); goto fail; }

    /* ── Entity table name ── */
    /* TODO (Phase 4): resolve PK of entity table via PRAGMA table_info */
    p->zEntityTable = sqlite3NameFromToken(db, pEntityTable);
    if (!p->zEntityTable)
        goto fail;

    /* ── Slice SELECT SQL text from pLp/pRp ── */
    {
        const char *zBegin = pLp->z + pLp->n;
        int nSel = (int)(pRp->z - zBegin);
        while (nSel > 0 && sqlite3Isspace(zBegin[0]))
        {
            zBegin++;
            nSel--;
        }
        while (nSel > 0 && sqlite3Isspace(zBegin[nSel - 1]))
        {
            nSel--;
        }
        p->zQuery = sqlite3DbStrNDup(db, zBegin, nSel);
        if (!p->zQuery)
        {
            sqlite3ErrorMsg(pParse, "failed to extract query");
            goto fail;
        }
    }

    /* ── Prepare query: syntax + column existence checks ── */
    rc = sqlite3_prepare_v2(db, p->zQuery, -1, &pStmt, 0);
    if (rc != SQLITE_OK)
    {
        sqlite3ErrorMsg(pParse, "invalid query_definition: %s", sqlite3_errmsg(db));
        goto fail;
    }
    nCol = sqlite3_column_count(pStmt);

    /* validate timestamp column appears in SELECT output */
    {
        int found = 0;
        for (i = 0; i < nCol; i++)
        {
            const char *z = sqlite3_column_name(pStmt, i);
            if (z && sqlite3_stricmp(z, p->zTimestampCol) == 0)
            {
                found = 1;
                break;
            }
        }
        if (!found)
        {
            sqlite3ErrorMsg(pParse, "timestamp_column '%s' not in query output",
                            p->zTimestampCol);
            goto fail;
        }
    }


    /* ── Parse feature table schema ── */
    /* TODO (Phase 4): skip entity PK column once resolved via PRAGMA table_info */
    {
        char *z = sqlite3_mprintf("{");
        int bFirst = 1;
        if (!z) { sqlite3OomFault(db); goto fail; }
        for (i = 0; i < nCol; i++)
        {
            const char *zColName = sqlite3_column_name(pStmt, i);
            const char *zColType = sqlite3_column_decltype(pStmt, i);
            char *zNew;
            if (!zColName) continue;
            if (!zColType) zColType = "NUMERIC";
            zNew = sqlite3_mprintf("%s%s\"%s\":\"%s\"",
                                   z, bFirst ? "" : ",", zColName, zColType);
            sqlite3_free(z);
            if (!(z = zNew)) { sqlite3OomFault(db); goto fail; }
            bFirst = 0;
        }
        {
            char *zFinal = sqlite3_mprintf("%s}", z);
            sqlite3_free(z);
            p->zValueCols = zFinal;
        }
    }
    sqlite3_finalize(pStmt);
    pStmt = 0;
    if (!p->zValueCols)
    {
        sqlite3OomFault(db);
        goto fail;
    }

    /* ── Parse source_tables from pSelect->pSrc ── */
    /* Build ["t1","t2",...] from the FROM clause. Subqueries (zName==NULL) skipped. */
    {
        SrcList *pSrc = pSelect ? pSelect->pSrc : 0;
        char *z = sqlite3_mprintf("[");
        int bFirst = 1;
        if (!z) { sqlite3OomFault(db); goto fail; }
        if (pSrc)
        {
            for (i = 0; i < pSrc->nSrc; i++)
            {
                const char *zTbl = pSrc->a[i].zName;
                char *zNew;
                if (!zTbl) continue;
                zNew = sqlite3_mprintf("%s%s\"%s\"", z, bFirst ? "" : ",", zTbl);
                sqlite3_free(z);
                if (!(z = zNew)) { sqlite3OomFault(db); goto fail; }
                bFirst = 0;
            }
        }
        {
            char *zFinal = sqlite3_mprintf("%s]", z);
            sqlite3_free(z);
            p->zSourceTables = zFinal;
        }
    }
    if (!p->zSourceTables)
    {
        sqlite3OomFault(db);
        goto fail;
    }

    /* ── Infer feature_type from nWindowSize ── */
    p->nWindowSize = nWindowSize;
    p->eFeatureType = (nWindowSize < 0) ? SQLITEFS_TYPE_SNAPSHOT
                                        : SQLITEFS_TYPE_AGGREGATE;

    /* ── Resolve refresh_mode from token ── */
    if (pRefreshMode->z && pRefreshMode->n > 0 &&
        sqlite3_strnicmp(pRefreshMode->z, "FULL", 4) == 0)
        p->eRefreshMode = SQLITEFS_REFRESH_FULL;
    else
        p->eRefreshMode = SQLITEFS_REFRESH_INCR;

    p->nRetentionCount = nRetainCount;
    return p;

fail:
    if (pStmt)
        sqlite3_finalize(pStmt);
    sqlitefs_free_feature_def(p);
    return 0;
}

/* ============================================================
** sqlitefsRegisterFeature()
**
** Persist a fully-constructed FeatureDef into _sqlite_fs_features.
** Also ensures _sqlite_fs_partitions exists.
** Input is a FeatureDef* — no parsing or validation here, only DB ops.
** ============================================================ */
static void sqlitefsRegisterFeature(Parse *pParse, const FeatureDef *pDef)
{
    sqlite3 *db = pParse->db;
    char zWin[24], zRetain[24];
    const char *zFeatType, *zRefresh;
    char *zSql = 0;
    int rc;

    /* Step 1: ensure both registry tables exist. */
    rc = sqlite3_exec(db, sqlitefs_meta_ddl, 0, 0, 0);
    if (rc != SQLITE_OK)
    {
        sqlite3ErrorMsg(pParse, "create features table: %s", sqlite3_errmsg(db));
        return;
    }

    rc = sqlite3_exec(db, sqlitefs_part_ddl, 0, 0, 0);
    if (rc != SQLITE_OK)
    {
        sqlite3ErrorMsg(pParse, "create partitions table: %s", sqlite3_errmsg(db));
        return;
    }

    /* Step 2: convert -1 sentinels → SQL NULL strings for %s placeholders. */
    if (pDef->nWindowSize < 0)
        sqlite3_snprintf(sizeof(zWin), zWin, "NULL");
    else
        sqlite3_snprintf(sizeof(zWin), zWin, "%d", pDef->nWindowSize);

    if (pDef->nRetentionCount < 0)
        sqlite3_snprintf(sizeof(zRetain), zRetain, "NULL");
    else
        sqlite3_snprintf(sizeof(zRetain), zRetain, "%d", pDef->nRetentionCount);

    zFeatType = (pDef->eFeatureType == SQLITEFS_TYPE_SNAPSHOT)
                    ? "SNAPSHOT"
                    : "AGGREGATE";
    zRefresh = (pDef->eRefreshMode == SQLITEFS_REFRESH_FULL)
                   ? "FULL"
                   : "INCREMENTAL";

    /* Step 3: INSERT OR REPLACE the full profile row. */
    zSql = sqlite3_mprintf(sqlitefs_insert_feature,
                           pDef->zName,         pDef->zQuery,
                           pDef->zSourceTables, pDef->zEntityTable,
                           pDef->zTimestampCol, pDef->zGranularity,
                           pDef->zGranExpr,     /* e.g. DATE(event_time) */
                           zWin,                /* %s: NULL or integer */
                           zFeatType,           pDef->zValueCols,
                           zRefresh,
                           zRetain);            /* %s: NULL or integer */
    if (!zSql)
    {
        sqlite3OomFault(db);
        return;
    }
    rc = sqlite3_exec(db, zSql, 0, 0, 0);
    sqlite3_free(zSql);
    if (rc == SQLITE_CONSTRAINT)
        sqlite3ErrorMsg(pParse, "feature '%s' already exists", pDef->zName);
    else if (rc != SQLITE_OK)
        sqlite3ErrorMsg(pParse, "INSERT FEATURE: %s", sqlite3_errmsg(db));
}

/* ============================================================
** sqlite3CreateFeature()
**
** Thin coordinator for CREATE FEATURE:
**   1. Build FeatureDef from tokens (all validation + inference inside)
**   2. Persist via sqlitefsRegisterFeature
**   3. Free resources
** ============================================================ */
void sqlite3CreateFeature(
    Parse *pParse,
    Token *pName,
    Token *pEntityTable,
    Token *pTsCol,
    Token *pGran,
    int nWindowSize,
    Token *pRefreshMode,
    int nRetainCount,
    Select *pSelect,
    Token *pLp,
    Token *pRp)
{
    FeatureDef *pDef = sqlitefs_feature_def_from_tokens(
        pParse, pName, pEntityTable, pTsCol, pGran,
        nWindowSize, pRefreshMode, nRetainCount,
        pSelect, pLp, pRp);

    if (pDef)
        sqlitefsRegisterFeature(pParse, pDef);

    sqlitefs_free_feature_def(pDef);
    sqlite3SelectDelete(pParse->db, pSelect);
}

/* ============================================================
** sqlitefs_ensure_feat_table()
**
** Ensure the partition table _feat_<name> exists.
** If it already exists: return SQLITE_OK immediately.
** If it does not exist: derive schema via sqlitefs_derive_feat_schema
** and CREATE TABLE.
** Returns SQLITE_ERROR on failure; sqlite3_errmsg(db) carries the reason.
** ============================================================ */
static int sqlitefs_ensure_feat_table(sqlite3 *db, const FeatureDef *pDef)
{
    sqlite3_stmt *pStmt = 0;
    char *zSql = 0;
    char *zErrMsg = 0;
    int rc;

    /* Check if the table already exists — skip creation if so. */
    zSql = sqlite3_mprintf(
        "SELECT 1 FROM sqlite_schema WHERE type='table' AND name=%Q",
        pDef->zFeatTable);
    if (zSql == 0)
        return SQLITE_NOMEM;

    rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
    sqlite3_free(zSql);
    if (rc != SQLITE_OK)
        return SQLITE_ERROR;

    if (sqlite3_step(pStmt) == SQLITE_ROW)
    {
        sqlite3_finalize(pStmt);
        return SQLITE_OK;
    }
    sqlite3_finalize(pStmt);

    /* Table does not exist — build DDL and create it. */
    zSql = sqlitefs_feat_table_ddl(pDef);
    if (zSql == 0)
        return SQLITE_NOMEM;

    rc = sqlite3_exec(db, zSql, 0, 0, &zErrMsg);
    sqlite3_free(zSql);
    sqlite3_free(zErrMsg);
    return (rc == SQLITE_OK) ? SQLITE_OK : SQLITE_ERROR;
}

/*
** Handle REFRESH FEATURE <name>.
**
** Executes the stored query and appends all results into _feat_<name>
** with a refreshed_at timestamp. partition_column and granularity are
** kept as profile metadata for future optimization but not used here.
**
**   1. Load feature profile from _sqlite_fs_features.
**   2. Ensure _feat_<name> partition table exists.
**   3. INSERT INTO _feat_<name> SELECT datetime('now'), * FROM (<query>).
**   4. Update last_refreshed in _sqlite_fs_features.
*/
void sqlite3RefreshFeature(Parse *pParse, Token *pName)
{
    sqlite3 *db = pParse->db;
    char *zName = 0;
    FeatureDef *pDef = 0;
    char *zSql = 0;
    int rc;

    zName = sqlite3NameFromToken(db, pName);
    if (zName == 0)
        return;

    /* ---- Step 1: load feature profile ---- */
    rc = sqlitefs_load_feature_def(db, zName, &pDef);
    if (rc == SQLITE_NOTFOUND)
    {
        sqlite3ErrorMsg(pParse, "feature '%s' not found", zName);
        goto refresh_cleanup;
    }
    else if (rc == SQLITE_NOMEM)
    {
        sqlite3OomFault(db);
        goto refresh_cleanup;
    }
    else if (rc != SQLITE_OK)
    {
        sqlite3ErrorMsg(pParse, "%s", sqlite3_errmsg(db));
        goto refresh_cleanup;
    }

    /* ---- Step 2: ensure _feat_<name> table exists ---- */
    rc = sqlitefs_ensure_feat_table(db, pDef);
    if (rc == SQLITE_NOMEM)
    {
        sqlite3OomFault(db);
        goto refresh_cleanup;
    }
    else if (rc != SQLITE_OK)
    {
        sqlite3ErrorMsg(pParse, "%s", sqlite3_errmsg(db));
        goto refresh_cleanup;
    }

    /* ---- Step 3: append query results with refresh timestamp ---- */
    zSql = sqlitefs_feat_insert_ddl(pDef);
    if (zSql == 0)
    {
        sqlite3OomFault(db);
        goto refresh_cleanup;
    }

    rc = sqlite3_exec(db, zSql, 0, 0, 0);
    sqlite3_free(zSql);
    zSql = 0;
    if (rc != SQLITE_OK)
    {
        sqlite3ErrorMsg(pParse, "%s", sqlite3_errmsg(db));
        goto refresh_cleanup;
    }

    /* ---- Step 4: update last_refreshed ---- */
    zSql = sqlitefs_meta_feat_refresh(pDef);
    if (zSql == 0)
    {
        sqlite3OomFault(db);
        goto refresh_cleanup;
    }
    rc = sqlite3_exec(db, zSql, 0, 0, 0);
    sqlite3_free(zSql);
    zSql = 0;
    if (rc != SQLITE_OK)
    {
        sqlite3ErrorMsg(pParse, "%s", sqlite3_errmsg(db));
    }

refresh_cleanup:
    sqlite3DbFree(db, zName);
    sqlitefs_free_feature_def(pDef);
}

/*
** Stub for DESCRIBE FEATURE -- implemented in Phase 4
*/
void sqlite3DescribeFeature(Parse *pParse, Token *pName)
{
    sqlite3ErrorMsg(pParse, "DESCRIBE FEATURE not yet implemented");
}

void sqlite3DropFeature(Parse *pParse, Token *pName)
{
    sqlite3ErrorMsg(pParse, "DROP FEATURE not yet implemented");
}

#endif /* SQLITE_OMIT_FEATURE */
