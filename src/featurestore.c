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
#define SQLITEFS_VIEW_PREFIX "_sqlite_fs_view_"

/* _sqlite_fs_features columns */
#define SQLITEFS_COL_NAME "feature_name"
#define SQLITEFS_COL_ENTITYTAB "entity_table"
#define SQLITEFS_COL_TSCOL "timestamp_column" /* raw event-time col in source */
#define SQLITEFS_COL_GRAN "col_gran"          /* HOUR | DAY only */
#define SQLITEFS_COL_GRANEXPR "gran_expr"     /* e.g. DATE(event_time) */
#define SQLITEFS_COL_WINDOW "window_size"
#define SQLITEFS_COL_FEATTYPE "feature_type"
#define SQLITEFS_COL_REFRESH "refresh_mode"
#define SQLITEFS_COL_RETAIN "retention_count"
#define SQLITEFS_COL_CREATED "created_at"
#define SQLITEFS_COL_REFRESHED "last_refreshed"

/* _feat_<name> built-in columns */
#define SQLITEFS_COL_VERSION "version"

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
    "  " SQLITEFS_COL_NAME " TEXT PRIMARY KEY,"
    "  " SQLITEFS_COL_ENTITYTAB " TEXT NOT NULL,"
    "  " SQLITEFS_COL_TSCOL " TEXT NOT NULL,"
    "  " SQLITEFS_COL_GRAN " TEXT NOT NULL,"
    "  " SQLITEFS_COL_GRANEXPR " TEXT NOT NULL,"
    "  " SQLITEFS_COL_WINDOW " INTEGER," /* NULL = SNAPSHOT */
    "  " SQLITEFS_COL_FEATTYPE " TEXT NOT NULL DEFAULT 'AGGREGATE',"
    "  " SQLITEFS_COL_REFRESH " TEXT NOT NULL DEFAULT 'INCREMENTAL',"
    "  " SQLITEFS_COL_RETAIN " INTEGER," /* NULL = unlimited */
    "  " SQLITEFS_COL_CREATED " TEXT DEFAULT (datetime('now')),"
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
    " (" SQLITEFS_COL_NAME "," SQLITEFS_COL_ENTITYTAB "," SQLITEFS_COL_TSCOL "," SQLITEFS_COL_GRAN "," SQLITEFS_COL_GRANEXPR "," SQLITEFS_COL_WINDOW "," SQLITEFS_COL_FEATTYPE "," SQLITEFS_COL_REFRESH "," SQLITEFS_COL_RETAIN ")"
    " VALUES (%Q,%Q,%Q,%Q,%Q,%s,%Q,%Q,%s)";
/*          name  ent   ts    gran  gexp  win   type  ref   ret */

/* CREATE VIEW _sqlite_fs_view_<name> AS <query>
** %Q = feature_name, %s = query text */
static const char sqlitefs_insert_feature_view[] =
    "CREATE VIEW IF NOT EXISTS \"" SQLITEFS_VIEW_PREFIX "%w\" AS %s";

/* SELECT full profile — column indices used in sqlitefs_load_feature_def */
static const char sqlitefs_search_feature[] =
    "SELECT " SQLITEFS_COL_ENTITYTAB "," /* 0 */
    SQLITEFS_COL_TSCOL ","               /* 1 */
    SQLITEFS_COL_GRAN ","                /* 2 */
    SQLITEFS_COL_GRANEXPR ","            /* 3 */
    SQLITEFS_COL_WINDOW ","              /* 4  integer, NULL for SNAPSHOT */
    SQLITEFS_COL_FEATTYPE ","            /* 5 */
    SQLITEFS_COL_REFRESH ","             /* 6 */
    SQLITEFS_COL_RETAIN                  /* 7  integer, NULL for unlimited */
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
    char *zName; /* feature name */

    /* Table pointers populated by sqlitefs_load_feature_def */
    Table *pFeatTable; /* pointer to _sqlite_fs_feat_<name> table */
    Table *pView;      /* pointer to _sqlite_fs_view_<name> view */

    /* transient — used for view creation, never stored in DB */
    char *zQuery; /* raw SELECT text extracted from pLp/pRp */

    /* entity (who) */
    char *zEntityTable; /* name of the entity table, e.g. "users" */

    /* temporal (when) */
    char *zTimestampCol; /* raw event-time column name in AS query output */
    char *zGranularity;  /* "HOUR" | "DAY" */
    char *zGranExpr;     /* derived at CREATE: "DATE(event_time)"
                         ** or "strftime('%Y-%m-%dT%H', event_time)" */
    int nWindowSize;     /* lookback units; -1 = SNAPSHOT */
    int eFeatureType;    /* SQLITEFS_TYPE_SNAPSHOT | SQLITEFS_TYPE_AGGREGATE */

    /* lifecycle */
    int eRefreshMode;    /* SQLITEFS_REFRESH_INCR | SQLITEFS_REFRESH_FULL */
    int nRetentionCount; /* max partitions; -1 = unlimited */
};

static void sqlitefs_free_feature_def(FeatureDef *p)
{
    if (p == 0)
        return;
    sqlite3_free(p->zName);
    sqlite3_free(p->zQuery);
    sqlite3_free(p->zEntityTable);
    sqlite3_free(p->zTimestampCol);
    sqlite3_free(p->zGranularity);
    sqlite3_free(p->zGranExpr);
    sqlite3_free(p);
}

/*
** sqlitefs_init_feat_table() logic is now integrated into sqlitefs_load_feature_def().
*/

/* ============================================================
** sqlitefs_load_feature_def()
**
** Load the full feature profile for zName into a heap-allocated
** FeatureDef. Also locates the view and ensures the feature table
** exists (creating it from view schema if needed).
**
** Caller must free with sqlitefs_free_feature_def().
** Returns SQLITE_OK on success.
** Returns SQLITE_NOTFOUND if the feature does not exist.
** Returns SQLITE_ERROR on a database error (see sqlite3_errmsg).
** Returns SQLITE_NOMEM on allocation failure.
** ============================================================ */
static int sqlitefs_load_feature_def(
    Parse *pParse,
    const char *zName,
    FeatureDef **ppDef /* OUT */
)
{
    sqlite3 *db = pParse->db;
    sqlite3_stmt *pStmt = 0;
    char *zSql = 0;
    char *zViewName = 0;
    char *zFeatName = 0;
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
        sqlite3ErrorMsg(pParse, "feature '%s' not found", zName);
        return SQLITE_ERROR;
    }

    p = sqlite3_malloc(sizeof(FeatureDef));
    if (p == 0)
    {
        sqlite3_finalize(pStmt);
        return SQLITE_NOMEM;
    }
    memset(p, 0, sizeof(FeatureDef));

    /* Column indices match sqlitefs_search_feature (0-7) */
    const char *zRawEntTbl = (const char *)sqlite3_column_text(pStmt, 0);
    const char *zRawTsCol = (const char *)sqlite3_column_text(pStmt, 1);
    const char *zRawGran = (const char *)sqlite3_column_text(pStmt, 2);
    const char *zRawGranExpr = (const char *)sqlite3_column_text(pStmt, 3);
    /* col 4: window_size — integer, NULL for SNAPSHOT */
    int nWindow = (sqlite3_column_type(pStmt, 4) == SQLITE_NULL)
                      ? -1
                      : sqlite3_column_int(pStmt, 4);
    const char *zRawFeatType = (const char *)sqlite3_column_text(pStmt, 5);
    const char *zRawRefresh = (const char *)sqlite3_column_text(pStmt, 6);
    /* col 7: retention_count — integer, NULL for unlimited */
    int nRetain = (sqlite3_column_type(pStmt, 7) == SQLITE_NULL)
                      ? -1
                      : sqlite3_column_int(pStmt, 7);

    p->zName = sqlite3_mprintf("%s", zName);
    p->zEntityTable = sqlite3_mprintf("%s", zRawEntTbl ? zRawEntTbl : "");
    p->zTimestampCol = sqlite3_mprintf("%s", zRawTsCol ? zRawTsCol : "");
    p->zGranularity = sqlite3_mprintf("%s", zRawGran ? zRawGran : "DAY");
    p->zGranExpr = sqlite3_mprintf("%s", zRawGranExpr ? zRawGranExpr : "");
    p->nWindowSize = nWindow;

    /* Resolve type/refresh before finalize — column text pointers
    ** become invalid after sqlite3_finalize(). */
    if (zRawFeatType && sqlite3_stricmp(zRawFeatType, "SNAPSHOT") == 0)
        p->eFeatureType = SQLITEFS_TYPE_SNAPSHOT;
    else
        p->eFeatureType = SQLITEFS_TYPE_AGGREGATE;

    if (zRawRefresh && sqlite3_stricmp(zRawRefresh, "FULL") == 0)
        p->eRefreshMode = SQLITEFS_REFRESH_FULL;
    else
        p->eRefreshMode = SQLITEFS_REFRESH_INCR;

    sqlite3_finalize(pStmt);

    p->nRetentionCount = nRetain;

    if (!p->zName || !p->zEntityTable || !p->zTimestampCol ||
        !p->zGranularity || !p->zGranExpr)
    {
        sqlitefs_free_feature_def(p);
        return SQLITE_NOMEM;
    }

    /* --- Locate view and ensure feature table exists --- */
    zViewName = sqlite3_mprintf("%s%s", SQLITEFS_VIEW_PREFIX, zName);
    zFeatName = sqlite3_mprintf("%s%s", SQLITEFS_FEATURE_TABLE_PREFIX, zName);
    if (!zViewName || !zFeatName)
    {
        rc = SQLITE_NOMEM;
        goto load_cleanup;
    }

    /* Find and validate the view */
    p->pView = sqlite3FindTable(db, zViewName, 0);
    if (!p->pView || !IsView(p->pView))
    {
        sqlite3ErrorMsg(pParse, "view for feature '%s' not found", p->zName);
        rc = SQLITE_ERROR;
        goto load_cleanup;
    }

    /* Ensure view column names are populated */
    if (sqlite3ViewGetColumnNames(pParse, p->pView))
    {
        rc = SQLITE_ERROR;
        goto load_cleanup;
    }

    /* Fast path: feature table already exists */
    p->pFeatTable = sqlite3FindTable(db, zFeatName, 0);
    if (!p->pFeatTable)
    {
        /* Slow path: create feature table from view schema.
        ** View-derived columns come first, then the built-in
        ** version column is appended as the last column. */
        char *zDdl = sqlite3_mprintf("CREATE TABLE \"%w\"(", zFeatName);
        if (!zDdl)
        {
            sqlite3OomFault(db);
            rc = SQLITE_NOMEM;
            goto load_cleanup;
        }

        int i;
        for (i = 0; zDdl && i < p->pView->nCol; i++)
        {
            Column *pCol = &p->pView->aCol[i];
            const char *zType = sqlite3ColumnType(pCol, 0);
            char *zNew;
            if (zType)
            {
                zNew = sqlite3_mprintf("%s%s\"%w\" %s", zDdl,
                                       i ? ", " : "", pCol->zCnName, zType);
            }
            else
            {
                zNew = sqlite3_mprintf("%s%s\"%w\"", zDdl,
                                       i ? ", " : "", pCol->zCnName);
            }
            sqlite3_free(zDdl);
            zDdl = zNew;
        }

        /* Append built-in version column */
        if (zDdl)
        {
            char *zNew = sqlite3_mprintf("%s, " SQLITEFS_COL_VERSION " TEXT",
                                         zDdl);
            sqlite3_free(zDdl);
            zDdl = zNew;
        }

        if (!zDdl)
        {
            sqlite3OomFault(db);
            rc = SQLITE_NOMEM;
            goto load_cleanup;
        }

        char *zFinal = sqlite3_mprintf("%s)", zDdl);
        sqlite3_free(zDdl);
        if (!zFinal)
        {
            sqlite3OomFault(db);
            rc = SQLITE_NOMEM;
            goto load_cleanup;
        }

        rc = sqlite3_exec(db, zFinal, 0, 0, 0);
        sqlite3_free(zFinal);
        if (rc != SQLITE_OK)
        {
            sqlite3ErrorMsg(pParse, "create feature table: %s", sqlite3_errmsg(db));
            rc = SQLITE_ERROR;
            goto load_cleanup;
        }

        /* Look up the newly created table */
        p->pFeatTable = sqlite3FindTable(db, zFeatName, 0);
        if (!p->pFeatTable)
        {
            sqlite3ErrorMsg(pParse, "feature table '%s' not found after creation", zFeatName);
            rc = SQLITE_ERROR;
            goto load_cleanup;
        }
    }

    *ppDef = p;
    p = 0; /* prevent cleanup from freeing the result */
    rc = SQLITE_OK;

load_cleanup:
    sqlite3_free(zViewName);
    sqlite3_free(zFeatName);
    sqlitefs_free_feature_def(p);
    return rc;
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
    return 0; /* unreachable — granularity already validated */
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

    /* ── Timestamp column ── */
    p->zTimestampCol = sqlite3DbStrNDup(db, pTsCol->z, pTsCol->n);
    if (!p->zTimestampCol)
        goto fail;

    /* ── Granularity: validate (HOUR|DAY only) then store ── */
    if (sqlite3_strnicmp(pGran->z, "HOUR", pGran->n) != 0 &&
        sqlite3_strnicmp(pGran->z, "DAY", pGran->n) != 0)
    {
        sqlite3ErrorMsg(pParse,
                        "granularity must be HOUR or DAY"
                        " (use WINDOW + DAY for week/month/year)");
        goto fail;
    }
    p->zGranularity = sqlite3DbStrNDup(db, pGran->z, pGran->n);
    if (!p->zGranularity)
        goto fail;

    /* ── Gran expression: derive from timestamp_col + granularity ── */
    p->zGranExpr = sqlitefs_gran_expr(p->zTimestampCol, p->zGranularity);
    if (!p->zGranExpr)
    {
        sqlite3OomFault(db);
        goto fail;
    }

    /* ── Entity table name ── */
    /* TODO (Phase 3): validate that the inner SELECT includes a column whose
    ** name matches the entity table's PRIMARY KEY. At REFRESH time,
    ** sqlitefs_build_pit_query() should LEFT JOIN from the entity table to
    ** the inner query as a subquery, and COALESCE aggregate columns to 0,
    ** so that entities with no matching rows get default values instead of
    ** being omitted.
    ** Use sqlite3FindTable() + pTab->iPKey / aCol[iPKey].zCnName to resolve
    ** the PK column name, then scan pSelect->pEList for a matching alias. */
    p->zEntityTable = sqlite3NameFromToken(db, pEntityTable);
    if (!p->zEntityTable)
        goto fail;

    /* ── Query text: slice raw SQL from pLp/pRp (transient, for view creation) ── */
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
    sqlitefs_free_feature_def(p);
    return 0;
}

/* ----------------- Point-in-Time Query Rewrite ---------------------*/
/*
** sqlitefs_gran_col_expr()
**
** Build an Expr that applies the granularity truncation function
** to the timestamp column reference:
**   DAY  → DATE(<zTimestampCol>)
**   HOUR → strftime('%Y-%m-%dT%H', <zTimestampCol>)
*/
static Expr *sqlitefs_gran_col_expr(Parse *pParse, FeatureDef *pDef)
{
    sqlite3 *db = pParse->db;
    ExprList *pArgs = 0;
    Token tFunc;
    int isHour = (sqlite3_stricmp(pDef->zGranularity, "HOUR") == 0);

    if (isHour)
    {
        Expr *pFmt = sqlite3Expr(db, TK_STRING, "%Y-%m-%dT%H");
        pArgs = sqlite3ExprListAppend(pParse, pArgs, pFmt);
        tFunc.z = "strftime";
        tFunc.n = 8;
    }
    else
    {
        tFunc.z = "date";
        tFunc.n = 4;
    }

    /* Timestamp column reference as unresolved identifier */
    Expr *pCol = sqlite3Expr(db, TK_ID, pDef->zTimestampCol);
    pArgs = sqlite3ExprListAppend(pParse, pArgs, pCol);

    return sqlite3ExprFunction(pParse, pArgs, &tFunc, 0);
}

/*
** sqlitefs_gran_now_expr()
**
** Build an Expr for a granularity-truncated boundary relative to 'now'.
**   nOffset == 0 → gran('now')           e.g. DATE('now')
**   nOffset >  0 → gran('now', '-N u')   e.g. DATE('now', '-6 days')
**
** The offset subtracts nOffset granularity units from 'now' before
** truncation, so the result is a partition key in the past.
*/
static Expr *sqlitefs_gran_now_expr(
    Parse *pParse,
    FeatureDef *pDef,
    int nOffset)
{
    sqlite3 *db = pParse->db;
    ExprList *pArgs = 0;
    Token tFunc;
    int isHour = (sqlite3_stricmp(pDef->zGranularity, "HOUR") == 0);

    if (isHour)
    {
        Expr *pFmt = sqlite3Expr(db, TK_STRING, "%Y-%m-%dT%H");
        pArgs = sqlite3ExprListAppend(pParse, pArgs, pFmt);
        tFunc.z = "strftime";
        tFunc.n = 8;
    }
    else
    {
        tFunc.z = "date";
        tFunc.n = 4;
    }

    /* 'now' literal — the time-value argument */
    Expr *pNow = sqlite3Expr(db, TK_STRING, "now");
    pArgs = sqlite3ExprListAppend(pParse, pArgs, pNow);

    /* Optional modifier: '-<nOffset> days' or '-<nOffset> hours' */
    if (nOffset > 0)
    {
        const char *zUnit = isHour ? "hours" : "days";
        char *zMod = sqlite3_mprintf("-%d %s", nOffset, zUnit);
        if (zMod)
        {
            Expr *pMod = sqlite3Expr(db, TK_STRING, zMod);
            sqlite3_free(zMod);
            pArgs = sqlite3ExprListAppend(pParse, pArgs, pMod);
        }
    }

    return sqlite3ExprFunction(pParse, pArgs, &tFunc, 0);
}

/*
** sqlitefs_build_pit_query()
**
** Rewrite the duplicated view Select* in-place for point-in-time
** materialization.
**
** Part 1 — pWhere: inject sliding-window lower bound (AGGREGATE only).
**   ts_col >= datetime('now', '-N units')
**
** The comparison uses the raw timestamp column — no granularity
** truncation. This gives a true sliding window anchored at the
** refresh moment.
**
** Example: GRANULARITY DAY, DURATION 7, refresh at 2026-04-10 12:00
**   clicked_at >= datetime('now', '-7 days')
**   → clicked_at >= '2026-04-03 12:00:00'
**   Window covers exactly 7×24h ending at the refresh moment.
**
** SNAPSHOT features (nWindowSize == -1): no filter.
**
** Part 2 — pEList: append datetime('now') AS version.
*/
static int sqlitefs_build_pit_query(
    Parse *pParse,
    FeatureDef *pDef,
    Select *pSelect)
{
    sqlite3 *db = pParse->db;

    /* ---- Part 1: pWhere — sliding-window lower bound (AGGREGATE only) ---- */
    if (pDef->eFeatureType != SQLITEFS_TYPE_SNAPSHOT)
    {
        int isHour = (sqlite3_stricmp(pDef->zGranularity, "HOUR") == 0);
        const char *zUnit = isHour ? "hours" : "days";

        /* LHS: bare column reference — ts_col */
        Expr *pCol = sqlite3Expr(db, TK_ID, pDef->zTimestampCol);

        /* RHS: datetime('now', '-N units') */
        ExprList *pArgs = 0;
        Expr *pNow = sqlite3Expr(db, TK_STRING, "now");
        pArgs = sqlite3ExprListAppend(pParse, pArgs, pNow);

        char *zMod = sqlite3_mprintf("-%d %s", pDef->nWindowSize, zUnit);
        if (zMod)
        {
            Expr *pMod = sqlite3Expr(db, TK_STRING, zMod);
            sqlite3_free(zMod);
            pArgs = sqlite3ExprListAppend(pParse, pArgs, pMod);
        }

        Token tFunc;
        tFunc.z = "datetime";
        tFunc.n = 8;
        Expr *pBound = sqlite3ExprFunction(pParse, pArgs, &tFunc, 0);

        /* ts_col >= datetime('now', '-N units') */
        Expr *pFilter = sqlite3PExpr(pParse, TK_GE, pCol, pBound);

        /* AND onto existing WHERE */
        pSelect->pWhere = sqlite3ExprAnd(pParse, pSelect->pWhere, pFilter);
    }

    /* ---- Part 2: pEList — append datetime('now') AS version ---- */
    {
        ExprList *pArgs = 0;
        Expr *pNow = sqlite3Expr(db, TK_STRING, "now");
        pArgs = sqlite3ExprListAppend(pParse, pArgs, pNow);

        Token tFunc;
        tFunc.z = "datetime";
        tFunc.n = 8;
        Expr *pVersion = sqlite3ExprFunction(pParse, pArgs, &tFunc, 0);

        Token tAlias;
        tAlias.z = "version";
        tAlias.n = 7;
        pSelect->pEList = sqlite3ExprListAppend(pParse, pSelect->pEList, pVersion);
        sqlite3ExprListSetName(pParse, pSelect->pEList, &tAlias, 0);
    }

    return SQLITE_OK;
}

/* ============================================================
** sqlite3CreateFeature()
**
**   1. Build FeatureDef from tokens (all validation + inference inside)
**   2. Ensure registry tables exist
**   3. INSERT feature profile into _sqlite_fs_features
**   4. Free resources
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
    sqlite3 *db = pParse->db;
    FeatureDef *pDef = 0;
    char zWin[24], zRetain[24];
    const char *zFeatType, *zRefresh;
    int rc;

    pDef = sqlitefs_feature_def_from_tokens(
        pParse, pName, pEntityTable, pTsCol, pGran,
        nWindowSize, pRefreshMode, nRetainCount,
        pSelect, pLp, pRp);
    if (!pDef)
        goto cleanup;

    /* Step 1: ensure both registry tables exist. */
    rc = sqlite3_exec(db, sqlitefs_meta_ddl, 0, 0, 0);
    if (rc != SQLITE_OK)
    {
        sqlite3ErrorMsg(pParse, "create features table: %s", sqlite3_errmsg(db));
        goto cleanup;
    }
    rc = sqlite3_exec(db, sqlitefs_part_ddl, 0, 0, 0);
    if (rc != SQLITE_OK)
    {
        sqlite3ErrorMsg(pParse, "create partitions table: %s", sqlite3_errmsg(db));
        goto cleanup;
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

    /* TODO: validate table/column references via sqlite3_prepare_v2 on the
    ** view before committing the profile. Currently deferred to REFRESH. */

    /* Step 3: atomically INSERT profile row + CREATE VIEW via NestedParse.
    ** Both operations share the same VDBE program — if CREATE VIEW fails
    ** (e.g. bad query), the INSERT is also rolled back. */
    sqlite3NestedParse(pParse, sqlitefs_insert_feature,
                       pDef->zName, pDef->zEntityTable,
                       pDef->zTimestampCol, pDef->zGranularity,
                       pDef->zGranExpr, /* e.g. DATE(event_time) */
                       zWin,            /* %s: NULL or integer */
                       zFeatType, zRefresh,
                       zRetain); /* %s: NULL or integer */
    if (pParse->nErr)
        goto cleanup;

    sqlite3NestedParse(pParse, sqlitefs_insert_feature_view,
                       pDef->zName, pDef->zQuery);
    if (pParse->nErr)
        goto cleanup;

cleanup:
    sqlitefs_free_feature_def(pDef);
    sqlite3SelectDelete(db, pSelect);
}

/*
** Handle REFRESH FEATURE <name>.
**
**   1. Load feature profile from _sqlite_fs_features.
**   2. Ensure _feat_<name> exists (sqlitefs_init_feat_table).
**   3. Find view → dup its Select* → PIT rewrite (sqlitefs_build_pit_query).
**   4. Build SrcList for _feat_<name>, call sqlite3Insert().
**   5. TODO: upsert _sqlite_fs_partitions via NestedParse.
**   6. TODO: update last_refreshed via NestedParse.
**   7. TODO: retention enforcement.
**
** Steps 4-6 emit into the same VDBE → one atomic transaction.
*/
void sqlite3RefreshFeature(Parse *pParse, Token *pName)
{
    sqlite3 *db = pParse->db;
    char *zName = 0;
    FeatureDef *pDef = 0;
    Select *pSel = 0;
    SrcList *pTabList = 0;
    int rc;

    zName = sqlite3NameFromToken(db, pName);
    if (zName == 0)
        return;

    /* ---- Step 1: load feature profile (also ensures view and feature table exist) ---- */
    rc = sqlitefs_load_feature_def(pParse, zName, &pDef);
    if (rc != SQLITE_OK)
    {
        goto refresh_cleanup;
    }

    /* ---- Step 2: get view's Select*, dup it, run PIT rewrite ---- */
    if (!pDef->pView || !IsView(pDef->pView))
    {
        sqlite3ErrorMsg(pParse, "view for feature '%s' not found", pDef->zName);
        goto refresh_cleanup;
    }
    pSel = sqlite3SelectDup(db, pDef->pView->u.view.pSelect, 0);
    if (!pSel)
    {
        sqlite3OomFault(db);
        goto refresh_cleanup;
    }
    rc = sqlitefs_build_pit_query(pParse, pDef, pSel);
    if (rc != SQLITE_OK)
        goto refresh_cleanup;

    /* ---- Step 4: build SrcList for _feat_<name>, call sqlite3Insert ---- */
    {
        Token tblToken;
        tblToken.z = pDef->pFeatTable->zName;
        tblToken.n = (unsigned)sqlite3Strlen30(pDef->pFeatTable->zName);
        pTabList = sqlite3SrcListAppend(pParse, 0, &tblToken, 0);
        if (!pTabList)
        {
            sqlite3OomFault(db);
            goto refresh_cleanup;
        }
    }

    /* sqlite3Insert takes ownership of pTabList and pSel — do NOT free */
    sqlite3Insert(pParse, pTabList, pSel, NULL, OE_Abort, NULL);
    pTabList = 0; /* consumed */
    pSel = 0;     /* consumed */

    /* ---- Step 5: TODO — upsert _sqlite_fs_partitions via NestedParse ---- */

    /* ---- Step 6: TODO — update last_refreshed via NestedParse ---- */

    /* ---- Step 7: TODO — retention enforcement ---- */

refresh_cleanup:
    sqlite3SrcListDelete(db, pTabList);
    sqlite3SelectDelete(db, pSel);
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
