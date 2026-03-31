/*
** featurestore.c -- SQLite-FS Feature Store Extension
**
** This file contains ALL feature store logic. The rest of the SQLite
** codebase interacts with this file only through:
**   1. sqlite3FeatureStoreInit() -- called to register SQL functions
**   2. sqlite3CreateFeature() etc. -- called from parse.y actions (Phase 2+)
**
** This design minimizes edits to core SQLite source files.
*/
#include "sqliteInt.h"

/* #include "featurestore.h"  -- already included via sqliteInt.h */

/* ============================================================
** Metadata table name and column name constants.
** All SQL in this file must reference these macros — never
** hardcode the strings directly, so a rename only touches here.
** ============================================================ */
// #define SQLITEFS_META_TABLE    "sqlite_fs_features"
#define SQLITEFS_META_TABLE "_sqlite_fs_features"
#define SQLITEFS_COL_NAME "feature_name"
#define SQLITEFS_COL_QUERY "query_definition"
#define SQLITEFS_COL_PARTCOL "partition_column"
#define SQLITEFS_COL_GRAN "granularity"
#define SQLITEFS_COL_CREATED "created_at"
#define SQLITEFS_COL_REFRESHED "last_refreshed"
#define SQLITEFS_META_TABLE_NCOL 4
#define SQLITEFS_FEATURE_TABLE_PREFIX "_feat_"

/* DDL — assembled from the macros above via C string concatenation */
static const char sqlitefs_meta_ddl[] =
    "CREATE TABLE IF NOT EXISTS " SQLITEFS_META_TABLE "("
    "  " SQLITEFS_COL_NAME " TEXT PRIMARY KEY,"
    "  " SQLITEFS_COL_QUERY " TEXT NOT NULL,"
    "  " SQLITEFS_COL_PARTCOL " TEXT,"
    "  " SQLITEFS_COL_GRAN " TEXT,"
    "  " SQLITEFS_COL_CREATED " TEXT DEFAULT (datetime('now')),"
    "  " SQLITEFS_COL_REFRESHED " TEXT"
    ")";

static const char sqlitefs_insert_feature[] =
    "INSERT OR REPLACE INTO " SQLITEFS_META_TABLE
    " (" SQLITEFS_COL_NAME ", " SQLITEFS_COL_QUERY ", " SQLITEFS_COL_PARTCOL ", " SQLITEFS_COL_GRAN ")"
    " VALUES (%Q, %Q, %Q, %Q)";

static const char sqlitefs_search_feature[] =
    "SELECT " SQLITEFS_COL_QUERY ", " SQLITEFS_COL_PARTCOL ", " SQLITEFS_COL_GRAN
    " FROM " SQLITEFS_META_TABLE
    " WHERE " SQLITEFS_COL_NAME "=%Q";

/* ============================================================
** FeatureDef — full profile of a registered feature.
** All strings are sqlite3_mprintf'd; free with sqlitefs FreeFeatureDef.
** ============================================================ */
typedef struct FeatureDef FeatureDef;
struct FeatureDef
{
    char *zName;        /* feature name */
    char *zFeatTable;   /* partition table name: _feat_<name> */
    char *zQuery;       /* query_definition — the stored SELECT */
    char *zPartCol;     /* partition_column — profile only, for future use */
    char *zGranularity; /* granularity (DAY/WEEK/...) — profile only, for future use */
};

static void sqlitefs_free_feature_def(FeatureDef *p)
{
    if (p == 0)
        return;
    sqlite3_free(p->zName);
    sqlite3_free(p->zFeatTable);
    sqlite3_free(p->zQuery);
    sqlite3_free(p->zPartCol);
    sqlite3_free(p->zGranularity);
    sqlite3_free(p);
}

/* ============================================================
** sqlitefs_feat_table_ddl()
**
** Returns the full CREATE TABLE SQL for a feature's partition table.
** The schema is currently fixed (refreshed_at, entity_id, feature_value);
** pDef is available so future versions can extend it based on
** granularity, partition_col, or other profile fields.
** Returns a sqlite3_mprintf'd string the caller must sqlite3_free.
** ============================================================ */
static char *sqlitefs_feat_table_ddl(const FeatureDef *pDef)
{
    return sqlite3_mprintf(
        "CREATE TABLE \"%w\" ("
        "  \"refreshed_at\"   TEXT,"
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

    const char *zRawQuery = (const char *)sqlite3_column_text(pStmt, 0);
    const char *zRawPartCol = (const char *)sqlite3_column_text(pStmt, 1);
    const char *zRawGran = (const char *)sqlite3_column_text(pStmt, 2);
    assert(zRawQuery != 0 && zRawPartCol != 0 && zRawGran != 0);

    p->zName        = sqlite3_mprintf("%s", zName);
    p->zFeatTable   = sqlite3_mprintf("%s%s", SQLITEFS_FEATURE_TABLE_PREFIX, zName);
    p->zQuery       = sqlite3_mprintf("%s", zRawQuery);
    p->zPartCol     = sqlite3_mprintf("%s", zRawPartCol);
    p->zGranularity = sqlite3_mprintf("%s", zRawGran);
    sqlite3_finalize(pStmt);

    if (p->zName == 0 || p->zFeatTable == 0 || p->zQuery == 0 || p->zPartCol == 0 || p->zGranularity == 0)
    {
        sqlitefs_free_feature_def(p);
        return SQLITE_NOMEM;
    }

    *ppDef = p;
    return SQLITE_OK;
}

#ifndef SQLITE_OMIT_FEATURE

/*
** Register a feature using sqlite3_exec — runs immediately at parse time.
** Unlike sqlitefsRegisterFeature (VDBE path), this calls sqlite3_exec
** directly so SQLite's full INSERT machinery runs, which updates both
** the main table B-tree AND the feature_name PRIMARY KEY index correctly.
**
** Trade-off: executes at sqlite3_prepare time (not sqlite3_step time),
** so it is not part of the caller's VDBE transaction.  For a metadata
** table this is acceptable.
*/
static void sqlitefsRegisterFeature(
    Parse *pParse,
    const char *zName,
    const char *zQuery,
    const char *zPartCol,
    const char *zGranularity)
{
    sqlite3 *db = pParse->db;
    char *zSql = 0;
    int rc;

    /* Step 1: ensure the metadata table exists. */
    rc = sqlite3_exec(db, sqlitefs_meta_ddl, 0, 0, 0);
    if (rc != SQLITE_OK)
    {
        sqlite3ErrorMsg(pParse, "Ensure Feature Metadata: %s", sqlite3_errmsg(db));
        return;
    }

    /* Step 2: insert (or replace) the feature row.
    ** sqlite3_exec runs a full INSERT which updates all B-trees, including
    ** the implicit index on feature_name TEXT PRIMARY KEY. */
    zSql = sqlite3_mprintf(sqlitefs_insert_feature,
                           zName, zQuery, zPartCol, zGranularity);
    if (zSql == 0)
    {
        sqlite3OomFault(db);
        return;
    }
    rc = sqlite3_exec(db, zSql, 0, 0, 0);
    sqlite3_free(zSql);
    if (rc != SQLITE_OK)
    {
        sqlite3ErrorMsg(pParse, "INSERT FEATURE: %s", sqlite3_errmsg(db));
    }
}

/*
** Handle CREATE FEATURE statement.
**
** CREATE FEATURE <name> AS <select> PARTITION BY <column> BY <granularity>
**
** This stores the feature definition in the _sqlitefs_features metadata
** table and creates a placeholder view for the feature.
*/
void sqlite3CreateFeature(
    Parse *pParse,   /* The parsing context */
    Token *pName,    /* The feature name token */
    Select *pSelect, /* The SELECT defining the feature */
    Token *pLp,      /* The '(' token — select text starts just after */
    Token *pRp,      /* The ')' token — select text ends just before */
    Token *pPartCol, /* The partition column name */
    Token *pGran     /* The granularity keyword token (HOUR/DAY/WEEK/MONTH/YEAR) */
)
{
    sqlite3 *db = pParse->db;
    char *zName = 0;        /* Feature name as a C string */
    char *zQuery = 0;       /* SELECT SQL text sliced from original input */
    char *zPartCol = 0;     /* Partition column as a C string */
    char *zGranularity = 0; /* Granularity as a db-allocated string */

    /* Extract feature name from token */
    zName = sqlite3NameFromToken(db, pName);
    if (zName == 0)
    {
        goto feature_cleanup;
    }

    /* Extract partition column name from token */
    zPartCol = sqlite3DbStrNDup(db, pPartCol->z, pPartCol->n);
    if (zPartCol == 0)
    {
        goto feature_cleanup;
    }

    /* Validate and extract the granularity keyword from its token. */
    if (sqlite3_strnicmp(pGran->z, "HOUR", 4) == 0 ||
        sqlite3_strnicmp(pGran->z, "DAY", 3) == 0 ||
        sqlite3_strnicmp(pGran->z, "WEEK", 4) == 0 ||
        sqlite3_strnicmp(pGran->z, "MONTH", 5) == 0 ||
        sqlite3_strnicmp(pGran->z, "YEAR", 4) == 0)
    {
        zGranularity = sqlite3DbStrNDup(db, pGran->z, pGran->n);
        if (zGranularity == 0)
            goto feature_cleanup;
    }
    else
    {
        sqlite3ErrorMsg(pParse, "unknown granularity: %.*s", pGran->n, pGran->z);
        goto feature_cleanup;
    }

    /* Slice the SELECT SQL text from the original input buffer.
    ** The text is the content between '(' and ')', with whitespace trimmed. */
    {
        const char *zSelBegin = pLp->z + pLp->n;
        int nSel = (int)(pRp->z - zSelBegin);
        while (nSel > 0 && sqlite3Isspace(zSelBegin[0]))
        {
            zSelBegin++;
            nSel--;
        }
        while (nSel > 0 && sqlite3Isspace(zSelBegin[nSel - 1]))
        {
            nSel--;
        }
        zQuery = sqlite3DbStrNDup(db, zSelBegin, nSel);
        if (zQuery == 0)
        {
            sqlite3ErrorMsg(pParse, "Fail to extract query_definition");
            goto feature_cleanup;
        }
    }

    /* Validate query_definition and partition_column.
    ** Step 1: prepare the query — catches syntax errors and missing tables.
    ** Step 2: scan column names — partition_column must be in the result set.
    */
    {
        sqlite3_stmt *pStmt = 0;
        int rc;
        int i, nCol;
        int found = 0;

        // check the defined query syntax valid
        rc = sqlite3_prepare_v2(db, zQuery, -1, &pStmt, 0);
        if (rc != SQLITE_OK)
        {
            sqlite3ErrorMsg(pParse, "invalid query_definition: %s",
                            sqlite3_errmsg(db));
            sqlite3_finalize(pStmt);
            goto feature_cleanup;
        }

        nCol = sqlite3_column_count(pStmt);
        for (i = 0; i < nCol; i++)
        {
            const char *zCol = sqlite3_column_name(pStmt, i);
            if (zCol && sqlite3_stricmp(zCol, zPartCol) == 0)
            {
                found = 1;
                break;
            }
        }
        sqlite3_finalize(pStmt);

        if (!found)
        {
            sqlite3ErrorMsg(pParse,
                            "partition_column '%s' not found in query_definition",
                            zPartCol);
            goto feature_cleanup;
        }
    }

    sqlitefsRegisterFeature(pParse, zName, zQuery, zPartCol, zGranularity);

feature_cleanup:
    sqlite3DbFree(db, zName);
    sqlite3DbFree(db, zQuery);
    sqlite3DbFree(db, zPartCol);
    sqlite3DbFree(db, zGranularity);
    sqlite3SelectDelete(db, pSelect);
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
    if (zSql == 0) { sqlite3OomFault(db); goto refresh_cleanup; }
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

/* --------------------------   User Defined Function ---------------------*/

/* ============================================================
** SQL Function: create_feature(name, query)
**
** Registers a feature definition in the metadata table.
** Usage: SELECT create_feature('daily_clicks', 'SELECT ...');
** ============================================================ */
static void sqlitefsCreateFeatureFunc(
    sqlite3_context *context,
    int argc,
    sqlite3_value **argv)
{
    const char *zName;
    const char *zQuery;
    sqlite3 *db;
    char *zErrMsg = 0;
    char *zSql;
    int rc;

    assert(argc == 2);
    zName = (const char *)sqlite3_value_text(argv[0]);
    zQuery = (const char *)sqlite3_value_text(argv[1]);

    if (zName == NULL || zName[0] == '\0')
    {
        sqlite3_result_error(context, "Feature name cannot be empty", -1);
        return;
    }

    if (zQuery == NULL || zQuery[0] == '\0')
    {
        sqlite3_result_error(context, "Query definition cannot be empty", -1);
        return;
    }

    db = sqlite3_context_db_handle(context);

    /* Ensure metadata table exists */
    rc = sqlite3_exec(db, sqlitefs_meta_ddl, 0, 0, 0);
    if (rc != SQLITE_OK)
    {
        sqlite3_result_error(context, sqlite3_errmsg(db), -1);
        return;
    }

    /* Insert or replace feature definition */
    zSql = sqlite3_mprintf(
        "INSERT OR REPLACE INTO " SQLITEFS_META_TABLE
        " (" SQLITEFS_COL_NAME ", " SQLITEFS_COL_QUERY ")"
        " VALUES (%Q, %Q)",
        zName, zQuery);
    rc = sqlite3_exec(db, zSql, 0, 0, 0);
    sqlite3_free(zSql);
    if (rc != SQLITE_OK)
    {
        sqlite3_result_error(context, sqlite3_errmsg(db), -1);
        return;
    }

    sqlite3_result_text(context, "Feature created", -1, SQLITE_STATIC);
}

/* ============================================================
** SQL Function: drop_feature(name)
**
** Removes a feature definition from the metadata table.
** Usage: SELECT drop_feature('daily_clicks');
** ============================================================ */
static void sqlitefsDropFeatureFunc(
    sqlite3_context *context,
    int argc,
    sqlite3_value **argv)
{
    const char *zName;
    sqlite3 *db;
    char *zSql;
    int rc;

    assert(argc == 1);
    zName = (const char *)sqlite3_value_text(argv[0]);
    if (zName == 0)
    {
        sqlite3_result_error(context, "feature name required", -1);
        return;
    }

    db = sqlite3_context_db_handle(context);

    zSql = sqlite3_mprintf(
        "DELETE FROM " SQLITEFS_META_TABLE
        " WHERE " SQLITEFS_COL_NAME "=%Q",
        zName);
    rc = sqlite3_exec(db, zSql, 0, 0, 0);
    sqlite3_free(zSql);

    if (rc != SQLITE_OK)
    {
        sqlite3_result_error(context, sqlite3_errmsg(db), -1);
        return;
    }

    sqlite3_result_text(context, "Feature dropped", -1, SQLITE_STATIC);
}

/* ============================================================
** SQL Function: list_features()
**
** Returns a JSON array of all registered feature names.
** Usage: SELECT list_features();
** ============================================================ */
static void sqlitefsListFeaturesFunc(
    sqlite3_context *context,
    int argc,
    sqlite3_value **argv)
{
    sqlite3 *db;
    sqlite3_stmt *pStmt = 0;
    int rc;

    (void)argc;
    (void)argv;
    db = sqlite3_context_db_handle(context);

    rc = sqlite3_prepare_v2(db,
                            "SELECT json_group_array(feature_name) "
                            "FROM _sqlitefs_features ORDER BY feature_name",
                            -1, &pStmt, 0);
    if (rc != SQLITE_OK)
    {
        /* Table might not exist yet -- return empty array */
        sqlite3_result_text(context, "[]", -1, SQLITE_STATIC);
        return;
    }

    if (sqlite3_step(pStmt) == SQLITE_ROW)
    {
        const char *zResult = (const char *)sqlite3_column_text(pStmt, 0);
        sqlite3_result_text(context, zResult, -1, SQLITE_TRANSIENT);
    }
    else
    {
        sqlite3_result_text(context, "[]", -1, SQLITE_STATIC);
    }
    sqlite3_finalize(pStmt);
}

/* ============================================================
** Initialization: register all feature store functions
** ============================================================ */
int sqlite3FeatureStoreInit(sqlite3 *db)
{
    int rc = SQLITE_OK;

    /* NOTE: do NOT run any DDL here. sqlite3FeatureStoreInit is called
    ** during sqlite3_open() before the connection is fully initialized.
    ** DDL at this stage triggers schema machinery too early and causes
    ** "SQL logic error". The metadata table is created lazily by
    ** sqlitefsEnsureMetaTable() on the first CREATE FEATURE call. */

    rc = sqlite3_create_function(db, "create_feature", 2,
                                 SQLITE_UTF8 | SQLITE_DIRECTONLY, 0,
                                 sqlitefsCreateFeatureFunc, 0, 0);
    if (rc != SQLITE_OK)
        return rc;

    rc = sqlite3_create_function(db, "drop_feature", 1,
                                 SQLITE_UTF8 | SQLITE_DIRECTONLY, 0,
                                 sqlitefsDropFeatureFunc, 0, 0);
    if (rc != SQLITE_OK)
        return rc;

    rc = sqlite3_create_function(db, "list_features", 0,
                                 SQLITE_UTF8 | SQLITE_DIRECTONLY, 0,
                                 sqlitefsListFeaturesFunc, 0, 0);
    return rc;
}

#endif /* SQLITE_OMIT_FEATURE */
