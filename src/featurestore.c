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
#define SQLITEFS_META_TABLE    "_sqlite_fs_features"
#define SQLITEFS_COL_NAME      "feature_name"
#define SQLITEFS_COL_QUERY     "query_definition"
#define SQLITEFS_COL_PARTCOL   "partition_column"
#define SQLITEFS_COL_GRAN      "granularity"
#define SQLITEFS_COL_CREATED   "created_at"
#define SQLITEFS_COL_REFRESHED "last_refreshed"
#define SQLITEFS_META_TABLE_NCOL 4

/* DDL — assembled from the macros above via C string concatenation */
static const char sqlitefs_meta_ddl[] =
    "CREATE TABLE IF NOT EXISTS " SQLITEFS_META_TABLE "("
    "  " SQLITEFS_COL_NAME      " TEXT PRIMARY KEY,"
    "  " SQLITEFS_COL_QUERY     " TEXT NOT NULL,"
    "  " SQLITEFS_COL_PARTCOL   " TEXT,"
    "  " SQLITEFS_COL_GRAN      " TEXT,"
    "  " SQLITEFS_COL_CREATED   " TEXT DEFAULT (datetime('now')),"
    "  " SQLITEFS_COL_REFRESHED " TEXT"
    ")";

    
static const char sqlitefs_insert_feature[] =
    "INSERT OR REPLACE INTO " SQLITEFS_META_TABLE
    " (" SQLITEFS_COL_NAME ", " SQLITEFS_COL_QUERY ", " SQLITEFS_COL_PARTCOL ", " SQLITEFS_COL_GRAN ")"
    " VALUES (%Q, %Q, %Q, %Q)";



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
    rc = sqlite3_exec(db, sqlitefs_meta_ddl, 0, 0, &zErrMsg);

    if (rc != SQLITE_OK)
    {
        sqlite3_result_error(context, zErrMsg, -1);
        sqlite3_free(zErrMsg);
        return;
    }

    /* Insert or replace feature definition */
    zSql = sqlite3_mprintf(
        "INSERT OR REPLACE INTO " SQLITEFS_META_TABLE
        " (" SQLITEFS_COL_NAME ", " SQLITEFS_COL_QUERY ")"
        " VALUES (%Q, %Q)",
        zName, zQuery);
    rc = sqlite3_exec(db, zSql, 0, 0, &zErrMsg);
    sqlite3_free(zSql);
    if (rc != SQLITE_OK)
    {
        sqlite3_result_error(context, zErrMsg, -1);
        sqlite3_free(zErrMsg);
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
    char *zErrMsg = 0;
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
        " WHERE " SQLITEFS_COL_NAME "=%Q", zName);
    rc = sqlite3_exec(db, zSql, 0, 0, &zErrMsg);
    sqlite3_free(zSql);

    if (rc != SQLITE_OK)
    {
        sqlite3_result_error(context, zErrMsg, -1);
        sqlite3_free(zErrMsg);
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

#ifndef SQLITE_OMIT_FEATURE



/* 
================================ Add new Keyword ==============================
*/

/*
** Ensure the sqlite_fs_features metadata table exists.
** If the table is already in the schema: do nothing, return 0.
** If the table does not exist: emit CREATE TABLE opcodes and return
** OPFLAG_P2ISREG (pParse->u1.cr.regRoot now holds the root-page register).
** Returns -1 on error.
*/
static int sqlitefsEnsureMetaTable(Parse *pParse, int iDb, u32 *pTnum){
    sqlite3 *db    = pParse->db;
    Table   *pMeta = sqlite3FindTable(db, SQLITEFS_META_TABLE,
                                      db->aDb[iDb].zDbSName);
    if( pMeta!=0 ){
        /* Table already exists — bypass creation. */
        sqlite3TableLock(pParse, iDb, pMeta->tnum, 1, SQLITEFS_META_TABLE);
        *pTnum = pMeta->tnum;
        return 0;
    }
    /* Table does not exist — emit CREATE TABLE opcodes.
    ** Side-effect: pParse->u1.cr.regRoot is set to the register that
    ** will hold the new root page at VDBE runtime. */
    sqlite3NestedParse(pParse, "%s", sqlitefs_meta_ddl);
    if( pParse->nErr ) return -1;
    *pTnum = (u32)pParse->u1.cr.regRoot;
    return OPFLAG_P2ISREG;
}

/*
** Emit VDBE code to insert a feature row into sqlite_fs_features.
** Calls sqlitefsEnsureMetaTable first to guarantee the table exists.
** Uses NestedParse INSERT when the table was already in the schema,
** or raw VDBE opcodes when it was just created (not yet in schema).
*/
/*
Not used, cannot concurrently change the index.
*/
// static void sqlitefsRegisterFeature(
//     Parse      *pParse,
//     const char *zName,
//     const char *zQuery,
//     const char *zPartCol,
//     const char *zGranularity
// ){
//     Vdbe *v   = sqlite3GetVdbe(pParse);
//     int   iDb = 0;
//     int   iCur = pParse->nTab++;
//     u32   tnum;
//     int   createTbl;

//     if( v==0 ) return;

//     createTbl = sqlitefsEnsureMetaTable(pParse, iDb, &tnum);
//     if( createTbl<0 ) {
//         sqlite3ErrorMsg(pParse, "unable to create metadata table");
//         return;
//     }

//     sqlite3VdbeAddOp4Int(v, OP_OpenWrite, iCur, (int)tnum, iDb, SQLITEFS_META_TABLE_NCOL);
//     sqlite3VdbeChangeP5(v, (u8)createTbl);

//     if( createTbl==0 ){
//         /* Table was already in the schema — NestedParse INSERT is safe. */
//         sqlite3VdbeAddOp1(v, OP_Close, iCur);
//         sqlite3NestedParse(pParse, sqlitefs_insert_feature,
//                            zName, zQuery, zPartCol, zGranularity);
//     }else{
//         /* Table was just created — NestedParse INSERT would fail because
//         ** the table is not yet in the in-memory schema.  Use raw opcodes. */
//         int regBase   = sqlite3GetTempRange(pParse, 4);
//         int regRecord = sqlite3GetTempReg(pParse);
//         int regRowid  = sqlite3GetTempReg(pParse);

//         sqlite3VdbeAddOp4(v, OP_String8, 0, regBase+0, 0, zName,        0);
//         sqlite3VdbeAddOp4(v, OP_String8, 0, regBase+1, 0, zQuery,       0);
//         sqlite3VdbeAddOp4(v, OP_String8, 0, regBase+2, 0, zPartCol,     0);
//         sqlite3VdbeAddOp4(v, OP_String8, 0, regBase+3, 0, zGranularity, 0);

//         sqlite3VdbeAddOp3(v, OP_MakeRecord, regBase, 4, regRecord);
//         sqlite3VdbeAddOp3(v, OP_NewRowid,   iCur, regRowid, 0);
//         sqlite3VdbeAddOp3(v, OP_Insert,     iCur, regRecord, regRowid);

//         sqlite3ReleaseTempReg(pParse, regRowid);
//         sqlite3ReleaseTempReg(pParse, regRecord);
//         sqlite3ReleaseTempRange(pParse, regBase, 4);
//         sqlite3VdbeAddOp1(v, OP_Close, iCur);
//     }
// }

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
    Parse      *pParse,
    const char *zName,
    const char *zQuery,
    const char *zPartCol,
    const char *zGranularity
){
    sqlite3 *db = pParse->db;
    char    *zErrMsg = 0;
    char    *zSql    = 0;
    int      rc;

    /* Step 1: ensure the metadata table exists. */
    rc = sqlite3_exec(db, sqlitefs_meta_ddl, 0, 0, &zErrMsg);
    if( rc!=SQLITE_OK ){
        sqlite3ErrorMsg(pParse, "Ensure Feature Metadata: %s", zErrMsg);
        sqlite3_free(zErrMsg);
        return;
    }

    /* Step 2: insert (or replace) the feature row.
    ** sqlite3_exec runs a full INSERT which updates all B-trees, including
    ** the implicit index on feature_name TEXT PRIMARY KEY. */
    zSql = sqlite3_mprintf(sqlitefs_insert_feature,
                           zName, zQuery, zPartCol, zGranularity);
    if( zSql==0 ){
        sqlite3OomFault(db);
        return;
    }
    rc = sqlite3_exec(db, zSql, 0, 0, &zErrMsg);
    sqlite3_free(zSql);
    if( rc!=SQLITE_OK ){
        sqlite3ErrorMsg(pParse, "INSERT FEATURE: %s", zErrMsg);
        sqlite3_free(zErrMsg);
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
    Parse *pParse,      /* The parsing context */
    Token *pName,       /* The feature name token */
    Select *pSelect,    /* The SELECT defining the feature */
    Token *pLp,         /* The '(' token — select text starts just after */
    Token *pRp,         /* The ')' token — select text ends just before */
    Token *pPartCol,    /* The partition column name */
    Token *pGran        /* The granularity keyword token (HOUR/DAY/WEEK/MONTH/YEAR) */
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
    if( sqlite3_strnicmp(pGran->z, "HOUR",  4)==0 ||
        sqlite3_strnicmp(pGran->z, "DAY",   3)==0 ||
        sqlite3_strnicmp(pGran->z, "WEEK",  4)==0 ||
        sqlite3_strnicmp(pGran->z, "MONTH", 5)==0 ||
        sqlite3_strnicmp(pGran->z, "YEAR",  4)==0 ){
        zGranularity = sqlite3DbStrNDup(db, pGran->z, pGran->n);
        if( zGranularity==0 ) goto feature_cleanup;
    }else{
        sqlite3ErrorMsg(pParse, "unknown granularity: %.*s", pGran->n, pGran->z);
        goto feature_cleanup;
    }

    /* Slice the SELECT SQL text from the original input buffer.
    ** The text is the content between '(' and ')', with whitespace trimmed. */
    {
        const char *zSelBegin = pLp->z + pLp->n;
        int nSel = (int)(pRp->z - zSelBegin);
        while( nSel>0 && sqlite3Isspace(zSelBegin[0]) ){ zSelBegin++; nSel--; }
        while( nSel>0 && sqlite3Isspace(zSelBegin[nSel-1]) ){ nSel--; }
        zQuery = sqlite3DbStrNDup(db, zSelBegin, nSel);
        if( zQuery==0 ) goto feature_cleanup;
    }

    sqlitefsRegisterFeature(pParse, zName, zQuery, zPartCol, zGranularity);

feature_cleanup:
    sqlite3DbFree(db, zName);
    sqlite3DbFree(db, zQuery);
    sqlite3DbFree(db, zPartCol);
    sqlite3DbFree(db, zGranularity);
    sqlite3SelectDelete(db, pSelect);
}


/*
** Stub for REFRESH FEATURE -- implemented in Phase 3
*/
void sqlite3RefreshFeature(Parse *pParse, Token *pName){
  sqlite3ErrorMsg(pParse, "REFRESH FEATURE not yet implemented");
}

/*
** Stub for DESCRIBE FEATURE -- implemented in Phase 4
*/
void sqlite3DescribeFeature(Parse *pParse, Token *pName){
  sqlite3ErrorMsg(pParse, "DESCRIBE FEATURE not yet implemented");
}


void sqlite3DropFeature(Parse *pParse, Token *pName){
  sqlite3ErrorMsg(pParse, "DROP FEATURE not yet implemented");
}

#endif /* SQLITE_OMIT_FEATURE */

