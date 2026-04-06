/*
** featurestore.h -- SQLite-FS Feature Store Extension
**
** This header declares the public interface for the feature store.
** It is #included from sqliteInt.h so that parse.y action code
** can call these handler functions.
*/
#ifndef SQLITE_FEATURESTORE_H
#define SQLITE_FEATURESTORE_H

/*
** Parser handler functions -- called from parse.y grammar actions.
*/

void sqlite3CreateFeature(Parse*, Token*, Token*, Token*, Token*,
                          int, Token*, int, Select*, Token*, Token*);
/* args: pName, pEntityTable, pTsCol, pGran,
**       nWindowSize, pRefreshMode, nRetainCount,
**       pSelect, pLp '(', pRp ')' */
void sqlite3RefreshFeature(Parse*, Token*);                       
void sqlite3DescribeFeature(Parse*, Token*);       
void sqlite3DropFeature(Parse*, Token*);              

/* void sqlite3BackfillFeature(Parse*, Token*, Token*, Expr*, Expr*);-- Phase 5 */

#endif /* SQLITE_FEATURESTORE_H */