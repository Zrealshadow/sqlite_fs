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
** Register all feature store SQL functions on a database connection.
** Called during database initialization.
*/
int sqlite3FeatureStoreInit(sqlite3 *db);


/*
** Parser handler functions -- called from parse.y grammar actions.
** These are stubs for now; implemented in Phase 2+.
*/

void sqlite3CreateFeature(Parse*, Token*, Select*, Token*, Token*, Token*, Token*);
/* args: pName, pSelect, pLp '(', pRp ')', pPartCol, pGran */
void sqlite3RefreshFeature(Parse*, Token*);                       
void sqlite3DescribeFeature(Parse*, Token*);       
void sqlite3DropFeature(Parse*, Token*);              

/* void sqlite3BackfillFeature(Parse*, Token*, Token*, Expr*, Expr*);-- Phase 5 */

#endif /* SQLITE_FEATURESTORE_H */