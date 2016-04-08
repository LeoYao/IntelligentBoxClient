
#include <sqlite3.h>
#include <stdio.h>

static int callback(void *NotUsed, int argc, char **argv, char **azColName){
  int i;
  for(i=0; i<argc; i++){
    printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
  }
  printf("-------------------------\n");
  return 0;
}

int test_sqlite(){
	sqlite3 *db1;
	char *zErrMsg = 0;
	int rc;
		rc = sqlite3_open_v2("test.db", &db1, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
	if( rc ){
		fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db1));
		sqlite3_close(db1);
		return(1);
	}

	//Clean the database
	char *sql = "DROP TABLE IF EXISTS Cars;";
	rc = sqlite3_exec(db1, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	//Create a new table
	sql = "CREATE TABLE Cars(Id INT, Name TEXT, Price INT);";
	rc = sqlite3_exec(db1, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	//Insert a record with autocommit
	sql = "INSERT INTO Cars VALUES(1, 'Audi', 52642);";
	rc = sqlite3_exec(db1, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	//Query the result
	rc = sqlite3_exec(db1, "select * from Cars", callback, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	//Transaction
	sql = "BEGIN TRANSACTION;";
	rc = sqlite3_exec(db1, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	sql = "INSERT INTO Cars VALUES(2, 'Mercedes', 57127);";
	rc = sqlite3_exec(db1, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	sql = "ROLLBACK TRANSACTION;";
	rc = sqlite3_exec(db1, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	//No change should be made
	rc = sqlite3_exec(db1, "select * from Cars", callback, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	//Transaction with commiting
	sql = "BEGIN TRANSACTION;"
			"INSERT INTO Cars VALUES(3, 'Skoda', 9000);"
			"COMMIT;";
	rc = sqlite3_exec(db1, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	//A new row is inserted
	rc = sqlite3_exec(db1, "select * from Cars", callback, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	sqlite3_close(db1);
	return 0;
}


int test_sqlite_bind(){
	sqlite3 *db;
	char *zErrMsg = 0;
	int rc;
		rc = sqlite3_open_v2("test.db", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
	if( rc ){
		fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
		sqlite3_close(db);
		return(1);
	}

	//Clean the database
	char *sql = "DROP TABLE IF EXISTS Cars;";
	rc = sqlite3_exec(db, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	//Create a new table
	sql = "CREATE TABLE Cars(Id INT, Name TEXT, Price INT);";
	rc = sqlite3_exec(db, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}
	sql = "INSERT INTO Cars VALUES(1, 'Audi', 52642);";
	rc = sqlite3_exec(db, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}
	sql = "INSERT INTO Cars VALUES(2, 'Mercedes', 57127);";
	rc = sqlite3_exec(db, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	//The following is the example.

	query(db);
	//Update
	update_bind(db);
	query(db);

	//The example is ended here.
	sqlite3_close(db);
	return 0;
}

void query(sqlite3 *db){
	sqlite3_stmt *stmt;

		//Query multiple records
	char* sql = "SELECT Name, Price FROM Cars WHERE Id IN (?,?)\0";

	int	rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);

	if (rc == SQLITE_OK) {
		sqlite3_bind_int(stmt, 1, 1);
		sqlite3_bind_int(stmt, 2, 2);
	} else {

		fprintf(stderr, "Failed to execute statement: %s\n", sqlite3_errmsg(db));
	}

	int step = sqlite3_step(stmt);
	while (step == SQLITE_ROW) {
		printf("%s: ", sqlite3_column_text(stmt, 0));
		printf("%d\n", sqlite3_column_int(stmt, 1));
		step = sqlite3_step(stmt);
	}
	sqlite3_finalize(stmt);
}

void update_bind(sqlite3 *db){

	sqlite3_stmt *stmt;
	char* sql = "Update Cars set Price = ? WHERE Id IN (?)\0";
	int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);

	if (rc == SQLITE_OK) {
		sqlite3_bind_int(stmt, 1, 1000);
		sqlite3_bind_int(stmt, 2, 1);
	} else {
		fprintf(stderr, "Failed to execute statement: %s\n", sqlite3_errmsg(db));
	}

	int step = sqlite3_step(stmt);
	if (step == SQLITE_DONE) {
		printf("Update is done.\n");
	} else {
		printf("Update is failed. Return Code: %d\n", step);
	}

	sqlite3_finalize(stmt);

}
